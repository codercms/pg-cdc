package replication

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/codercms/pg-cdc/replication/event"
	"github.com/codercms/pg-cdc/replication/types"
)

type Tx struct {
	CommitLSN pglogrepl.LSN
	Events    iter.Seq2[event.Event, error]

	consumer *Consumer
}

func (tx *Tx) Confirm(forceUpdate bool) {
	tx.consumer.ConfirmLSN(tx.CommitLSN+1, forceUpdate)
}

func (tx *Tx) ConfirmEvent(ev event.Event, forceUpdate bool) {
	tx.consumer.ConfirmLSN(ev.GetLSN(), forceUpdate)
}

var errTxSuccessfullyDone = errors.New("transaction completed")

func (c *Consumer) extractTransaction(
	ctx context.Context,
	stream iter.Seq2[LogicalReplicationMessage, error],
	commitLSN pglogrepl.LSN,
) *Tx {
	return &Tx{
		consumer: c,

		CommitLSN: commitLSN,
		Events: func(yield func(event.Event, error) bool) {
			for msg, err := range stream {
				if err := ctx.Err(); err != nil {
					yield(nil, err)
					return
				}

				if err != nil {
					yield(nil, err)
					return
				}

				switch msg := msg.(type) {
				// We can ignore keepalive messages while processing a transaction because the
				// commit_lsn will drive progress.
				case *PrimaryKeepaliveMessage:
					continue

				case *XLogData:
					ev, err := c.handleTxReplicationMessage(ctx, msg, commitLSN)
					if err != nil {
						// Special case: TX finished
						if err == errTxSuccessfullyDone {
							return
						}

						yield(nil, err)
						return
					}

					// No meaningful event for higher level consumer
					if ev == nil {
						continue
					}

					if !yield(ev, nil) {
						return
					}
				}
			}
		},
	}
}

func (c *Consumer) handleTxReplicationMessage(ctx context.Context, xld *XLogData, commitLSN pglogrepl.LSN) (event.Event, error) {
	switch msg := xld.Message.(type) {
	case *pglogrepl.InsertMessage:
		return c.handleInsertMessage(msg, xld.WALStart)

	case *pglogrepl.UpdateMessage:
		return c.handleUpdateMessage(msg, xld.WALStart)

	case *pglogrepl.DeleteMessage:
		return c.handleDeleteMessage(msg, xld.WALStart)

	case *pglogrepl.RelationMessage:
		var key []*pglogrepl.RelationMessageColumn
		fds := make([]pgconn.FieldDescription, 0, len(msg.Columns))

		var keyFDs []pgconn.FieldDescription

		for idx, col := range msg.Columns {
			fd := pgconn.FieldDescription{
				Name:                 col.Name,
				TableOID:             msg.RelationID,
				TableAttributeNumber: uint16(idx),
				DataTypeOID:          col.DataType,
				//DataTypeSize:         dataType.,
				TypeModifier: col.TypeModifier,
				Format:       0,
			}

			fds = append(fds, fd)

			if col.Flags == 1 {
				key = append(key, col)
				keyFDs = append(keyFDs, fd)
			}
		}

		if perTableDec, ok := c.perTableDecoderRef[msg.Namespace+"."+msg.RelationName]; ok {
			c.perTableDecoder[msg.RelationID] = perTableDec
		}

		c.setTableInfo(&types.TableInfo{
			OID: msg.RelationID,

			Schema: msg.Namespace,
			Name:   msg.RelationName,

			Columns: msg.Columns,
			FDs:     fds,

			Key:    key,
			KeyFDs: keyFDs,
		})

		return nil, nil

	case *pglogrepl.TypeMessage:
		typFullname := msg.Namespace + "." + msg.Name

		typ, err := c.metaConn.LoadType(ctx, typFullname)
		if err != nil {
			return nil, fmt.Errorf("failed to load type %q (OID %d): %w", typFullname, msg.DataType, err)
		}

		c.RegisterType(typ)

		return nil, nil

	case *pglogrepl.TruncateMessage:
		truncated := make([]event.BaseEvent, 0, len(msg.RelationIDs))

		for _, relationID := range msg.RelationIDs {
			tInfo := c.tableInfo[relationID]
			if tInfo == nil {
				continue
			}

			truncated = append(truncated, event.BaseEvent{
				Rel: tInfo,
				LSN: xld.WALStart,
			})
		}

		return &event.TruncateEvent{
			LSN:       xld.WALStart,
			Truncated: truncated,
		}, nil

	case *pglogrepl.CommitMessage:
		if commitLSN != msg.CommitLSN {
			return nil, &InvalidTransactionError{}
		}

		return nil, errTxSuccessfullyDone

	case *pglogrepl.OriginMessage:
		// TODO: We should handle origin messages and emit an error as they indicate that
		//   the upstream performed a point in time restore so all bets are off about the
		//   continuity of the stream.

	case *pglogrepl.BeginMessage:
		return nil, &NestedTransactionError{}
	}

	return nil, &UnknownLogicalReplicationMessageError{
		Type: fmt.Sprintf("%T", xld.Message),
	}
}

func (c *Consumer) handleInsertMessage(msg *pglogrepl.InsertMessage, lsn pglogrepl.LSN) (*event.InsertEvent, error) {
	rel := c.tableInfo[msg.RelationID]
	if rel == nil {
		return nil, nil
	}

	decoder := c.perTableDecoder[msg.RelationID]
	if decoder == nil {
		decoder = c.decoder
	}

	values, err := decoder.DecodeTuple(c.typeMap, rel, msg.Tuple.Columns, false)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tuple values: %w", err)
	}

	return &event.InsertEvent{
		BaseEvent: event.BaseEvent{
			Rel: rel,
			LSN: lsn,
		},
		Data: values,
	}, nil
}

// Implement similar handlers for update and delete messages
func (c *Consumer) handleUpdateMessage(msg *pglogrepl.UpdateMessage, lsn pglogrepl.LSN) (*event.UpdateEvent, error) {
	rel := c.tableInfo[msg.RelationID]
	if rel == nil {
		return nil, nil
	}

	decoder := c.perTableDecoder[msg.RelationID]
	if decoder == nil {
		decoder = c.decoder
	}

	var oldValues any
	var newValues any

	var err error

	// Handle old tuple (if present)
	if msg.OldTuple != nil {
		probablyOnlyKey := msg.OldTupleType == pglogrepl.UpdateMessageTupleTypeKey

		oldValues, err = decoder.DecodeTuple(c.typeMap, rel, msg.OldTuple.Columns, probablyOnlyKey)
		if err != nil {
			return nil, fmt.Errorf("failed to extract old tuple values: %w", err)
		}
	}

	// Handle new tuple
	newValues, err = decoder.DecodeTuple(c.typeMap, rel, msg.NewTuple.Columns, false)
	if err != nil {
		return nil, fmt.Errorf("failed to extract new tuple values: %w", err)
	}

	return &event.UpdateEvent{
		BaseEvent: event.BaseEvent{
			Rel: rel,
			LSN: lsn,
		},
		OldData: oldValues,
		Data:    newValues,
	}, nil
}

func (c *Consumer) handleDeleteMessage(msg *pglogrepl.DeleteMessage, lsn pglogrepl.LSN) (*event.DeleteEvent, error) {
	rel := c.tableInfo[msg.RelationID]
	if rel == nil {
		return nil, nil
	}

	decoder := c.perTableDecoder[msg.RelationID]
	if decoder == nil {
		decoder = c.decoder
	}

	probablyOnlyKey := msg.OldTupleType == pglogrepl.DeleteMessageTupleTypeKey

	oldValues, err := decoder.DecodeTuple(c.typeMap, rel, msg.OldTuple.Columns, probablyOnlyKey)
	if err != nil {
		return nil, fmt.Errorf("failed to extract old tuple values: %w", err)
	}

	return &event.DeleteEvent{
		BaseEvent: event.BaseEvent{
			Rel: rel,
			LSN: lsn,
		},
		OldData: oldValues,
	}, nil
}
