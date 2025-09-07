package replication

import (
	"context"
	"fmt"
	"iter"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/codercms/pg-cdc/replication/decoder"
	"github.com/codercms/pg-cdc/replication/types"
	"github.com/codercms/pg-cdc/replication/utils"
)

type Consumer struct {
	slot        string
	publication string

	logger *zap.Logger
	cfg    *Config

	replConn   *pgx.Conn
	replPgConn *pgconn.PgConn
	metaConn   *pgx.Conn

	// lastReceivedLSN last WAL record read from Postgres
	lastReceivedLSN pglogrepl.LSN
	// lastConfirmedLSN the last LSN that the app confirmed has been persisted or applied
	lastConfirmedLSN pglogrepl.LSN
	forceLSNSync     atomic.Bool

	tableInfo      map[uint32]*types.TableInfo
	onNewTableInfo func(*types.TableInfo)

	typeMap *pgtype.Map

	decoder decoder.Decoder
}

func NewConsumer(cfg *Config) *Consumer {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	cDecoder := cfg.Decoder
	if cDecoder == nil {
		cDecoder = &decoder.DefaultDecoder{}
	}

	return &Consumer{
		slot:        cfg.Slot,
		publication: cfg.Publication,

		logger: logger,
		cfg:    cfg,

		tableInfo: make(map[uint32]*types.TableInfo),

		typeMap: pgtype.NewMap(),

		onNewTableInfo: cfg.OnTableInfoChange,

		decoder: cDecoder,
	}
}

func (c *Consumer) StartReplication(ctx context.Context, resumeLSN pglogrepl.LSN) (iter.Seq2[*Tx, error], error) {
	replConn, err := c.cfg.ConnectReplication(ctx)
	if err != nil {
		return nil, fmt.Errorf("Config.ConnectReplication: %w", err)
	}

	c.replConn = replConn

	metaConn, err := c.cfg.ConnectMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("cfg.ConnectMetadata: %w", err)
	}

	c.replPgConn = replConn.PgConn()
	c.metaConn = metaConn

	if err := ensureReplicationSlot(ctx, replConn, c.slot); err != nil {
		return nil, fmt.Errorf("ensureReplicationSlot: %w", err)
	}

	slotMeta, err := fetchSlotMetadata(ctx, metaConn, c.slot, time.Millisecond*500)
	if err != nil {
		return nil, fmt.Errorf("fetchSlotMetadata: %w", err)
	}

	if err := ensurePublicationExists(ctx, c.metaConn, c.publication); err != nil {
		return nil, fmt.Errorf("ensurePublicationExists: %w", err)
	}

	// We're the only application that should be using this replication
	// slot. The only way that there can be another connection using
	// this slot under normal operation is if there's a stale TCP
	// connection from a prior incarnation of the source holding on to
	// the slot. We don't want to wait for the WAL sender timeout and/or
	// TCP keepalives to time out that connection, because these values
	// are generally under the control of the DBA and may not time out
	// the connection for multiple minutes, or at all. Instead we just
	// force kill the connection that's using the slot.
	//
	// Note that there's a small risk that *we're* the zombie cluster
	// that should not be using the replication slot. Kubernetes cannot
	// 100% guarantee that only one cluster is alive at a time. However,
	// this situation should not last long, and the worst that can
	// happen is a bit of transient thrashing over ownership of the
	// replication slot.
	if slotMeta.ActivePID != nil {
		// TODO: pg_terminate_backend
	}

	// Skip the timeline ID check for sources without a known timeline ID
	// (sources created before the timeline ID was added to the source details)
	// TODO
	//if let Some(expected_timeline_id) = timeline_id {
	//	if let Err(err) =
	//	ensure_replication_timeline_id(&replication_client, expected_timeline_id).await?
	//{
	//	return Ok(Err(err));
	//}
	//}

	stream, err := c.rawStream(ctx, resumeLSN)
	if err != nil {
		return nil, fmt.Errorf("RawStream: %w", err)
	}

	return func(yield func(*Tx, error) bool) {
		for msg, err := range stream {
			// Check for ctx cancellation
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}

			// Check for message read error
			if err != nil {
				yield(nil, err)
				return
			}

			switch msg := msg.(type) {
			case *PrimaryKeepaliveMessage:
				c.logger.Debug("Received PrimaryKeepalive message", zap.String("lsn", msg.ServerWALEnd.String()))

			case *XLogData:
				switch msg := msg.Message.(type) {
				case *pglogrepl.BeginMessage:
					commitLSN := msg.FinalLSN

					tx := c.extractTransaction(ctx, stream, commitLSN)
					if err != nil {
						yield(nil, err)
						return
					}

					c.lastReceivedLSN = max(c.lastReceivedLSN, commitLSN+1)

					if !yield(tx, nil) {
						return
					}
				}

			default:
				panic("Bad replication message returned from RawStream")
			}
		}
	}, nil
}

func (c *Consumer) ConfirmLSN(lsn pglogrepl.LSN, forceUpdate bool) {
	addr := (*uint64)(&c.lastConfirmedLSN)

	for {
		// read current value
		old := atomic.LoadUint64(addr)
		if uint64(lsn) <= old {
			return
		}

		// try to update
		if atomic.CompareAndSwapUint64(addr, old, uint64(lsn)) {
			if forceUpdate {
				c.forceLSNSync.Store(true)
			}

			return
		}

		// CAS failed, retry
	}
}

// rawStream Produces the logical replication stream while taking care of regularly sending standby
// keepalive messages with the provided `uppers` stream.
//
// The returned stream will contain all transactions that whose commit LSN is beyond `resume_lsn`.
func (c *Consumer) rawStream(ctx context.Context, resumeLSN pglogrepl.LSN) (iter.Seq2[LogicalReplicationMessage, error], error) {
	// How often a proactive standby status update message should be sent to the server.
	//
	// The upstream will periodically request status updates by setting the keepalive's reply field
	// value to 1. However, we cannot rely on these messages arriving on time. For example, when
	// the upstream is sending a big transaction its keepalive messages are queued and can be
	// delayed arbitrarily.
	//
	// See: <https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com>
	//
	// For this reason we query the server's timeout value and proactively send a keepalive at
	// twice the frequency to have a healthy margin from the deadline.
	//
	// Note: We must use the metadata client here which is NOT in replication mode. Some Aurora
	// Postgres versions disallow SHOW commands from within replication connection.
	// See: https://github.com/readysettech/readyset/discussions/28#discussioncomment-4405671

	var walSenderTimeout time.Duration
	row := c.metaConn.QueryRow(ctx, "SHOW wal_sender_timeout")
	{
		var timeoutStr string
		if err := row.Scan(&timeoutStr); err != nil {
			return nil, fmt.Errorf("query wal_sender_timeout: %w", err)
		}

		switch timeoutStr {
		// When this parameter is zero the timeout mechanism is disabled
		case "0":
			break
		default:
			var err error

			walSenderTimeout, err = utils.ParsePostgresDuration(timeoutStr)
			if err != nil {
				return nil, fmt.Errorf("cannot parse wal_sender_timeout: utils.ParsePostgresDuration: %w", err)
			}
		}
	}

	var feedbackInterval time.Duration

	switch walSenderTimeout {
	case 0:
		feedbackInterval = time.Second
	default:
		feedbackInterval = max(time.Second, walSenderTimeout/2)
	}

	if err := pglogrepl.StartReplication(ctx, c.replConn.PgConn(), c.slot, resumeLSN, pglogrepl.StartReplicationOptions{
		Timeline: 0,
		Mode:     0,
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, pgx.Identifier{c.publication}.Sanitize()),
			"messages 'true'",
			// Disable streaming
			"streaming 'false'",
		},
	}); err != nil {
		return nil, fmt.Errorf("pglogrepl.StartReplication: %w", err)
	}

	// According to the documentation [1] we must check that the slot LSN matches our
	// expectations otherwise we risk getting silently fast-forwarded to a future LSN. In order
	// to avoid a TOCTOU (Time Of Check To Time Of Use) issue we must do this check after starting the replication stream.
	// We cannot use the replication client to do that because it's already in CopyBoth mode.
	// [1] https://www.postgresql.org/docs/15/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION-SLOT-LOGICAL

	slotMetadata, err := fetchSlotMetadata(ctx, c.metaConn, c.slot, time.Millisecond*500)
	if err != nil {
		return nil, fmt.Errorf("fetchSlotMetadata: %w", err)
	}

	c.logger.Info("Started replication",
		zap.Int32p("pid", slotMetadata.ActivePID),
		zap.Duration("wal_sender_timeout", walSenderTimeout),
		zap.Duration("feedback_interval", feedbackInterval),
		zap.String("slot_lsn", slotMetadata.ConfirmedFlushLSN.String()),
		zap.String("resume_lsn", resumeLSN.String()),
	)

	if !(resumeLSN == 0 || slotMetadata.ConfirmedFlushLSN <= resumeLSN) {
		return nil, &OverCompactedReplicationSlotError{
			RequestedLSN: resumeLSN,
			AvailableLSN: slotMetadata.ConfirmedFlushLSN,
		}
	}

	c.lastReceivedLSN = resumeLSN
	c.lastConfirmedLSN = resumeLSN

	return func(yield func(LogicalReplicationMessage, error) bool) {
		nextFeedbackAt := time.Now().Add(feedbackInterval)

		for {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}

			// Postgres only sends PrimaryKeepAlive messages when *it* wants a reply, which
			// happens when out status update is late. Since we send them proactively this
			// may never happen. It is therefore *crucial* that we set the last parameter
			// (the reply flag) to 1 here. This will cause the upstream server to send us a
			// PrimaryKeepAlive message promptly which will give us frontier advancement
			// information in the absence of data updates.
			if now := time.Now(); now.After(nextFeedbackAt) || c.forceLSNSync.Load() {
				nextFeedbackAt = now.Add(feedbackInterval)

				if err := pglogrepl.SendStandbyStatusUpdate(ctx, c.replConn.PgConn(), pglogrepl.StandbyStatusUpdate{
					WALWritePosition: c.lastConfirmedLSN,
					WALFlushPosition: c.lastConfirmedLSN,
					WALApplyPosition: c.lastConfirmedLSN,
					ReplyRequested:   true,
				}); err != nil {
					yield(nil, fmt.Errorf("pglogrepl.SendStandbyStatusUpdate: %w", err))
					return
				}

				c.forceLSNSync.CompareAndSwap(true, false)

				c.logger.Debug("Sent standby status update", zap.String("lsn", c.lastConfirmedLSN.String()))
			}

			// Set connection read timeout until next feedback needed
			_ = c.replConn.PgConn().Conn().SetReadDeadline(nextFeedbackAt)
			//c.logger.Debug("Set read deadline to", zap.Time("at", nextFeedbackAt))

			msg, err := c.readReplicationMessage(ctx)
			if err != nil {
				// Continue on timeout errors, cause it maybe read deadline error
				if pgconn.Timeout(err) {
					continue
				}

				yield(nil, err)
				return
			}

			// Server requests response
			if pkm, ok := msg.(*PrimaryKeepaliveMessage); ok {
				c.lastReceivedLSN = max(c.lastReceivedLSN, pkm.ServerWALEnd)

				if pkm.ReplyRequested {
					nextFeedbackAt = time.Time{}
				}
			}

			if !yield(msg, err) {
				return
			}
		}
	}, nil
}

func (c *Consumer) readReplicationMessage(ctx context.Context) (LogicalReplicationMessage, error) {
	rawMsg, err := c.replConn.PgConn().ReceiveMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("conn.ReceiveMessage: %w", err)
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return nil, fmt.Errorf("replication ErrorResponse: %s (%s)", errMsg.Message, errMsg.Detail)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil, &UnknownReplicationMessageError{
			Type: fmt.Sprintf("%T", rawMsg),
		}
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return nil, &ReplicationMessageParseError{
				Err: fmt.Errorf("pglogrepl.ParsePrimaryKeepaliveMessage: %w", err),
			}
		}

		return &PrimaryKeepaliveMessage{pkm}, nil

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return nil, &ReplicationMessageParseError{
				Err: fmt.Errorf("pglogrepl.ParseXLogData: %w", err),
			}
		}

		xldMsg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return nil, &LogicalReplicationMessageParseError{
				Err: fmt.Errorf("pglogrepl.Parse: %w", err),
			}
		}

		return &XLogData{
			XLogData: xld,
			Message:  xldMsg,
		}, nil

	default:
		return nil, &UnknownReplicationMessageError{}
	}
}

// RegisterType register custom pg type
//
// This method IS NOT thread safe, actually it is safe to call it before replication started
func (c *Consumer) RegisterType(typ *pgtype.Type) {
	c.typeMap.RegisterType(typ)
}

func (c *Consumer) setTableInfo(ti *types.TableInfo) {
	c.tableInfo[ti.OID] = ti

	if c.onNewTableInfo != nil {
		c.onNewTableInfo(ti)
	}
}
