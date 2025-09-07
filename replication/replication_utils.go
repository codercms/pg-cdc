package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/codercms/pg-cdc/replication/utils"
)

// Ensures the publication exists on the server. It returns an outer transient error in case of
// connection issues and an inner definite error if the publication is dropped.
func ensurePublicationExists(
	ctx context.Context,
	client *pgx.Conn,
	publication string,
) error {
	const query = `SELECT 1 FROM pg_publication WHERE pubname = $1`

	var res int32

	row := client.QueryRow(ctx, query, publication)
	if err := row.Scan(&res); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &MissingPublicationError{Publication: publication}
		}

		return err
	}

	return nil
}

type SlotMetadata struct {
	ActivePID         *int32
	ConfirmedFlushLSN pglogrepl.LSN
}

func fetchSlotMetadata(ctx context.Context, client *pgx.Conn, slot string, interval time.Duration) (*SlotMetadata, error) {
	const query = "SELECT active_pid, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1"

	var activePid pgtype.Int4
	var confirmedFlushLsn utils.NullableLSN

	for {
		res := client.QueryRow(ctx, query, slot)
		if err := res.Scan(&activePid, &confirmedFlushLsn); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, &MissingReplicationSlotError{}
			}

			return nil, err
		}

		// It can happen that confirmed_flush_lsn is NULL as the slot initializes
		if !confirmedFlushLsn.Valid {
			time.Sleep(interval)

			continue
		}

		var activePidPtr *int32
		if activePid.Valid {
			activePidPtr = &activePid.Int32
		}

		return &SlotMetadata{
			ActivePID:         activePidPtr,
			ConfirmedFlushLSN: confirmedFlushLsn.LSN,
		}, nil
	}
}

func ensureReplicationSlot(ctx context.Context, client *pgx.Conn, slot string) error {
	if _, err := pglogrepl.CreateReplicationSlot(
		ctx,
		client.PgConn(),
		pgx.Identifier{slot}.Sanitize(),
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			SnapshotAction: "NOEXPORT_SNAPSHOT",
		},
	); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != pgerrcode.DuplicateObject {
			return fmt.Errorf("unable to create replication slot: %w", err)
		}

		return nil
	}

	return nil
}
