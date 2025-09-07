package main

import (
	"context"
	"errors"
	"flag"
	"github.com/jackc/pglogrepl"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/codercms/pg-cdc/replication"
	"github.com/codercms/pg-cdc/replication/event"
)

var connString string
var slot string
var publication string
var initialSnapshot bool

func init() {
	flag.StringVar(&connString, "conn", "", "connection string")
	flag.StringVar(&slot, "slot", "", "replication slot")
	flag.StringVar(&publication, "publication", "", "replication publication name")
	flag.BoolVar(&initialSnapshot, "snapshot", false, "make initial snapshot?")
}

func main() {
	flag.Parse()

	logger, _ := zap.NewDevelopment()

	cfg := replication.Config{
		ConnString:  connString,
		Slot:        slot,
		Publication: publication,
		Logger:      logger,
	}

	if err := cfg.Parse(); err != nil {
		logger.Fatal("Cannot parse config", zap.Error(err))
	}

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	var snapshotCoord *replication.SnapshotCoordinator

	if initialSnapshot {
		snapshotCoord = replication.NewSnapshotCoordinator(replication.SnapshotConfig{
			Workers: 1,
			Config:  &cfg,
		})

		if err := snapshotCoord.StartSnapshot(ctx); err != nil {
			logger.Fatal("Cannot start snapshot process", zap.Error(err))
		}

		if err := snapshotCoord.SnapshotTables(
			ctx,
			[]replication.SnapshotTable{
				{
					Schema: "public",
					Name:   "users",
					ProcessRow: func(row map[string]any) error {
						logger.Info("Snapshot row", zap.Any("row", row))

						return nil
					},
				},
				{
					Schema: "public",
					Name:   "users",
					ProcessRow: func(row map[string]any) error {
						logger.Info("Snapshot row", zap.Any("row", row))

						return nil
					},
				},
				{
					Schema: "public",
					Name:   "users",
					ProcessRow: func(row map[string]any) error {
						logger.Info("Snapshot row", zap.Any("row", row))

						return nil
					},
				},
			},
		); err != nil {
			logger.Fatal("Cannot finish snapshot process", zap.Error(err))
		}
	}

	var resumeLSN pglogrepl.LSN
	if snapshotCoord != nil {
		resumeLSN = snapshotCoord.ConsistentPoint()
		logger.Info("Using consistent point from snapshot", zap.String("lsn", resumeLSN.String()))
	}

	consumer := replication.NewConsumer(&cfg)

	handleError := func(err error) bool {
		if err == nil {
			return false
		}

		if errors.Is(err, context.Canceled) {
			return true
		}

		logger.Fatal("Replication error", zap.Error(err))
		return true
	}

	txIterator, err := consumer.StartReplication(ctx, resumeLSN)
	// It is only safe to stop snapshot coordinator after
	if snapshotCoord != nil {
		snapshotCoord.Close()
	}
	if err != nil {
		logger.Fatal("Cannot start replication", zap.Error(err))
	}

	for tx, err := range txIterator {
		if handleError(err) {
			return
		}

		logger.Info("TX started")

		for ev, err := range tx.Events {
			if handleError(err) {
				return
			}

			var rel string
			var evData any

			switch ev := ev.(type) {
			case *event.InsertEvent:
				rel = ev.Rel.String()
				evData = ev.Data

			case *event.UpdateEvent:
				rel = ev.Rel.String()
				evData = ev.Data

			case *event.DeleteEvent:
				rel = ev.Rel.String()
				evData = ev.OldData

			case *event.TruncateEvent:
			}

			logger.Info("Replication event",
				zap.String("event", ev.Operation().String()),
				zap.String("lsn", ev.GetLSN().String()),
				zap.String("rel", rel),
				zap.Any("data", evData),
			)
		}

		tx.Confirm(false)

		logger.Info("TX handled")
	}
}
