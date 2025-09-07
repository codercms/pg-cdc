package main

import (
	"context"
	"errors"
	"flag"
	"github.com/codercms/pg-cdc/replication/decoder"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"

	"github.com/codercms/pg-cdc/replication"
	"github.com/codercms/pg-cdc/replication/event"
)

var connString string
var slot string
var publication string
var initialSnapshot bool

func init() {
	flag.StringVar(&connString, "conn", "", "replication connection string")
	flag.StringVar(&slot, "slot", "", "replication slot")
	flag.StringVar(&publication, "publication", "", "replication publication name")
	flag.BoolVar(&initialSnapshot, "snapshot", false, "make initial snapshot?")
}

type User struct {
	ID        int64         `db:"id" json:"id"`
	Email     string        `db:"email" json:"email"`
	Dob       pgtype.Date   `db:"dob" json:"dob"`
	Bio       zeronull.Text `db:"bio" json:"bio"`
	CreatedAt time.Time     `db:"created_at" json:"created_at"`

	Toasted []string `db:"-" json:"toasted,omitempty"`
}
type UserDelete struct {
	ID int64 `db:"id" json:"id"`
}

func (u *User) SetToastedColumns(cols []string) {
	u.Toasted = cols
}

func main() {
	flag.Parse()

	loggerDevCfg := zap.NewDevelopmentConfig()
	loggerDevCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)

	logger, _ := loggerDevCfg.Build()

	cfg := replication.Config{
		ConnString:  connString,
		Slot:        slot,
		Publication: publication,
		Logger:      logger,

		PerTableDecoder: map[string]decoder.Decoder{
			"public.users": new(decoder.StructDecoder[User, UserDelete]),
		},
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
				//{
				//	Schema: "public",
				//	Name:   "users",
				//	ProcessRow: func(row map[string]any) error {
				//		logger.Info("Snapshot row", zap.Any("row", row))
				//
				//		return nil
				//	},
				//},
				//{
				//	Schema: "public",
				//	Name:   "users",
				//	ProcessRow: func(row map[string]any) error {
				//		logger.Info("Snapshot row", zap.Any("row", row))
				//
				//		return nil
				//	},
				//},
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

		start := time.Now()
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
		// You have to store and advance resumeLSN in some persistent storage, so you can always start at desired LSN
		// and skip already processed changes in case of replication handler crash, i.e. in the middle of transaction

		logger.Info("TX handled", zap.Duration("elapsed", time.Since(start)))
	}
}
