package replication

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

type SnapshotConfig struct {
	StatementTimeout   time.Duration
	CollectStrictCount bool

	Workers int

	Config *Config
}

type SnapshotConsumer struct {
	conn    *pgx.Conn
	typeMap *pgtype.Map
	config  SnapshotConfig

	logger *zap.Logger
}

func NewSnapshotConsumer(
	ctx context.Context,
	config SnapshotConfig,
) (*SnapshotConsumer, error) {
	metaConn, err := config.Config.ConnectMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("cfg.ConnectMetadata: %w", err)
	}

	return &SnapshotConsumer{
		typeMap: pgtype.NewMap(),
		config:  config,

		conn: metaConn,
	}, nil
}

func (sc *SnapshotConsumer) UseSnapshot(ctx context.Context, snapshotID string) error {
	_, err := sc.conn.Exec(ctx, "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	_, err = sc.conn.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", snapshotID))
	if err != nil {
		_, _ = sc.conn.Exec(ctx, "ROLLBACK")

		return fmt.Errorf("failed to set transaction snapshot: %w", err)
	}

	return nil
}

func (sc *SnapshotConsumer) SetStatementTimeout(ctx context.Context, timeout time.Duration) error {
	_, err := sc.conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = %d", timeout.Milliseconds()))
	return err
}

func (sc *SnapshotConsumer) SnapshotTable(
	ctx context.Context,
	schema, table string,
	sql string,
	outputHandler func(row map[string]any) error,
) error {
	// Set statement timeout
	err := sc.SetStatementTimeout(ctx, sc.config.StatementTimeout)
	if err != nil {
		return fmt.Errorf("failed to set statement timeout: %w", err)
	}

	var fdQuery string
	if len(sql) == 0 {
		fdQuery = "SELECT * FROM " + pgx.Identifier([]string{schema, table}).Sanitize()
	} else {
		fdQuery = sql
	}

	noRows, err := sc.conn.Query(ctx, fdQuery+" limit 0")
	if err != nil {
		return fmt.Errorf("failed to execute sql + limit 0 to get field descriptions: %w", err)
	}

	fieldDescriptions := noRows.FieldDescriptions()
	cols := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		cols = append(cols, fd.Name)
	}
	noRows.Close()

	// Execute COPY command to get table data using the replication connection
	query := fmt.Sprintf("COPY %s TO STDOUT WITH (FORMAT CSV, DELIMITER '\t')",
		pgx.Identifier([]string{schema, table}).Sanitize(),
	)
	if len(sql) > 0 {
		query = "COPY (" + sql + ") TO STDOUT WITH (FORMAT CSV, DELIMITER '\t')"
	}

	// Step 3. Run COPY and pipe results into csv.Reader
	pr, pw := io.Pipe()
	defer pr.Close()

	copyErrCh := make(chan error, 1)
	go func() {
		_, err := sc.conn.PgConn().CopyTo(ctx, pw, query)
		// Close pipe with error if CopyTo fails
		_ = pw.CloseWithError(err)
		copyErrCh <- err
	}()

	csvReader := csv.NewReader(pr)
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV row: %w", err)
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			if i < len(record) {
				row[col] = record[i]
			} else {
				row[col] = nil
			}
		}

		if err := outputHandler(row); err != nil {
			return err
		}
	}

	// Wait for COPY to finish
	if err := <-copyErrCh; err != nil {
		return fmt.Errorf("COPY failed: %w", err)
	}

	return nil
}

func (sc *SnapshotConsumer) GetTableRowCount(ctx context.Context, schema, table string, oid uint32) (int64, error) {
	// Try to get estimated count from pg_class
	var estimateCount int64

	if err := sc.conn.QueryRow(
		ctx,
		"SELECT reltuples::bigint FROM pg_class WHERE oid = $1",
		oid,
	).Scan(&estimateCount); err != nil {
		return 0, fmt.Errorf("failed to get estimated row count: %w", err)
	}

	// If enabled and estimate is reasonable, get exact count
	if sc.config.CollectStrictCount && estimateCount < 1000000 {
		var exactCount int64

		if err := sc.conn.QueryRow(
			ctx,
			fmt.Sprintf(
				"SELECT COUNT(*) FROM %s",
				pgx.Identifier([]string{schema, table}).Sanitize(),
			),
		).Scan(&exactCount); err != nil {
			return 0, fmt.Errorf("failed to get exact row count: %w", err)
		}

		return exactCount, nil
	}

	return estimateCount, nil
}

func (sc *SnapshotConsumer) Close() {
	if sc.conn != nil {
		sc.conn.Close(context.Background())
	}

	sc.logger.Debug("Snapshot consumer closed")
}

// SnapshotCoordinator manages the snapshot process across multiple workers
type SnapshotCoordinator struct {
	consumers []*SnapshotConsumer
	config    SnapshotConfig

	snapshotID  string
	snapshotLSN pglogrepl.LSN

	logger *zap.Logger

	replConn *pgconn.PgConn

	pool chan *snapshotConsumerPoolJob
}

type snapshotConsumerPoolJob struct {
	ctx   context.Context
	table SnapshotTable

	done func(error)
}

func NewSnapshotCoordinator(config SnapshotConfig) *SnapshotCoordinator {
	if config.Workers < 1 {
		config.Workers = 1
	}

	logger := config.Config.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	return &SnapshotCoordinator{
		config: config,

		logger: logger,

		pool: make(chan *snapshotConsumerPoolJob, config.Workers),
	}
}

func (sc *SnapshotCoordinator) StartSnapshot(ctx context.Context) error {
	consumers := make([]*SnapshotConsumer, sc.config.Workers)

	for i := 0; i < sc.config.Workers; i++ {
		consumer, err := NewSnapshotConsumer(ctx, sc.config)
		consumer.logger = sc.logger.With(zap.Int("worker", i))

		if err != nil {
			// Close already created consumers
			for j := 0; j < i; j++ {
				consumers[j].Close()
			}

			return fmt.Errorf("failed to create consumer %d: %w", i, err)
		}

		consumers[i] = consumer
	}

	sc.consumers = consumers

	if err := sc.exportSnapshot(ctx); err != nil {
		return fmt.Errorf("failed to export snapshot: %w", err)
	}

	// Have all other consumers use the exported snapshot
	for i, consumer := range sc.consumers {
		if err := consumer.UseSnapshot(ctx, sc.snapshotID); err != nil {
			return fmt.Errorf("failed to use snapshot on consumer %d: %w", i, err)
		}
	}

	for _, consumer := range sc.consumers {
		go func(consumer *SnapshotConsumer) {
			defer func() {
				consumer.logger.Debug("Snapshot consumer worker thread stopped")
			}()

			for job := range sc.pool {
				tblName := pgx.Identifier{job.table.Schema, job.table.Schema}.Sanitize()

				consumer.logger.Info("Starting snapshot process on table", zap.String("table", tblName))

				err := consumer.SnapshotTable(job.ctx, job.table.Schema, job.table.Name, job.table.Query, job.table.ProcessRow)

				if err != nil {
					consumer.logger.Error("Failed to snapshot table", zap.String("table", tblName), zap.Error(err))
				} else {
					consumer.logger.Info("Snapshot done on table", zap.String("table", tblName))
				}

				job.done(err)
			}
		}(consumer)
	}

	return nil
}

func (sc *SnapshotCoordinator) exportSnapshot(ctx context.Context) error {
	replConn, err := sc.config.Config.ConnectReplication(ctx)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %w", err)
	}

	sc.replConn = replConn.PgConn()

	if _, err := replConn.Exec(ctx, "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("failed to start snapshot tx: %w", err)
	}

	// Create a temporary replication slot to get a consistent point
	tempSlot := fmt.Sprintf("cdc_snapshot_%d", time.Now().UnixNano())
	slotRes, err := pglogrepl.CreateReplicationSlot(ctx, sc.replConn, tempSlot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary:      true,
		SnapshotAction: "USE_SNAPSHOT",
	})
	if err != nil {
		return fmt.Errorf("CreateReplicationSlot: %w", err)
	}

	// Parse the consistent point LSN
	lsn, err := pglogrepl.ParseLSN(slotRes.ConsistentPoint)
	if err != nil {
		return fmt.Errorf("failed to parse consistent point: %w", err)
	}

	var snapshotID string
	if err := replConn.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
		return fmt.Errorf("failed to export snapshot: %w", err)
	}

	sc.snapshotID = snapshotID
	sc.snapshotLSN = lsn

	sc.logger.Info("Snapshot coordinator has exported snapshot",
		zap.String("snapshot_id", sc.snapshotID),
		zap.String("snapshot_lsn", sc.snapshotLSN.String()),
	)

	return nil
}

func (sc *SnapshotCoordinator) SnapshotTables(ctx context.Context, tables []SnapshotTable) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var firstErrMu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(tables))

	// Distribute tables among consumers
	for _, table := range tables {
		sc.pool <- &snapshotConsumerPoolJob{
			ctx:   ctx,
			table: table,

			done: func(err error) {
				if err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
				}

				wg.Done()
			},
		}
	}

	wg.Wait()

	return firstErr
}

func (sc *SnapshotCoordinator) ConsistentPoint() pglogrepl.LSN {
	return sc.snapshotLSN
}

func (sc *SnapshotCoordinator) Close() {
	for _, consumer := range sc.consumers {
		consumer.Close()
	}

	if sc.replConn != nil {
		sc.replConn.Close(context.Background())
	}

	close(sc.pool)

	sc.logger.Debug("Snapshot coordinator closed")
}

type SnapshotTable struct {
	Schema string
	Name   string

	Query string

	ProcessRow func(row map[string]any) error
}
