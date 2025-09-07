package replication

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/codercms/pg-cdc/replication/decoder"
	"github.com/codercms/pg-cdc/replication/types"
)

type Config struct {
	// ConnString PostgreSQL connection string in format accepted by pgx driver
	ConnString string

	// Slot replication slot name
	Slot string
	// Publication replication publication name
	Publication string

	// MetadataConnCfg is a config that used for metadata connection create
	//
	// You free to modify it, but note that it gets filled only after Parse method is called
	MetadataConnCfg *pgx.ConnConfig

	// ReplicationConnCfg is a config that used for replication connection create
	//
	// You free to modify it, but note that it gets filled only after Parse method is called
	ReplicationConnCfg *pgx.ConnConfig

	// Logger optional zap logger instance to log replication status
	Logger *zap.Logger

	// Decoder optional custom tuple decoder impl
	//
	// Can be used to decode tuples as a structs
	Decoder decoder.Decoder

	// OnTableInfoChange is called when new table info sent via replication
	OnTableInfoChange func(*types.TableInfo)

	// Connections below is filled when initial snapshot requested

	metadataConn *pgx.Conn
	snapshotConn *pgx.Conn
}

func (c *Config) Parse() error {
	connCfg, err := pgx.ParseConfig(c.ConnString)
	if err != nil {
		return fmt.Errorf("pgx.ParseConfig: %w", err)
	}

	// Set metadata conn config

	c.MetadataConnCfg = connCfg

	delete(c.MetadataConnCfg.RuntimeParams, "replication")
	c.MetadataConnCfg.RuntimeParams["application_name"] = "CDC Metadata conn"
	c.setGenericRuntimeParams(c.MetadataConnCfg)

	// Set repl conn config

	c.ReplicationConnCfg = connCfg.Copy()
	c.ReplicationConnCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	c.ReplicationConnCfg.RuntimeParams["replication"] = "database"
	c.ReplicationConnCfg.RuntimeParams["application_name"] = "CDC Replication conn"
	c.setGenericRuntimeParams(c.ReplicationConnCfg)

	return nil
}

func (c *Config) ConnectReplication(ctx context.Context) (*pgx.Conn, error) {
	if c.ReplicationConnCfg == nil {
		return nil, fmt.Errorf("ReplicationConnCfg is nil")
	}

	conn, err := pgx.ConnectConfig(ctx, c.ReplicationConnCfg)
	if err != nil {
		return nil, fmt.Errorf("pgx.ConnectConfig: %w", err)
	}

	return conn, nil
}

func (c *Config) ConnectMetadata(ctx context.Context) (*pgx.Conn, error) {
	if c.MetadataConnCfg == nil {
		return nil, fmt.Errorf("MetadataConnCfg is nil")
	}

	conn, err := pgx.ConnectConfig(ctx, c.MetadataConnCfg)
	if err != nil {
		return nil, fmt.Errorf("pgx.ConnectConfig: %w", err)
	}

	return conn, nil
}

func (c *Config) setGenericRuntimeParams(cfg *pgx.ConnConfig) {
	cfg.RuntimeParams["timezone"] = "UTC"
	cfg.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	cfg.RuntimeParams["statement_timeout"] = "0"
	cfg.RuntimeParams["DateStyle"] = "ISO, DMY"
}
