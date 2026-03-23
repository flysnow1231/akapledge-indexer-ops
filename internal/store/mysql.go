package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

type MySQLStore struct {
	DB  *sql.DB
	log *zap.Logger
}

func NewMySQL(dsn string, maxOpen, maxIdle, lifeMinutes int, logger *zap.Logger) (*MySQLStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(time.Duration(lifeMinutes) * time.Minute)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &MySQLStore{DB: db, log: logger}, nil
}

func (s *MySQLStore) Close() error { return s.DB.Close() }

func (s *MySQLStore) EnsureSchema(ctx context.Context) error {
	ddl, err := os.ReadFile("./schema/ddl.sql")
	if err != nil {
		return fmt.Errorf("read ddl: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, string(ddl)); err != nil {
		return fmt.Errorf("exec ddl: %w", err)
	}
	return nil
}

func (s *MySQLStore) GetCheckpoint(ctx context.Context, taskName string, chainID int64, contract string) (uint64, error) {
	var block uint64
	err := s.DB.QueryRowContext(ctx,
		`SELECT last_scanned_block FROM sync_task_checkpoint WHERE task_name=? AND chain_id=? AND contract_address=?`,
		taskName, chainID, contract,
	).Scan(&block)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return block, err
}

func (s *MySQLStore) SaveCheckpointTx(ctx context.Context, tx *sql.Tx, taskName string, chainID int64, contract string, startBlock, scannedBlock, safeBlock, delayBlocks uint64, status string) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO sync_task_checkpoint(
  task_name, chain_id, contract_address, start_block, last_scanned_block, last_safe_block, delay_blocks, status
) VALUES(?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  start_block=VALUES(start_block),
  last_scanned_block=VALUES(last_scanned_block),
  last_safe_block=VALUES(last_safe_block),
  delay_blocks=VALUES(delay_blocks),
  status=VALUES(status)`,
		taskName, chainID, contract, startBlock, scannedBlock, safeBlock, delayBlocks, status,
	)
	return err
}
