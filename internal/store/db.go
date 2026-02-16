package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

type DB struct {
	*sql.DB
}

func New(dataDir string) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "worb.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	store := &DB{db}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return store, nil
}

func (db *DB) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS projects (
			id TEXT PRIMARY KEY,
			entity TEXT NOT NULL DEFAULT 'local',
			name TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT current_timestamp,
			UNIQUE(entity, name)
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL REFERENCES projects(id),
			name TEXT NOT NULL,
			display_name TEXT,
			config JSON,
			summary JSON,
			state TEXT DEFAULT 'running',
			host TEXT,
			program TEXT,
			git_commit TEXT,
			tags JSON DEFAULT '[]',
			notes TEXT,
			group_name TEXT,
			job_type TEXT,
			sweep_name TEXT,
			history_line_count INTEGER DEFAULT 0,
			events_line_count INTEGER DEFAULT 0,
			log_line_count INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT current_timestamp,
			updated_at TIMESTAMP DEFAULT current_timestamp,
			heartbeat_at TIMESTAMP DEFAULT current_timestamp
		)`,
		`CREATE TABLE IF NOT EXISTS history (
			run_id TEXT NOT NULL REFERENCES runs(id),
			step INTEGER NOT NULL,
			data JSON NOT NULL,
			timestamp TIMESTAMP DEFAULT current_timestamp
		)`,
		`CREATE TABLE IF NOT EXISTS system_events (
			run_id TEXT NOT NULL REFERENCES runs(id),
			line_num INTEGER NOT NULL,
			data JSON NOT NULL,
			timestamp TIMESTAMP DEFAULT current_timestamp
		)`,
		`CREATE TABLE IF NOT EXISTS console_logs (
			run_id TEXT NOT NULL REFERENCES runs(id),
			line_num INTEGER NOT NULL,
			line TEXT NOT NULL,
			stream TEXT DEFAULT 'stdout',
			timestamp TIMESTAMP DEFAULT current_timestamp
		)`,
		`CREATE TABLE IF NOT EXISTS artifacts (
			id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL REFERENCES runs(id),
			type TEXT NOT NULL,
			name TEXT NOT NULL,
			digest TEXT,
			state TEXT DEFAULT 'pending',
			metadata JSON,
			created_at TIMESTAMP DEFAULT current_timestamp
		)`,
		`CREATE TABLE IF NOT EXISTS files (
			id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL REFERENCES runs(id),
			name TEXT NOT NULL,
			url TEXT,
			size INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT current_timestamp
		)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}
	return nil
}
