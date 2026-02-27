package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

type flexTime struct {
	time.Time
}

func (ft *flexTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case time.Time:
		ft.Time = v
		return nil
	case string:
		for _, layout := range []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			time.RFC3339,
		} {
			if t, err := time.Parse(layout, v); err == nil {
				ft.Time = t
				return nil
			}
		}
		return fmt.Errorf("flexTime: cannot parse %q", v)
	case nil:
		ft.Time = time.Time{}
		return nil
	default:
		return fmt.Errorf("flexTime: unsupported type %T (%v)", value, value)
	}
}

func (ft flexTime) Value() (driver.Value, error) {
	return ft.Time, nil
}

type DB struct {
	*sql.DB
	Engine string
}

func (db *DB) castJSON(col string) string {
	if db.Engine == "duckdb" {
		return "CAST(" + col + " AS VARCHAR)"
	}
	return col
}

func (db *DB) isSQLite() bool {
	return db.Engine == "sqlite" || db.Engine == "turso"
}

func New(dataDir, engine string) (*DB, error) {
	if engine == "" {
		engine = "sqlite"
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	var db *sql.DB
	var err error

	switch engine {
	case "sqlite":
		dbPath := filepath.Join(dataDir, "worb.db")
		db, err = sql.Open("sqlite", dbPath+"?_pragma=journal_mode(wal)&_pragma=foreign_keys(1)")
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
	case "turso":
		url := os.Getenv("TURSO_URL")
		if url == "" {
			return nil, fmt.Errorf("TURSO_URL environment variable is required for turso engine")
		}
		token := os.Getenv("TURSO_AUTH_TOKEN")
		dsn := url
		if token != "" {
			dsn += "?authToken=" + token
		}
		db, err = sql.Open("libsql", dsn)
		if err != nil {
			return nil, fmt.Errorf("open turso: %w", err)
		}
	case "duckdb":
		dbPath := filepath.Join(dataDir, "worb.duckdb")
		db, err = sql.Open("duckdb", dbPath)
		if err != nil {
			return nil, fmt.Errorf("open duckdb: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported db engine: %s", engine)
	}

	store := &DB{DB: db, Engine: engine}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return store, nil
}

type QueryResult struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	Truncated bool            `json:"truncated,omitempty"`
}

const maxQueryRows = 10000

func (db *DB) ExecuteQuery(query string) (*QueryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := &QueryResult{Columns: cols}

	for rows.Next() {
		if len(result.Rows) >= maxQueryRows {
			result.Truncated = true
			break
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]interface{}, len(cols))
		for i, v := range vals {
			switch val := v.(type) {
			case []byte:
				row[i] = string(val)
			default:
				row[i] = val
			}
		}
		result.Rows = append(result.Rows, row)
	}

	if result.Rows == nil {
		result.Rows = [][]interface{}{}
	}

	return result, rows.Err()
}

func (db *DB) migrate() error {
	if db.isSQLite() {
		return db.migrateSQLite()
	}
	return db.migrateDuckDB()
}

func (db *DB) migrateSQLite() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS projects (
			id TEXT PRIMARY KEY,
			entity TEXT NOT NULL DEFAULT 'local',
			name TEXT NOT NULL,
			created_at TEXT DEFAULT (datetime('now')),
			UNIQUE(entity, name)
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL REFERENCES projects(id),
			name TEXT NOT NULL,
			display_name TEXT,
			config TEXT,
			summary TEXT,
			state TEXT DEFAULT 'running',
			host TEXT,
			program TEXT,
			git_commit TEXT,
			tags TEXT DEFAULT '[]',
			notes TEXT,
			group_name TEXT,
			job_type TEXT,
			sweep_name TEXT,
			history_line_count INTEGER DEFAULT 0,
			events_line_count INTEGER DEFAULT 0,
			log_line_count INTEGER DEFAULT 0,
			created_at TEXT DEFAULT (datetime('now')),
			updated_at TEXT DEFAULT (datetime('now')),
			heartbeat_at TEXT DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS history (
			run_id TEXT NOT NULL REFERENCES runs(id),
			step INTEGER NOT NULL,
			data TEXT NOT NULL,
			timestamp TEXT DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS system_events (
			run_id TEXT NOT NULL REFERENCES runs(id),
			line_num INTEGER NOT NULL,
			data TEXT NOT NULL,
			timestamp TEXT DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS console_logs (
			run_id TEXT NOT NULL REFERENCES runs(id),
			line_num INTEGER NOT NULL,
			line TEXT NOT NULL,
			stream TEXT DEFAULT 'stdout',
			timestamp TEXT DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS artifacts (
			id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL REFERENCES runs(id),
			type TEXT NOT NULL,
			name TEXT NOT NULL,
			digest TEXT,
			state TEXT DEFAULT 'pending',
			metadata TEXT,
			created_at TEXT DEFAULT (datetime('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS files (
			id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL REFERENCES runs(id),
			name TEXT NOT NULL,
			url TEXT,
			size INTEGER DEFAULT 0,
			created_at TEXT DEFAULT (datetime('now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_history_run_id ON history(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_system_events_run_id ON system_events(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_console_logs_run_id ON console_logs(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_project_id ON runs(project_id)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}

	return nil
}

func (db *DB) migrateDuckDB() error {
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
		`CREATE INDEX IF NOT EXISTS idx_history_run_id ON history(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_system_events_run_id ON system_events(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_console_logs_run_id ON console_logs(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_project_id ON runs(project_id)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}

	db.Exec("DROP TABLE IF EXISTS history_scalars")

	return nil
}
