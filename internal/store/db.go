package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

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
	Engine       string
	wal          *WAL
	dataDir      string
	ctx          context.Context
	cancel       context.CancelFunc
	sqlPerf      *sqlPerfTracker
	storageCache *storageUsageCache
}

type SQLPerfTopQuery struct {
	Query   string  `json:"query"`
	Count   int64   `json:"count"`
	AvgMS   float64 `json:"avgMs"`
	MaxMS   float64 `json:"maxMs"`
	TotalMS float64 `json:"totalMs"`
}

type SQLPerfHistogramBucket struct {
	Label   string  `json:"label"`
	Count   int64   `json:"count"`
	Percent float64 `json:"percent"`
}

type SQLPerfSnapshot struct {
	TotalQueries int64                    `json:"totalQueries"`
	TopQueries   []SQLPerfTopQuery        `json:"topQueries"`
	Histogram    []SQLPerfHistogramBucket `json:"histogram"`
}

type sqlPerfTracker struct {
	mu       sync.Mutex
	total    int64
	byType   map[string]*sqlPerfAgg
	boundsMS []float64
	labels   []string
	counts   []int64
}

type sqlPerfAgg struct {
	count int64
	total float64
	max   float64
}

func newSQLPerfTracker() *sqlPerfTracker {
	labels := []string{
		"<=1ms",
		"<=5ms",
		"<=10ms",
		"<=25ms",
		"<=50ms",
		"<=100ms",
		"<=250ms",
		"<=500ms",
		"<=1s",
		"<=2.5s",
		">2.5s",
	}
	return &sqlPerfTracker{
		boundsMS: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
		labels:   labels,
		counts:   make([]int64, len(labels)),
		byType:   make(map[string]*sqlPerfAgg),
	}
}

func normalizeSQLQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return "<empty>"
	}
	fields := strings.Fields(query)
	query = strings.Join(fields, " ")
	if len(query) > 260 {
		return query[:260] + "..."
	}
	return query
}

func (t *sqlPerfTracker) record(query string, d time.Duration) {
	query = normalizeSQLQuery(query)
	ms := float64(d) / float64(time.Millisecond)
	if ms < 0 {
		ms = 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.total++
	idx := len(t.boundsMS)
	for i, b := range t.boundsMS {
		if ms <= b {
			idx = i
			break
		}
	}
	t.counts[idx]++

	agg, ok := t.byType[query]
	if !ok {
		agg = &sqlPerfAgg{}
		t.byType[query] = agg
	}
	agg.count++
	agg.total += ms
	if ms > agg.max {
		agg.max = ms
	}
}

func (t *sqlPerfTracker) snapshot() SQLPerfSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	top := make([]SQLPerfTopQuery, 0, len(t.byType))
	for typ, agg := range t.byType {
		avg := 0.0
		if agg.count > 0 {
			avg = agg.total / float64(agg.count)
		}
		top = append(top, SQLPerfTopQuery{
			Query:   typ,
			Count:   agg.count,
			AvgMS:   avg,
			MaxMS:   agg.max,
			TotalMS: agg.total,
		})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].AvgMS == top[j].AvgMS {
			return top[i].MaxMS > top[j].MaxMS
		}
		return top[i].AvgMS > top[j].AvgMS
	})
	if len(top) > 10 {
		top = top[:10]
	}
	h := make([]SQLPerfHistogramBucket, len(t.labels))
	var maxCount int64
	for i := range t.counts {
		if t.counts[i] > maxCount {
			maxCount = t.counts[i]
		}
	}
	for i := range t.labels {
		percent := 0.0
		if maxCount > 0 {
			percent = float64(t.counts[i]) * 100.0 / float64(maxCount)
		}
		h[i] = SQLPerfHistogramBucket{
			Label:   t.labels[i],
			Count:   t.counts[i],
			Percent: percent,
		}
	}
	return SQLPerfSnapshot{
		TotalQueries: t.total,
		TopQueries:   top,
		Histogram:    h,
	}
}

func (db *DB) castJSON(col string) string {
	return col
}

func (db *DB) isSQLite() bool {
	return true
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
		db, err = sql.Open("sqlite", dbPath+"?_pragma=journal_mode(wal)&_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)")
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		db.SetMaxOpenConns(32)
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
	default:
		return nil, fmt.Errorf("unsupported db engine: %s", engine)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store := &DB{DB: db, Engine: engine, dataDir: dataDir, ctx: ctx, cancel: cancel, sqlPerf: newSQLPerfTracker(), storageCache: newStorageUsageCache()}
	if err := store.migrate(); err != nil {
		db.Close()
		cancel()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	wal, err := newWAL(dataDir)
	if err != nil {
		db.Close()
		cancel()
		return nil, fmt.Errorf("init wal: %w", err)
	}
	store.wal = wal
	wal.startReader(store)

	store.StartCleanup()
	store.StartStorageUsageRefresh()

	return store, nil
}

func (db *DB) WalStats() WALStats { return db.wal.Stats() }

func (db *DB) Flush() {
	db.wal.flushSync()
}

func (db *DB) Close() error {
	db.wal.flushSync()
	db.wal.close()
	db.cancel()
	return db.DB.Close()
}

func (db *DB) recordSQLPerf(query string, start time.Time) {
	if db.sqlPerf == nil {
		return
	}
	db.sqlPerf.record(query, time.Since(start))
}

func (db *DB) SQLPerfSnapshot() SQLPerfSnapshot {
	if db.sqlPerf == nil {
		return SQLPerfSnapshot{}
	}
	return db.sqlPerf.snapshot()
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := db.DB.Exec(query, args...)
	db.recordSQLPerf(query, start)
	return res, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := db.DB.ExecContext(ctx, query, args...)
	db.recordSQLPerf(query, start)
	return res, err
}

func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := db.DB.Query(query, args...)
	db.recordSQLPerf(query, start)
	return rows, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := db.DB.QueryContext(ctx, query, args...)
	db.recordSQLPerf(query, start)
	return rows, err
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	start := time.Now()
	row := db.DB.QueryRow(query, args...)
	db.recordSQLPerf(query, start)
	return row
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	start := time.Now()
	row := db.DB.QueryRowContext(ctx, query, args...)
	db.recordSQLPerf(query, start)
	return row
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
	return db.migrateSQLite()
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
		`CREATE TABLE IF NOT EXISTS history_scalars (
			run_id TEXT NOT NULL REFERENCES runs(id),
			step INTEGER NOT NULL,
			key TEXT NOT NULL,
			value REAL NOT NULL,
			x_step REAL,
			PRIMARY KEY (run_id, key, step)
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS history_histograms (
			run_id TEXT NOT NULL REFERENCES runs(id),
			step INTEGER NOT NULL,
			key TEXT NOT NULL,
			x_step REAL,
			bins TEXT NOT NULL,
			vals TEXT NOT NULL,
			PRIMARY KEY (run_id, key, step)
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS run_steps (
			run_id TEXT NOT NULL REFERENCES runs(id),
			step INTEGER NOT NULL,
			PRIMARY KEY (run_id, step)
		) WITHOUT ROWID`,
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
		`CREATE INDEX IF NOT EXISTS idx_system_events_run_id ON system_events(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_console_logs_run_id ON console_logs(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_project_id ON runs(project_id)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}

	db.Exec("ALTER TABLE runs ADD COLUMN deleted_at TEXT")
	db.Exec("ALTER TABLE projects ADD COLUMN deleted_at TEXT")
	db.Exec("ALTER TABLE runs ADD COLUMN received_history_count INTEGER DEFAULT 0")

	db.Exec(`CREATE TABLE IF NOT EXISTS run_keys (
		run_id TEXT NOT NULL REFERENCES runs(id),
		key TEXT NOT NULL,
		PRIMARY KEY (run_id, key)
	) WITHOUT ROWID`)

	db.Exec(`CREATE INDEX IF NOT EXISTS idx_history_scalars_run_step ON history_scalars(run_id, step)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_run_steps_run_step ON run_steps(run_id, step)`)

	return nil
}

// activeRunIDs returns the subset of ids that exist and are not deleted.
func (db *DB) activeRunIDs(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := db.Query("SELECT id FROM runs WHERE id IN ("+placeholders+") AND deleted_at IS NULL", args...)
	if err != nil {
		// On error, assume all are active to avoid dropping data
		active := make(map[string]bool, len(ids))
		for _, id := range ids {
			active[id] = true
		}
		return active
	}
	defer rows.Close()
	active := make(map[string]bool)
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			active[id] = true
		}
	}
	return active
}
