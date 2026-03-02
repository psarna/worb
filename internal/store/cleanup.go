package store

import (
	"database/sql"
	"log"
	"time"
)

// StartCleanup launches a background goroutine that periodically purges
// data belonging to soft-deleted runs and projects.
func (db *DB) StartCleanup() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := db.purgeDeletedData(5000); err != nil {
				log.Printf("cleanup error: %v", err)
			}
		}
	}()
}

// purgeDeletedData removes a batch of data for soft-deleted runs, then
// hard-deletes the run row once empty. Returns true if more work remains.
func (db *DB) purgeDeletedData(batchSize int) (bool, error) {
	// Find one soft-deleted run
	var runID string
	err := db.QueryRow("SELECT id FROM runs WHERE deleted_at IS NOT NULL LIMIT 1").Scan(&runID)
	if err == sql.ErrNoRows {
		// No deleted runs — check for orphaned projects
		return db.purgeDeletedProjects()
	}
	if err != nil {
		return false, err
	}

	// Delete data in batches from each table
	dataTables := []string{"history_scalars", "history_histograms", "system_events", "console_logs", "files", "artifacts"}
	for _, table := range dataTables {
		deleted, err := db.deleteBatch(table, "run_id", runID, batchSize)
		if err != nil {
			return false, err
		}
		if deleted > 0 {
			return true, nil // more work to do
		}
	}

	if _, err := db.Exec("DELETE FROM runs WHERE id = ?", runID); err != nil {
		return false, err
	}
	return true, nil // check for more deleted runs
}

func (db *DB) deleteBatch(table, column, id string, batchSize int) (int64, error) {
	var query string
	switch table {
	case "history_scalars", "history_histograms":
		// WITHOUT ROWID tables — use primary key subquery
		query = "DELETE FROM " + table + " WHERE (" + column + ", key, step) IN (SELECT " + column + ", key, step FROM " + table + " WHERE " + column + " = ? LIMIT ?)"
	default:
		query = "DELETE FROM " + table + " WHERE rowid IN (SELECT rowid FROM " + table + " WHERE " + column + " = ? LIMIT ?)"
	}
	res, err := db.Exec(query, id, batchSize)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// purgeDeletedProjects hard-deletes projects that are soft-deleted and have no remaining runs.
func (db *DB) purgeDeletedProjects() (bool, error) {
	var projectID string
	err := db.QueryRow(`
		SELECT p.id FROM projects p
		WHERE p.deleted_at IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.project_id = p.id)
		LIMIT 1
	`).Scan(&projectID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := db.Exec("DELETE FROM projects WHERE id = ?", projectID); err != nil {
		return false, err
	}
	return true, nil
}
