package store

import (
	"log"
)

type scalarItem struct {
	runID string
	parsedScalar
}

type histogramItem struct {
	runID string
	parsedHistogram
}

type eventItem struct {
	runID   string
	lineNum int
	data    string
}

type logItem struct {
	runID   string
	lineNum int
	line    string
}

type counterDelta struct {
	runID  string
	column string
	delta  int
}

func collectRunIDs[T any](items []T, getRunID func(T) string) []string {
	seen := map[string]bool{}
	var ids []string
	for _, item := range items {
		id := getRunID(item)
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

func (db *DB) processScalars(items []scalarItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i scalarItem) string { return i.runID }))
	byRun := map[string][]parsedScalar{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item.parsedScalar)
	}
	total := 0
	for runID, scalars := range byRun {
		if err := db.flushScalars(runID, scalars); err != nil {
			log.Printf("[flush] ERROR scalars run=%s: %v", runID[:8], err)
		}
		total += len(scalars)
	}
	if total > 0 {
		log.Printf("[flush] OK scalars runs=%d count=%d", len(byRun), total)
	}
	return nil
}

func (db *DB) processHistograms(items []histogramItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i histogramItem) string { return i.runID }))
	byRun := map[string][]parsedHistogram{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item.parsedHistogram)
	}
	total := 0
	for runID, histograms := range byRun {
		if err := db.flushHistograms(runID, histograms); err != nil {
			log.Printf("[flush] ERROR histograms run=%s: %v", runID[:8], err)
		}
		total += len(histograms)
	}
	if total > 0 {
		log.Printf("[flush] OK histograms runs=%d count=%d", len(byRun), total)
	}
	return nil
}

func (db *DB) processEvents(items []eventItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i eventItem) string { return i.runID }))
	byRun := map[string][]eventItem{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item)
	}
	for runID, events := range byRun {
		if err := db.flushEvents(runID, events); err != nil {
			log.Printf("[flush] ERROR events run=%s: %v", runID[:8], err)
		}
	}
	return nil
}

func (db *DB) processLogs(items []logItem) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i logItem) string { return i.runID }))
	byRun := map[string][]logItem{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		byRun[item.runID] = append(byRun[item.runID], item)
	}
	for runID, logs := range byRun {
		if err := db.flushLogs(runID, logs); err != nil {
			log.Printf("[flush] ERROR logs run=%s: %v", runID[:8], err)
		}
	}
	return nil
}

// counterQueries maps counter columns to queries that return the actual count.
// Using absolute counts instead of additive deltas makes re-migration idempotent.
var counterQueries = map[string]string{
	"history_line_count": "SELECT COUNT(DISTINCT step) FROM history_scalars WHERE run_id = ?",
	"events_line_count":  "SELECT COUNT(*) FROM system_events WHERE run_id = ?",
	"log_line_count":     "SELECT COUNT(*) FROM console_logs WHERE run_id = ?",
}

func (db *DB) processCounters(items []counterDelta) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i counterDelta) string { return i.runID }))
	// Collect unique (runID, column) pairs that need updating.
	type key struct {
		runID  string
		column string
	}
	seen := map[key]bool{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		seen[key{item.runID, item.column}] = true
	}
	for k := range seen {
		query, ok := counterQueries[k.column]
		if !ok {
			log.Printf("[flush] WARN unknown counter column %q", k.column)
			continue
		}
		var count int
		if err := db.QueryRow(query, k.runID).Scan(&count); err != nil {
			log.Printf("[flush] ERROR count %s run=%s: %v", k.column, k.runID[:8], err)
			continue
		}
		if _, err := db.Exec("UPDATE runs SET "+k.column+" = ?, updated_at = current_timestamp WHERE id = ?", count, k.runID); err != nil {
			log.Printf("[flush] ERROR %s run=%s: %v", k.column, k.runID[:8], err)
		}
	}
	return nil
}
