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

func (db *DB) processCounters(items []counterDelta) error {
	active := db.activeRunIDs(collectRunIDs(items, func(i counterDelta) string { return i.runID }))
	type key struct {
		runID  string
		column string
	}
	merged := map[key]int{}
	for _, item := range items {
		if !active[item.runID] {
			continue
		}
		merged[key{item.runID, item.column}] += item.delta
	}
	for k, delta := range merged {
		if _, err := db.Exec("UPDATE runs SET "+k.column+" = "+k.column+" + ?, updated_at = current_timestamp WHERE id = ?", delta, k.runID); err != nil {
			log.Printf("[flush] ERROR %s run=%s: %v", k.column, k.runID[:8], err)
		}
	}
	return nil
}
