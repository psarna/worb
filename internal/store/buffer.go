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

func (db *DB) processScalars(items []scalarItem) error {
	byRun := map[string][]parsedScalar{}
	for _, item := range items {
		byRun[item.runID] = append(byRun[item.runID], item.parsedScalar)
	}
	for runID, scalars := range byRun {
		if err := db.flushScalars(runID, scalars); err != nil {
			log.Printf("[flush] ERROR scalars run=%s: %v", runID[:8], err)
		} else {
			log.Printf("[flush] OK scalars run=%s count=%d", runID[:8], len(scalars))
		}
	}
	return nil
}

func (db *DB) processHistograms(items []histogramItem) error {
	byRun := map[string][]parsedHistogram{}
	for _, item := range items {
		byRun[item.runID] = append(byRun[item.runID], item.parsedHistogram)
	}
	for runID, histograms := range byRun {
		if err := db.flushHistograms(runID, histograms); err != nil {
			log.Printf("[flush] ERROR histograms run=%s: %v", runID[:8], err)
		} else {
			log.Printf("[flush] OK histograms run=%s count=%d", runID[:8], len(histograms))
		}
	}
	return nil
}

func (db *DB) processEvents(items []eventItem) error {
	byRun := map[string][]eventItem{}
	for _, item := range items {
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
	byRun := map[string][]logItem{}
	for _, item := range items {
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
	type key struct {
		runID  string
		column string
	}
	merged := map[key]int{}
	for _, item := range items {
		merged[key{item.runID, item.column}] += item.delta
	}
	for k, delta := range merged {
		if _, err := db.Exec("UPDATE runs SET "+k.column+" = "+k.column+" + ?, updated_at = current_timestamp WHERE id = ?", delta, k.runID); err != nil {
			log.Printf("[flush] ERROR %s run=%s: %v", k.column, k.runID[:8], err)
		}
	}
	return nil
}
