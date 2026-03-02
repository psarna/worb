package store

import (
	"encoding/json"
	"log"
)

type rawPayload struct {
	runID   string
	history []struct {
		Step int
		Data json.RawMessage
	}
	events []struct {
		LineNum int
		Data    json.RawMessage
	}
	logs []struct {
		LineNum int
		Line    string
	}
}

func (db *DB) processIngest(payloads []rawPayload) error {
	for _, p := range payloads {
		if len(p.history) > 0 {
			scalars, histograms := parseHistoryRows(p.history)
			for _, s := range scalars {
				db.scalars <- scalarItem{runID: p.runID, parsedScalar: s}
			}
			for _, h := range histograms {
				db.histograms <- histogramItem{runID: p.runID, parsedHistogram: h}
			}
			db.counters <- counterDelta{runID: p.runID, column: "history_line_count", delta: len(p.history)}
		}
		for _, e := range p.events {
			db.events <- eventItem{runID: p.runID, lineNum: e.LineNum, data: string(e.Data)}
		}
		if len(p.events) > 0 {
			db.counters <- counterDelta{runID: p.runID, column: "events_line_count", delta: len(p.events)}
		}
		for _, l := range p.logs {
			db.logs <- logItem{runID: p.runID, lineNum: l.LineNum, line: l.Line}
		}
		if len(p.logs) > 0 {
			db.counters <- counterDelta{runID: p.runID, column: "log_line_count", delta: len(p.logs)}
		}
	}
	return nil
}

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
