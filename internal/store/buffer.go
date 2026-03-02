package store

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

const (
	bufferFlushInterval = 500 * time.Millisecond
)

type pendingHistory struct {
	scalars    []parsedScalar
	histograms []parsedHistogram
	lineCount  int
}

type pendingEvent struct {
	lineNum int
	data    string
}

type pendingLog struct {
	lineNum int
	line    string
}

type pendingData struct {
	history *pendingHistory
	events  []pendingEvent
	logs    []pendingLog
}

type writeBuffer struct {
	mu    sync.Mutex
	db    *DB
	runs  map[string]*pendingData
	timer *time.Timer
}

func newWriteBuffer(db *DB) *writeBuffer {
	return &writeBuffer{
		db:   db,
		runs: make(map[string]*pendingData),
	}
}

func (b *writeBuffer) pending(runID string) *pendingData {
	p := b.runs[runID]
	if p == nil {
		p = &pendingData{}
		b.runs[runID] = p
	}
	return p
}

func (b *writeBuffer) ensureTimer() {
	if b.timer == nil {
		b.timer = time.AfterFunc(bufferFlushInterval, b.flushAsync)
	}
}

func (b *writeBuffer) Add(runID string, rows []struct {
	Step int
	Data json.RawMessage
}) {
	scalars, histograms := parseHistoryRows(rows)

	b.mu.Lock()
	defer b.mu.Unlock()

	p := b.pending(runID)
	if p.history == nil {
		p.history = &pendingHistory{}
	}
	p.history.scalars = append(p.history.scalars, scalars...)
	p.history.histograms = append(p.history.histograms, histograms...)
	p.history.lineCount += len(rows)

	b.ensureTimer()
}

func (b *writeBuffer) AddEvents(runID string, rows []struct {
	LineNum int
	Data    json.RawMessage
}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p := b.pending(runID)
	for _, r := range rows {
		p.events = append(p.events, pendingEvent{lineNum: r.LineNum, data: string(r.Data)})
	}

	b.ensureTimer()
}

func (b *writeBuffer) AddLogs(runID string, rows []struct {
	LineNum int
	Line    string
}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p := b.pending(runID)
	for _, r := range rows {
		p.logs = append(p.logs, pendingLog{lineNum: r.LineNum, line: r.Line})
	}

	b.ensureTimer()
}

func (b *writeBuffer) flushAsync() {
	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	toFlush := b.runs
	b.runs = make(map[string]*pendingData)
	b.mu.Unlock()

	b.flush(toFlush)
}

func (b *writeBuffer) flush(batches map[string]*pendingData) {
	for runID, p := range batches {
		b.db.writeMu.Lock()
		err := b.db.flushRunData(runID, p)
		b.db.writeMu.Unlock()
		if err != nil {
			log.Printf("buffer flush %s: %v", runID, err)
		}
	}
}

func (b *writeBuffer) Close() {
	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	toFlush := b.runs
	b.runs = make(map[string]*pendingData)
	b.mu.Unlock()

	b.flush(toFlush)
}
