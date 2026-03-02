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

type writeBuffer struct {
	mu      sync.Mutex
	db      *DB
	history map[string]*pendingHistory
	timer   *time.Timer
}

func newWriteBuffer(db *DB) *writeBuffer {
	return &writeBuffer{
		db:      db,
		history: make(map[string]*pendingHistory),
	}
}

func (b *writeBuffer) Add(runID string, rows []struct {
	Step int
	Data json.RawMessage
}) {
	scalars, histograms := parseHistoryRows(rows)

	b.mu.Lock()
	defer b.mu.Unlock()

	p := b.history[runID]
	if p == nil {
		p = &pendingHistory{}
		b.history[runID] = p
	}
	p.scalars = append(p.scalars, scalars...)
	p.histograms = append(p.histograms, histograms...)
	p.lineCount += len(rows)

	if b.timer == nil {
		b.timer = time.AfterFunc(bufferFlushInterval, b.flushAsync)
	}
}

func (b *writeBuffer) flushAsync() {
	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	toFlush := b.history
	b.history = make(map[string]*pendingHistory)
	b.mu.Unlock()

	b.flush(toFlush)
}

func (b *writeBuffer) flush(batches map[string]*pendingHistory) {
	for runID, p := range batches {
		if err := b.db.insertParsedHistory(runID, p); err != nil {
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
	toFlush := b.history
	b.history = make(map[string]*pendingHistory)
	b.mu.Unlock()

	b.flush(toFlush)
}
