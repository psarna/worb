package store

import (
	"encoding/json"
	"sync"
	"time"
)

const (
	bufferFlushInterval = 500 * time.Millisecond
	bufferFlushSize     = 5000
)

type historyEntry struct {
	Step int
	Data json.RawMessage
}

type writeBuffer struct {
	mu      sync.Mutex
	db      *DB
	history map[string][]historyEntry
	pending int
	timer   *time.Timer
}

func newWriteBuffer(db *DB) *writeBuffer {
	return &writeBuffer{
		db:      db,
		history: make(map[string][]historyEntry),
	}
}

func (b *writeBuffer) Add(runID string, rows []struct {
	Step int
	Data json.RawMessage
}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, r := range rows {
		b.history[runID] = append(b.history[runID], historyEntry{Step: r.Step, Data: r.Data})
		b.pending++
	}

	if b.pending >= bufferFlushSize {
		b.flushLocked()
		return
	}

	if b.timer == nil {
		b.timer = time.AfterFunc(bufferFlushInterval, b.flushFromTimer)
	}
}

func (b *writeBuffer) flushFromTimer() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *writeBuffer) flushLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	if b.pending == 0 {
		return
	}

	toFlush := b.history
	b.history = make(map[string][]historyEntry)
	b.pending = 0

	b.mu.Unlock()
	for runID, entries := range toFlush {
		rows := make([]struct {
			Step int
			Data json.RawMessage
		}, len(entries))
		for i, e := range entries {
			rows[i].Step = e.Step
			rows[i].Data = e.Data
		}
		b.db.insertHistoryBatchDirect(runID, rows)
	}
	b.mu.Lock()
}

func (b *writeBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}
