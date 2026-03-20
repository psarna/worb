package store

import (
	"encoding/json"
	"testing"
)

func TestFlushScalarsMaintainsRunSteps(t *testing.T) {
	db := setupTestDB(t)

	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "history-steps", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	batch := []struct {
		Step int
		Data json.RawMessage
	}{
		{Step: 1, Data: mustJSON(t, map[string]any{"loss": 1.0, "acc": 0.1})},
		{Step: 2, Data: mustJSON(t, map[string]any{"loss": 2.0, "acc": 0.2})},
		{Step: 2, Data: mustJSON(t, map[string]any{"loss": 2.1, "acc": 0.3})},
		{Step: 4, Data: mustJSON(t, map[string]any{"loss": 4.0})},
	}

	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatalf("InsertHistoryBatch: %v", err)
	}
	db.Flush()

	var stepCount, historyLineCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM run_steps WHERE run_id = ?", run.ID).Scan(&stepCount); err != nil {
		t.Fatalf("run_steps count: %v", err)
	}
	if err := db.QueryRow("SELECT history_line_count FROM runs WHERE id = ?", run.ID).Scan(&historyLineCount); err != nil {
		t.Fatalf("history_line_count: %v", err)
	}
	if stepCount != 3 {
		t.Fatalf("expected 3 unique steps, got %d", stepCount)
	}
	if historyLineCount != 3 {
		t.Fatalf("expected history_line_count=3, got %d", historyLineCount)
	}
}

func TestStreamHistoryScalarsBackfillsRunSteps(t *testing.T) {
	db := setupTestDB(t)

	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "history-backfill", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	batch := []struct {
		Step int
		Data json.RawMessage
	}{
		{Step: 10, Data: mustJSON(t, map[string]any{"loss": 10.0})},
		{Step: 12, Data: mustJSON(t, map[string]any{"loss": 12.0})},
		{Step: 15, Data: mustJSON(t, map[string]any{"loss": 15.0})},
	}

	if err := db.InsertHistoryBatch(run.ID, batch); err != nil {
		t.Fatalf("InsertHistoryBatch: %v", err)
	}
	db.Flush()

	if _, err := db.Exec("DELETE FROM run_steps WHERE run_id = ?", run.ID); err != nil {
		t.Fatalf("clear run_steps: %v", err)
	}
	if _, err := db.Exec("UPDATE runs SET history_line_count = 3 WHERE id = ?", run.ID); err != nil {
		t.Fatalf("restore history_line_count: %v", err)
	}

	xMin := 11.0
	xMax := 15.0
	var points []ScalarPoint
	err = db.StreamHistoryScalars(run.ID, 2, []string{"loss"}, &xMin, &xMax, func(p ScalarPoint) error {
		points = append(points, p)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamHistoryScalars: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("expected 2 points after exact backfill count, got %d", len(points))
	}

	var stepCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM run_steps WHERE run_id = ?", run.ID).Scan(&stepCount); err != nil {
		t.Fatalf("run_steps count: %v", err)
	}
	if stepCount != 3 {
		t.Fatalf("expected 3 backfilled steps, got %d", stepCount)
	}
}

func mustJSON(t *testing.T, v map[string]any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return data
}
