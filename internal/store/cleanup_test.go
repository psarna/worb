package store

import (
	"encoding/json"
	"testing"
)

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := New(dir, "sqlite")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPurgeDeletedRun(t *testing.T) {
	db := setupTestDB(t)

	// Create a project and run with 200 history rows
	proj, err := db.EnsureProject("local", "cleanup-test")
	if err != nil {
		t.Fatalf("EnsureProject: %v", err)
	}
	run, err := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "cleanup-test", Name: "run1"})
	if err != nil {
		t.Fatalf("UpsertRun: %v", err)
	}

	batch := make([]struct {
		Step int
		Data json.RawMessage
	}, 200)
	for i := range batch {
		data, _ := json.Marshal(map[string]interface{}{"_step": float64(i), "loss": float64(i) * 0.1})
		batch[i] = struct {
			Step int
			Data json.RawMessage
		}{Step: i, Data: data}
	}
	db.InsertHistoryBatch(run.ID, batch)
	db.buf.flushAsync()

	// Verify data is there
	var scalarCount int
	db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ?", run.ID).Scan(&scalarCount)
	if scalarCount != 200 {
		t.Fatalf("expected 200 scalars, got %d", scalarCount)
	}

	// Soft-delete the run
	if err := db.DeleteRun(run.ID); err != nil {
		t.Fatalf("DeleteRun: %v", err)
	}

	// Run should be hidden from queries
	runs, _ := db.ListRuns(proj.ID)
	if len(runs) != 0 {
		t.Errorf("expected 0 visible runs, got %d", len(runs))
	}

	// Purge with small batch size — should take multiple rounds
	batchSize := 50
	rounds := 0
	for {
		more, err := db.purgeDeletedData(batchSize)
		if err != nil {
			t.Fatalf("purgeDeletedData round %d: %v", rounds, err)
		}
		rounds++
		if !more {
			break
		}
		if rounds > 20 {
			t.Fatal("too many purge rounds")
		}
	}

	t.Logf("purged in %d rounds (batch=%d, scalars=%d)", rounds, batchSize, scalarCount)

	// Verify everything is gone
	db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ?", run.ID).Scan(&scalarCount)
	if scalarCount != 0 {
		t.Errorf("expected 0 scalars after purge, got %d", scalarCount)
	}

	var runCount int
	db.QueryRow("SELECT COUNT(*) FROM runs WHERE id = ?", run.ID).Scan(&runCount)
	if runCount != 0 {
		t.Errorf("expected run to be hard-deleted, got %d", runCount)
	}
}

func TestPurgeDeletedProject(t *testing.T) {
	db := setupTestDB(t)

	proj, err := db.EnsureProject("local", "proj-cleanup")
	if err != nil {
		t.Fatalf("EnsureProject: %v", err)
	}

	// Create two runs with data
	for _, name := range []string{"run-a", "run-b"} {
		run, _ := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "proj-cleanup", Name: name})
		batch := make([]struct {
			Step int
			Data json.RawMessage
		}, 100)
		for i := range batch {
			data, _ := json.Marshal(map[string]interface{}{"_step": float64(i), "acc": float64(i)})
			batch[i] = struct {
				Step int
				Data json.RawMessage
			}{Step: i, Data: data}
		}
		db.InsertHistoryBatch(run.ID, batch)
	}
	db.buf.flushAsync()

	// Delete the project (soft-deletes project + all runs)
	if err := db.DeleteProject(proj.ID); err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}

	// Project should be hidden
	projects, _ := db.ListAllProjects()
	for _, p := range projects {
		if p.ID == proj.ID {
			t.Error("deleted project still visible in ListAllProjects")
		}
	}

	// Purge until done
	rounds := 0
	for {
		more, err := db.purgeDeletedData(50)
		if err != nil {
			t.Fatalf("purgeDeletedData round %d: %v", rounds, err)
		}
		rounds++
		if !more {
			break
		}
		if rounds > 50 {
			t.Fatal("too many purge rounds")
		}
	}

	t.Logf("project purged in %d rounds", rounds)

	// Verify everything is gone
	var runCount int
	db.QueryRow("SELECT COUNT(*) FROM runs WHERE project_id = ?", proj.ID).Scan(&runCount)
	if runCount != 0 {
		t.Errorf("expected 0 runs after purge, got %d", runCount)
	}

	var projCount int
	db.QueryRow("SELECT COUNT(*) FROM projects WHERE id = ?", proj.ID).Scan(&projCount)
	if projCount != 0 {
		t.Errorf("expected project to be hard-deleted, got %d", projCount)
	}
}

func TestPurgeBatchSizeRespected(t *testing.T) {
	db := setupTestDB(t)

	run, _ := db.UpsertRun(UpsertRunParams{Entity: "local", Project: "batch-test", Name: "run1"})

	// Insert 500 scalar rows
	batch := make([]struct {
		Step int
		Data json.RawMessage
	}, 500)
	for i := range batch {
		data, _ := json.Marshal(map[string]interface{}{"_step": float64(i), "loss": float64(i)})
		batch[i] = struct {
			Step int
			Data json.RawMessage
		}{Step: i, Data: data}
	}
	db.InsertHistoryBatch(run.ID, batch)
	db.buf.flushAsync()

	// Soft-delete
	db.DeleteRun(run.ID)

	// One purge call with batchSize=100 should delete exactly 100 scalars
	more, err := db.purgeDeletedData(100)
	if err != nil {
		t.Fatalf("purgeDeletedData: %v", err)
	}
	if !more {
		t.Error("expected more=true, there should be remaining data")
	}

	var remaining int
	db.QueryRow("SELECT COUNT(*) FROM history_scalars WHERE run_id = ?", run.ID).Scan(&remaining)
	if remaining != 400 {
		t.Errorf("expected 400 remaining scalars after batch of 100, got %d", remaining)
	}
}

func TestPurgeNoDeletedData(t *testing.T) {
	db := setupTestDB(t)

	// No deleted runs/projects — purge should be a no-op
	more, err := db.purgeDeletedData(5000)
	if err != nil {
		t.Fatalf("purgeDeletedData: %v", err)
	}
	if more {
		t.Error("expected more=false when nothing is deleted")
	}
}
