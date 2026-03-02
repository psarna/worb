package store

import (
	"encoding/json"
	"fmt"
)

type HistoryRow struct {
	RunID     string
	Step      int
	Data      json.RawMessage
	Timestamp string
}

func (db *DB) InsertHistory(runID string, step int, data json.RawMessage) error {
	_, err := db.Exec("INSERT INTO history (run_id, step, data) VALUES (?, ?, ?)", runID, step, string(data))
	if err != nil {
		return fmt.Errorf("insert history: %w", err)
	}
	_, err = db.Exec("UPDATE runs SET history_line_count = history_line_count + 1, updated_at = current_timestamp WHERE id = ?", runID)
	return err
}

func (db *DB) InsertHistoryBatch(runID string, rows []struct {
	Step int
	Data json.RawMessage
}) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO history (run_id, step, data) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(runID, r.Step, string(r.Data)); err != nil {
			return fmt.Errorf("insert history step %d: %w", r.Step, err)
		}
	}

	if _, err := tx.Exec("UPDATE runs SET history_line_count = history_line_count + ?, updated_at = current_timestamp WHERE id = ?", len(rows), runID); err != nil {
		return fmt.Errorf("update line count: %w", err)
	}

	return tx.Commit()
}

func (db *DB) GetHistory(runID string) ([]HistoryRow, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT run_id, step, %s, timestamp FROM history WHERE run_id = ? ORDER BY step", db.castJSON("data")), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []HistoryRow
	for rows.Next() {
		var h HistoryRow
		var data string
		if err := rows.Scan(&h.RunID, &h.Step, &data, &h.Timestamp); err != nil {
			return nil, err
		}
		h.Data = json.RawMessage(data)
		result = append(result, h)
	}
	return result, nil
}

type ScalarPoint struct {
	Key   string  `json:"k"`
	Step  float64 `json:"s"`
	Value float64 `json:"v"`
	Index int     `json:"i"`
}

func (db *DB) StreamHistoryScalars(runID string, maxPoints int, emit func(ScalarPoint) error) error {
	// Determine sampling interval
	every := 1
	if maxPoints > 0 {
		var totalRows int
		if err := db.QueryRow("SELECT COUNT(*) FROM history WHERE run_id = ?", runID).Scan(&totalRows); err != nil {
			return err
		}
		if totalRows > maxPoints {
			every = totalRows / maxPoints
		}
	}

	rows, err := db.Query(fmt.Sprintf("SELECT step, %s FROM history WHERE run_id = ? ORDER BY step", db.castJSON("data")), runID)
	if err != nil {
		return err
	}
	defer rows.Close()

	rowIdx := 0
	var lastStep int
	var lastData string
	hasLast := false
	for rows.Next() {
		var step int
		var data string
		if err := rows.Scan(&step, &data); err != nil {
			return err
		}
		rowIdx++

		if every > 1 && rowIdx != 1 && rowIdx%every != 0 {
			lastStep = step
			lastData = data
			hasLast = true
			continue
		}
		hasLast = false

		if err := emitHistoryRow(step, data, emit); err != nil {
			return err
		}
	}
	// Always emit the last row for good chart boundaries
	if hasLast {
		if err := emitHistoryRow(lastStep, lastData, emit); err != nil {
			return err
		}
	}
	return rows.Err()
}

func emitHistoryRow(step int, data string, emit func(ScalarPoint) error) error {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(data), &obj) != nil {
		return nil
	}

	xVal := float64(step)
	if raw, ok := obj["_step"]; ok {
		var v float64
		if json.Unmarshal(raw, &v) == nil {
			xVal = v
		}
	}

	for key, raw := range obj {
		if len(key) > 0 && key[0] == '_' || key == "step" {
			continue
		}
		var v float64
		if json.Unmarshal(raw, &v) != nil {
			continue
		}
		if err := emit(ScalarPoint{Key: key, Step: xVal, Value: v, Index: step}); err != nil {
			return err
		}
	}
	return nil
}

type ProjectScalarPoint struct {
	RunID   string  `json:"r"`
	RunName string  `json:"n"`
	Key     string  `json:"k"`
	Step    float64 `json:"s"`
	Value   float64 `json:"v"`
	Index   int     `json:"i"`
}

func (db *DB) StreamProjectHistoryScalars(projectID string, maxPoints int, emit func(ProjectScalarPoint) error) error {
	// Determine per-run sampling interval
	every := 1
	if maxPoints > 0 {
		var maxRunRows int
		err := db.QueryRow(`SELECT COALESCE(MAX(cnt), 0) FROM (
			SELECT COUNT(*) as cnt FROM history h
			JOIN runs r ON r.id = h.run_id
			WHERE r.project_id = ?
			GROUP BY h.run_id
		) sub`, projectID).Scan(&maxRunRows)
		if err != nil {
			return err
		}
		if maxRunRows > maxPoints {
			every = maxRunRows / maxPoints
		}
	}

	rows, err := db.Query(fmt.Sprintf(`
		SELECT r.id, COALESCE(NULLIF(r.display_name, ''), r.name) as run_name, h.step, %s
		FROM history h
		JOIN runs r ON r.id = h.run_id
		WHERE r.project_id = ?
		ORDER BY r.created_at, h.step`, db.castJSON("h.data")), projectID)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Track row index per run for sampling
	runRowIdx := map[string]int{}
	runLastRow := map[string][4]string{} // runID, runName, step, data

	for rows.Next() {
		var runID, runName string
		var step int
		var data string
		if err := rows.Scan(&runID, &runName, &step, &data); err != nil {
			return err
		}

		runRowIdx[runID]++
		idx := runRowIdx[runID]

		if every > 1 && idx != 1 && idx%every != 0 {
			runLastRow[runID] = [4]string{runID, runName, fmt.Sprintf("%d", step), data}
			continue
		}
		delete(runLastRow, runID)

		if err := emitProjectHistoryRow(runID, runName, step, data, emit); err != nil {
			return err
		}
	}
	// Emit last row of each run
	for _, last := range runLastRow {
		var step int
		fmt.Sscanf(last[2], "%d", &step)
		if err := emitProjectHistoryRow(last[0], last[1], step, last[3], emit); err != nil {
			return err
		}
	}
	return rows.Err()
}

func emitProjectHistoryRow(runID, runName string, step int, data string, emit func(ProjectScalarPoint) error) error {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(data), &obj) != nil {
		return nil
	}

	xVal := float64(step)
	if raw, ok := obj["_step"]; ok {
		var v float64
		if json.Unmarshal(raw, &v) == nil {
			xVal = v
		}
	}

	for key, raw := range obj {
		if len(key) > 0 && key[0] == '_' || key == "step" {
			continue
		}
		var v float64
		if json.Unmarshal(raw, &v) != nil {
			continue
		}
		if err := emit(ProjectScalarPoint{
			RunID:   runID,
			RunName: runName,
			Key:     key,
			Step:    xVal,
			Value:   v,
			Index:   step,
		}); err != nil {
			return err
		}
	}
	return nil
}

type HistogramPoint struct {
	Key    string    `json:"k"`
	Step   float64   `json:"s"`
	Bins   []float64 `json:"b"`
	Values []float64 `json:"v"`
}

func (db *DB) StreamHistoryHistograms(runID string, emit func(HistogramPoint) error) error {
	rows, err := db.Query(fmt.Sprintf("SELECT step, %s FROM history WHERE run_id = ? ORDER BY step", db.castJSON("data")), runID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var step int
		var data string
		if err := rows.Scan(&step, &data); err != nil {
			return err
		}

		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(data), &obj) != nil {
			continue
		}

		xVal := float64(step)
		if raw, ok := obj["_step"]; ok {
			var v float64
			if json.Unmarshal(raw, &v) == nil {
				xVal = v
			}
		}

		for key, raw := range obj {
			if len(key) > 0 && key[0] == '_' || key == "step" {
				continue
			}
			var histObj struct {
				Type   string    `json:"_type"`
				Bins   []float64 `json:"bins"`
				Values []float64 `json:"values"`
			}
			if json.Unmarshal(raw, &histObj) != nil {
				continue
			}
			if histObj.Type != "histogram" || len(histObj.Bins) < 2 || len(histObj.Values) == 0 {
				continue
			}
			if err := emit(HistogramPoint{Key: key, Step: xVal, Bins: histObj.Bins, Values: histObj.Values}); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

func (db *DB) UpdateRunSummary(runID string, summary json.RawMessage) error {
	_, err := db.Exec("UPDATE runs SET summary = ?, updated_at = current_timestamp WHERE id = ?", string(summary), runID)
	return err
}

func (db *DB) InsertSystemEvent(runID string, lineNum int, data json.RawMessage) error {
	_, err := db.Exec("INSERT INTO system_events (run_id, line_num, data) VALUES (?, ?, ?)", runID, lineNum, string(data))
	if err != nil {
		return fmt.Errorf("insert system event: %w", err)
	}
	_, err = db.Exec("UPDATE runs SET events_line_count = events_line_count + 1, updated_at = current_timestamp WHERE id = ?", runID)
	return err
}

func (db *DB) InsertSystemEventBatch(runID string, rows []struct {
	LineNum int
	Data    json.RawMessage
}) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO system_events (run_id, line_num, data) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(runID, r.LineNum, string(r.Data)); err != nil {
			return fmt.Errorf("insert event %d: %w", r.LineNum, err)
		}
	}

	if _, err := tx.Exec("UPDATE runs SET events_line_count = events_line_count + ?, updated_at = current_timestamp WHERE id = ?", len(rows), runID); err != nil {
		return fmt.Errorf("update events count: %w", err)
	}

	return tx.Commit()
}

func (db *DB) InsertConsoleLog(runID string, lineNum int, line, stream string) error {
	if stream == "" {
		stream = "stdout"
	}
	_, err := db.Exec("INSERT INTO console_logs (run_id, line_num, line, stream) VALUES (?, ?, ?, ?)", runID, lineNum, line, stream)
	if err != nil {
		return fmt.Errorf("insert console log: %w", err)
	}
	_, err = db.Exec("UPDATE runs SET log_line_count = log_line_count + 1, updated_at = current_timestamp WHERE id = ?", runID)
	return err
}

func (db *DB) InsertConsoleLogBatch(runID string, rows []struct {
	LineNum int
	Line    string
}) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO console_logs (run_id, line_num, line, stream) VALUES (?, ?, ?, 'stdout')")
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(runID, r.LineNum, r.Line); err != nil {
			return fmt.Errorf("insert log %d: %w", r.LineNum, err)
		}
	}

	if _, err := tx.Exec("UPDATE runs SET log_line_count = log_line_count + ?, updated_at = current_timestamp WHERE id = ?", len(rows), runID); err != nil {
		return fmt.Errorf("update log count: %w", err)
	}

	return tx.Commit()
}

func (db *DB) GetConsoleLogs(runID string) ([]string, error) {
	rows, err := db.Query("SELECT line FROM console_logs WHERE run_id = ? ORDER BY line_num", runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, nil
}
