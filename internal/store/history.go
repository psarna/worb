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

func (db *DB) StreamHistoryScalars(runID string, emit func(ScalarPoint) error) error {
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
			var v float64
			if json.Unmarshal(raw, &v) != nil {
				continue
			}
			if err := emit(ScalarPoint{Key: key, Step: xVal, Value: v, Index: step}); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

type ProjectScalarPoint struct {
	RunID   string  `json:"r"`
	RunName string  `json:"n"`
	Key     string  `json:"k"`
	Step    float64 `json:"s"`
	Value   float64 `json:"v"`
	Index   int     `json:"i"`
}

func (db *DB) StreamProjectHistoryScalars(projectID string, emit func(ProjectScalarPoint) error) error {
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

	for rows.Next() {
		var runID, runName string
		var step int
		var data string
		if err := rows.Scan(&runID, &runName, &step, &data); err != nil {
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
