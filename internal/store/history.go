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

func (db *DB) GetHistory(runID string) ([]HistoryRow, error) {
	rows, err := db.Query("SELECT run_id, step, CAST(data AS VARCHAR), timestamp FROM history WHERE run_id = ? ORDER BY step", runID)
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
