package store

import (
	"encoding/json"
	"fmt"
	"sort"
)

func (db *DB) InsertHistory(runID string, step int, data json.RawMessage) error {
	return db.InsertHistoryBatch(runID, []struct {
		Step int
		Data json.RawMessage
	}{{Step: step, Data: data}})
}

func (db *DB) InsertHistoryBatch(runID string, rows []struct {
	Step int
	Data json.RawMessage
}) error {
	if len(rows) == 0 {
		return nil
	}
	db.ingest <- rawPayload{runID: runID, history: rows}
	return nil
}

type parsedScalar struct {
	step  int
	key   string
	value float64
	xStep *float64
}

type parsedHistogram struct {
	step  int
	key   string
	xStep *float64
	bins  string
	vals  string
}

func parseHistoryRows(rows []struct {
	Step int
	Data json.RawMessage
}) ([]parsedScalar, []parsedHistogram) {
	var scalars []parsedScalar
	var histograms []parsedHistogram

	for _, r := range rows {
		var obj map[string]json.RawMessage
		if json.Unmarshal(r.Data, &obj) != nil {
			continue
		}

		var xStep *float64
		if raw, ok := obj["_step"]; ok {
			var v float64
			if json.Unmarshal(raw, &v) == nil {
				xStep = &v
			}
		}

		for key, raw := range obj {
			if len(key) > 0 && key[0] == '_' || key == "step" {
				continue
			}
			var v float64
			if json.Unmarshal(raw, &v) == nil {
				scalars = append(scalars, parsedScalar{step: r.Step, key: key, value: v, xStep: xStep})
				continue
			}
			var histObj struct {
				Type   string    `json:"_type"`
				Bins   []float64 `json:"bins"`
				Values []float64 `json:"values"`
			}
			if json.Unmarshal(raw, &histObj) == nil && histObj.Type == "histogram" && len(histObj.Bins) >= 2 && len(histObj.Values) > 0 {
				binsJSON, _ := json.Marshal(histObj.Bins)
				valsJSON, _ := json.Marshal(histObj.Values)
				histograms = append(histograms, parsedHistogram{step: r.Step, key: key, xStep: xStep, bins: string(binsJSON), vals: string(valsJSON)})
			}
		}
	}
	return scalars, histograms
}

const multiRowSize = 100

func (db *DB) flushScalars(runID string, scalars []parsedScalar) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	for i := 0; i < len(scalars); i += multiRowSize {
		end := i + multiRowSize
		if end > len(scalars) {
			end = len(scalars)
		}
		chunk := scalars[i:end]
		query := "INSERT OR IGNORE INTO history_scalars (run_id, step, key, value, x_step) VALUES "
		args := make([]interface{}, 0, len(chunk)*5)
		for j, s := range chunk {
			if j > 0 {
				query += ","
			}
			query += "(?,?,?,?,?)"
			args = append(args, runID, s.step, s.key, s.value, s.xStep)
		}
		if _, err := tx.Exec(query, args...); err != nil {
			return fmt.Errorf("insert scalars: %w", err)
		}
	}
	return tx.Commit()
}

func (db *DB) flushHistograms(runID string, histograms []parsedHistogram) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	for i := 0; i < len(histograms); i += multiRowSize {
		end := i + multiRowSize
		if end > len(histograms) {
			end = len(histograms)
		}
		chunk := histograms[i:end]
		query := "INSERT OR IGNORE INTO history_histograms (run_id, step, key, x_step, bins, vals) VALUES "
		args := make([]interface{}, 0, len(chunk)*6)
		for j, hh := range chunk {
			if j > 0 {
				query += ","
			}
			query += "(?,?,?,?,?,?)"
			args = append(args, runID, hh.step, hh.key, hh.xStep, hh.bins, hh.vals)
		}
		if _, err := tx.Exec(query, args...); err != nil {
			return fmt.Errorf("insert histograms: %w", err)
		}
	}
	return tx.Commit()
}

func (db *DB) flushEvents(runID string, events []eventItem) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO system_events (run_id, line_num, data) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("prepare events: %w", err)
	}
	defer stmt.Close()
	for _, e := range events {
		if _, err := stmt.Exec(runID, e.lineNum, e.data); err != nil {
			return fmt.Errorf("insert event %d: %w", e.lineNum, err)
		}
	}
	return tx.Commit()
}

func (db *DB) flushLogs(runID string, logs []logItem) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO console_logs (run_id, line_num, line, stream) VALUES (?, ?, ?, 'stdout')")
	if err != nil {
		return fmt.Errorf("prepare logs: %w", err)
	}
	defer stmt.Close()
	for _, l := range logs {
		if _, err := stmt.Exec(runID, l.lineNum, l.line); err != nil {
			return fmt.Errorf("insert log %d: %w", l.lineNum, err)
		}
	}
	return tx.Commit()
}

func (db *DB) GetHistoryKeys(runID string) ([]string, error) {
	rows, err := db.Query("SELECT DISTINCT key FROM history_scalars WHERE run_id = ?", runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := []string{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, rows.Err()
}

type ScalarPoint struct {
	Key   string   `json:"k"`
	Step  float64  `json:"s"`
	Value float64  `json:"v"`
	Min   *float64 `json:"min,omitempty"`
	Max   *float64 `json:"max,omitempty"`
	Index int      `json:"i"`
}

func (db *DB) StreamHistoryScalars(runID string, maxPoints int, filterKeys []string, xMin, xMax *float64, emit func(ScalarPoint) error) error {
	if len(filterKeys) == 0 {
		return nil
	}

	needsBucket := false
	bucketSize := 1
	if maxPoints > 0 {
		var totalRows int
		if xMin != nil || xMax != nil {
			rangeFilter := " WHERE run_id = ?"
			args := []interface{}{runID}
			if xMin != nil {
				rangeFilter += " AND step >= ?"
				args = append(args, int(*xMin))
			}
			if xMax != nil {
				rangeFilter += " AND step <= ?"
				args = append(args, int(*xMax))
			}
			db.QueryRow("SELECT COUNT(DISTINCT step) FROM history_scalars"+rangeFilter, args...).Scan(&totalRows)
		} else {
			db.QueryRow("SELECT history_line_count FROM runs WHERE id = ?", runID).Scan(&totalRows)
		}
		if totalRows > maxPoints {
			needsBucket = true
			bucketSize = totalRows / maxPoints
		}
	}

	for _, key := range filterKeys {
		if err := db.streamKeyScalars(runID, key, needsBucket, bucketSize, xMin, xMax, emit); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) streamKeyScalars(runID, key string, needsBucket bool, bucketSize int, xMin, xMax *float64, emit func(ScalarPoint) error) error {
	args := []interface{}{runID, key}
	stepFilter := ""
	if xMin != nil {
		stepFilter += " AND step >= ?"
		args = append(args, int(*xMin))
	}
	if xMax != nil {
		stepFilter += " AND step <= ?"
		args = append(args, int(*xMax))
	}

	if !needsBucket {
		query := fmt.Sprintf(
			"SELECT COALESCE(x_step, step), value, step FROM history_scalars WHERE run_id = ? AND key = ?%s ORDER BY step",
			stepFilter)
		rows, err := db.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var x, value float64
			var step int
			if err := rows.Scan(&x, &value, &step); err != nil {
				return err
			}
			if err := emit(ScalarPoint{Key: key, Step: x, Value: value, Index: step}); err != nil {
				return err
			}
		}
		return rows.Err()
	}

	query := fmt.Sprintf(
		"SELECT AVG(COALESCE(x_step, step)), AVG(value), MIN(value), MAX(value) FROM history_scalars WHERE run_id = ? AND key = ?%s GROUP BY step / %d ORDER BY step / %d",
		stepFilter, bucketSize, bucketSize)
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var x, avg, mn, mx float64
		if err := rows.Scan(&x, &avg, &mn, &mx); err != nil {
			return err
		}
		mnCopy, mxCopy := mn, mx
		if err := emit(ScalarPoint{Key: key, Step: x, Value: avg, Min: &mnCopy, Max: &mxCopy, Index: int(x)}); err != nil {
			return err
		}
	}
	return rows.Err()
}

type ProjectScalarPoint struct {
	RunID   string   `json:"r"`
	RunName string   `json:"n"`
	Key     string   `json:"k"`
	Step    float64  `json:"s"`
	Value   float64  `json:"v"`
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Index   int      `json:"i"`
}

func (db *DB) StreamProjectHistoryScalars(projectID string, maxPoints int, emit func(ProjectScalarPoint) error) error {
	type runInfo struct {
		id   string
		name string
	}
	infoRows, err := db.Query(`
		SELECT r.id, COALESCE(NULLIF(r.display_name, ''), r.name)
		FROM runs r
		WHERE r.project_id = ? AND r.deleted_at IS NULL
		ORDER BY r.created_at`, projectID)
	if err != nil {
		return err
	}
	var runs []runInfo
	for infoRows.Next() {
		var ri runInfo
		if err := infoRows.Scan(&ri.id, &ri.name); err != nil {
			infoRows.Close()
			return err
		}
		runs = append(runs, ri)
	}
	infoRows.Close()

	needsBucket := false
	bucketSize := 1
	if maxPoints > 0 {
		var maxRunRows int
		db.QueryRow(`SELECT COALESCE(MAX(history_line_count), 0) FROM runs WHERE project_id = ? AND deleted_at IS NULL`, projectID).Scan(&maxRunRows)
		if maxRunRows > maxPoints {
			needsBucket = true
			bucketSize = maxRunRows / maxPoints
		}
	}

	for _, ri := range runs {
		if !needsBucket {
			rows, err := db.Query(`
				SELECT key, COALESCE(x_step, step) as x, value, step
				FROM history_scalars WHERE run_id = ?
				ORDER BY step, key`, ri.id)
			if err != nil {
				return err
			}
			for rows.Next() {
				var key string
				var x, value float64
				var step int
				if err := rows.Scan(&key, &x, &value, &step); err != nil {
					rows.Close()
					return err
				}
				if err := emit(ProjectScalarPoint{RunID: ri.id, RunName: ri.name, Key: key, Step: x, Value: value, Index: step}); err != nil {
					rows.Close()
					return err
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		} else {
			rows, err := db.Query(fmt.Sprintf(`
				SELECT key, AVG(COALESCE(x_step, step)), AVG(value), MIN(value), MAX(value)
				FROM history_scalars WHERE run_id = ?
				GROUP BY key, step / %d
				ORDER BY step / %d, key`, bucketSize, bucketSize), ri.id)
			if err != nil {
				return err
			}
			for rows.Next() {
				var key string
				var x, avg, mn, mx float64
				if err := rows.Scan(&key, &x, &avg, &mn, &mx); err != nil {
					rows.Close()
					return err
				}
				mnCopy, mxCopy := mn, mx
				if err := emit(ProjectScalarPoint{RunID: ri.id, RunName: ri.name, Key: key, Step: x, Value: avg, Min: &mnCopy, Max: &mxCopy, Index: int(x)}); err != nil {
					rows.Close()
					return err
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
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
	rows, err := db.Query("SELECT step, key, x_step, bins, vals FROM history_histograms WHERE run_id = ? ORDER BY step", runID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var step int
		var key, binsStr, valsStr string
		var xStep *float64
		if err := rows.Scan(&step, &key, &xStep, &binsStr, &valsStr); err != nil {
			return err
		}
		x := float64(step)
		if xStep != nil {
			x = *xStep
		}
		var bins, vals []float64
		if json.Unmarshal([]byte(binsStr), &bins) != nil {
			continue
		}
		if json.Unmarshal([]byte(valsStr), &vals) != nil {
			continue
		}
		if err := emit(HistogramPoint{Key: key, Step: x, Bins: bins, Values: vals}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (db *DB) UpdateRunSummary(runID string, summary json.RawMessage) error {
	_, err := db.Exec("UPDATE runs SET summary = ?, updated_at = current_timestamp WHERE id = ?", string(summary), runID)
	return err
}

func (db *DB) InsertSystemEvent(runID string, lineNum int, data json.RawMessage) error {
	db.InsertSystemEventBatch(runID, []struct {
		LineNum int
		Data    json.RawMessage
	}{{LineNum: lineNum, Data: data}})
	return nil
}

func (db *DB) InsertSystemEventBatch(runID string, rows []struct {
	LineNum int
	Data    json.RawMessage
}) error {
	if len(rows) == 0 {
		return nil
	}
	db.ingest <- rawPayload{runID: runID, events: rows}
	return nil
}

func (db *DB) InsertConsoleLog(runID string, lineNum int, line, stream string) error {
	db.InsertConsoleLogBatch(runID, []struct {
		LineNum int
		Line    string
	}{{LineNum: lineNum, Line: line}})
	return nil
}

func (db *DB) InsertConsoleLogBatch(runID string, rows []struct {
	LineNum int
	Line    string
}) error {
	if len(rows) == 0 {
		return nil
	}
	db.ingest <- rawPayload{runID: runID, logs: rows}
	return nil
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
