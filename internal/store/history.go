package store

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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

func (db *DB) GetHistoryKeys(runID string) ([]string, error) {
	keySet := map[string]struct{}{}
	for _, query := range []string{
		fmt.Sprintf("SELECT %s FROM history WHERE run_id = ? ORDER BY step ASC LIMIT 1", db.castJSON("data")),
		fmt.Sprintf("SELECT %s FROM history WHERE run_id = ? ORDER BY step DESC LIMIT 1", db.castJSON("data")),
	} {
		var data string
		err := db.QueryRow(query, runID).Scan(&data)
		if err != nil {
			continue
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(data), &obj) != nil {
			continue
		}
		for key := range obj {
			if len(key) > 0 && key[0] == '_' || key == "step" {
				continue
			}
			var v float64
			if json.Unmarshal(obj[key], &v) == nil {
				keySet[key] = struct{}{}
			}
		}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

type ScalarPoint struct {
	Key   string   `json:"k"`
	Step  float64  `json:"s"`
	Value float64  `json:"v"`
	Min   *float64 `json:"min,omitempty"`
	Max   *float64 `json:"max,omitempty"`
	Index int      `json:"i"`
}

const aggTargetResolution = 2000

func (db *DB) StreamHistoryScalars(runID string, maxPoints int, filterKeys []string, xMin, xMax *float64, emit func(ScalarPoint) error) error {
	if len(filterKeys) > 0 && db.isSQLite() {
		return db.streamHistoryScalarsFast(runID, maxPoints, filterKeys, xMin, xMax, emit)
	}
	return db.streamHistoryScalarsRegular(runID, maxPoints, filterKeys, emit)
}

func (db *DB) streamHistoryScalarsRegular(runID string, maxPoints int, filterKeys []string, emit func(ScalarPoint) error) error {
	var keySet map[string]bool
	if len(filterKeys) > 0 {
		keySet = make(map[string]bool, len(filterKeys))
		for _, k := range filterKeys {
			keySet[k] = true
		}
	}

	var totalRows int
	needsBucket := false
	bucketSize := 1
	if maxPoints > 0 {
		if err := db.QueryRow("SELECT COUNT(*) FROM history WHERE run_id = ?", runID).Scan(&totalRows); err != nil {
			return err
		}
		if totalRows > maxPoints {
			needsBucket = true
			bucketSize = totalRows / maxPoints
		}
	}

	rows, err := db.Query(fmt.Sprintf("SELECT step, %s FROM history WHERE run_id = ? ORDER BY step", db.castJSON("data")), runID)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !needsBucket {
		for rows.Next() {
			var step int
			var data string
			if err := rows.Scan(&step, &data); err != nil {
				return err
			}
			if err := emitHistoryRow(step, data, keySet, emit); err != nil {
				return err
			}
		}
		return rows.Err()
	}

	accum := map[string]*metricAccum{}
	bucketStepSum := 0.0
	bucketStepCount := 0
	rowIdx := 0

	flushBucket := func() error {
		if bucketStepCount == 0 {
			return nil
		}
		midStep := bucketStepSum / float64(bucketStepCount)
		for key, a := range accum {
			avg := a.sum / float64(a.count)
			mn := a.min
			mx := a.max
			if err := emit(ScalarPoint{Key: key, Step: midStep, Value: avg, Min: &mn, Max: &mx, Index: int(midStep)}); err != nil {
				return err
			}
		}
		for k := range accum {
			delete(accum, k)
		}
		bucketStepSum = 0
		bucketStepCount = 0
		return nil
	}

	for rows.Next() {
		var step int
		var data string
		if err := rows.Scan(&step, &data); err != nil {
			return err
		}

		if rowIdx > 0 && rowIdx%bucketSize == 0 {
			if err := flushBucket(); err != nil {
				return err
			}
		}

		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(data), &obj) != nil {
			rowIdx++
			continue
		}

		xVal := float64(step)
		if raw, ok := obj["_step"]; ok {
			var v float64
			if json.Unmarshal(raw, &v) == nil {
				xVal = v
			}
		}

		bucketStepSum += xVal
		bucketStepCount++

		for key, raw := range obj {
			if len(key) > 0 && key[0] == '_' || key == "step" {
				continue
			}
			if keySet != nil && !keySet[key] {
				continue
			}
			var v float64
			if json.Unmarshal(raw, &v) != nil {
				continue
			}
			a, ok := accum[key]
			if !ok {
				accum[key] = &metricAccum{sum: v, min: v, max: v, count: 1}
			} else {
				a.sum += v
				a.count++
				if v < a.min {
					a.min = v
				}
				if v > a.max {
					a.max = v
				}
			}
		}
		rowIdx++
	}
	if err := flushBucket(); err != nil {
		return err
	}
	return rows.Err()
}

func (db *DB) streamHistoryScalarsFast(runID string, maxPoints int, filterKeys []string, xMin, xMax *float64, emit func(ScalarPoint) error) error {
	var totalRows int
	if err := db.QueryRow("SELECT COUNT(*) FROM history WHERE run_id = ?", runID).Scan(&totalRows); err != nil {
		return err
	}

	// When x range is set, count rows in range (using step column as approximation)
	// to decide whether downsampling is needed for the visible portion
	effectiveRows := totalRows
	hasRange := xMin != nil || xMax != nil
	if hasRange && maxPoints > 0 {
		countQuery := "SELECT COUNT(*) FROM history WHERE run_id = ?"
		countArgs := []interface{}{runID}
		if xMin != nil {
			countQuery += " AND step >= ?"
			countArgs = append(countArgs, int(*xMin))
		}
		if xMax != nil {
			countQuery += " AND step <= ?"
			countArgs = append(countArgs, int(*xMax))
		}
		db.QueryRow(countQuery, countArgs...).Scan(&effectiveRows)
	}

	if maxPoints == 0 || effectiveRows <= maxPoints {
		// No downsampling needed — emit individual points via json_extract
		cols := "step, json_extract(data, '$._step')"
		for _, k := range filterKeys {
			cols += fmt.Sprintf(`, json_extract(data, '$."%s"')`, k)
		}
		// Use indexed step column in WHERE for speed; exact _step filtering in Go
		where := "run_id = ?"
		args := []interface{}{runID}
		if xMin != nil {
			where += " AND step >= ?"
			args = append(args, int(*xMin))
		}
		if xMax != nil {
			where += " AND step <= ?"
			args = append(args, int(*xMax))
		}
		query := fmt.Sprintf("SELECT %s FROM history WHERE %s ORDER BY step", cols, where)

		rows, err := db.Query(query, args...)
		if err != nil {
			return db.streamHistoryScalarsRegular(runID, maxPoints, filterKeys, emit)
		}
		defer rows.Close()

		nCols := 2 + len(filterKeys)
		scanDest := make([]interface{}, nCols)
		var step int
		var xStepRaw *float64
		scanDest[0] = &step
		scanDest[1] = &xStepRaw
		valPtrs := make([]*float64, len(filterKeys))
		for i := range filterKeys {
			scanDest[2+i] = &valPtrs[i]
		}

		for rows.Next() {
			if err := rows.Scan(scanDest...); err != nil {
				return err
			}
			xVal := float64(step)
			if xStepRaw != nil {
				xVal = *xStepRaw
			}
			// Exact range check on the actual _step value
			if xMin != nil && xVal < *xMin {
				continue
			}
			if xMax != nil && xVal > *xMax {
				continue
			}
			for i, key := range filterKeys {
				if valPtrs[i] == nil {
					continue
				}
				if err := emit(ScalarPoint{Key: key, Step: xVal, Value: *valPtrs[i], Index: step}); err != nil {
					return err
				}
			}
		}
		return rows.Err()
	}

	// Downsampling needed — use cached agg table
	var aggLineCount int
	db.QueryRow("SELECT line_count FROM history_agg_meta WHERE run_id = ?", runID).Scan(&aggLineCount)

	if aggLineCount != totalRows {
		if err := db.rebuildHistoryAgg(runID, totalRows); err != nil {
			// Fall back to regular path on rebuild failure
			return db.streamHistoryScalarsRegular(runID, maxPoints, filterKeys, emit)
		}
	}

	foundKeys, err := db.queryHistoryAgg(runID, maxPoints, filterKeys, xMin, xMax, emit)
	if err != nil {
		return err
	}

	// Backfill any keys missing from the agg table
	var missingKeys []string
	for _, k := range filterKeys {
		if !foundKeys[k] {
			missingKeys = append(missingKeys, k)
		}
	}
	if len(missingKeys) > 0 {
		if err := db.backfillHistoryAgg(runID, missingKeys); err == nil {
			if _, err := db.queryHistoryAgg(runID, maxPoints, missingKeys, xMin, xMax, emit); err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *DB) rebuildHistoryAgg(runID string, lineCount int) error {
	// Collect all points using the regular bucket aggregation path
	var points []ScalarPoint
	if err := db.streamHistoryScalarsRegular(runID, aggTargetResolution, nil, func(p ScalarPoint) error {
		points = append(points, p)
		return nil
	}); err != nil {
		return fmt.Errorf("rebuild agg: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM history_scalar_agg WHERE run_id = ?", runID); err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO history_scalar_agg (run_id, key, bucket, step, value, min_value, max_value) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	bucketNum := map[string]int{}
	for _, p := range points {
		b := bucketNum[p.Key]
		bucketNum[p.Key] = b + 1
		mn, mx := p.Value, p.Value
		if p.Min != nil {
			mn = *p.Min
		}
		if p.Max != nil {
			mx = *p.Max
		}
		if _, err := stmt.Exec(runID, p.Key, b, p.Step, p.Value, mn, mx); err != nil {
			return err
		}
	}

	if _, err := tx.Exec("INSERT OR REPLACE INTO history_agg_meta (run_id, line_count) VALUES (?, ?)", runID, lineCount); err != nil {
		return err
	}

	return tx.Commit()
}

func (db *DB) queryHistoryAgg(runID string, maxPoints int, filterKeys []string, xMin, xMax *float64, emit func(ScalarPoint) error) (map[string]bool, error) {
	placeholders := make([]string, len(filterKeys))
	args := make([]interface{}, 0, 1+len(filterKeys))
	args = append(args, runID)
	for i, k := range filterKeys {
		placeholders[i] = "?"
		args = append(args, k)
	}
	inClause := strings.Join(placeholders, ",")

	// Step range filter (agg table step is in _step coordinate space)
	stepFilter := ""
	if xMin != nil {
		stepFilter += " AND step >= ?"
		args = append(args, *xMin)
	}
	if xMax != nil {
		stepFilter += " AND step <= ?"
		args = append(args, *xMax)
	}

	factor := 1
	if maxPoints > 0 && aggTargetResolution > maxPoints {
		factor = aggTargetResolution / maxPoints
	}

	var query string
	if factor <= 1 {
		query = fmt.Sprintf(
			"SELECT key, step, value, min_value, max_value FROM history_scalar_agg WHERE run_id = ? AND key IN (%s)%s ORDER BY key, bucket",
			inClause, stepFilter)
	} else {
		query = fmt.Sprintf(
			"SELECT key, AVG(step), AVG(value), MIN(min_value), MAX(max_value) FROM history_scalar_agg WHERE run_id = ? AND key IN (%s)%s GROUP BY key, bucket / %d ORDER BY key, bucket / %d",
			inClause, stepFilter, factor, factor)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	foundKeys := make(map[string]bool)
	for rows.Next() {
		var key string
		var step, value, mn, mx float64
		if err := rows.Scan(&key, &step, &value, &mn, &mx); err != nil {
			return nil, err
		}
		foundKeys[key] = true
		mnCopy, mxCopy := mn, mx
		if err := emit(ScalarPoint{
			Key: key, Step: step, Value: value,
			Min: &mnCopy, Max: &mxCopy, Index: int(step),
		}); err != nil {
			return nil, err
		}
	}
	return foundKeys, rows.Err()
}

func (db *DB) backfillHistoryAgg(runID string, keys []string) error {
	var points []ScalarPoint
	if err := db.streamHistoryScalarsRegular(runID, aggTargetResolution, keys, func(p ScalarPoint) error {
		points = append(points, p)
		return nil
	}); err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO history_scalar_agg (run_id, key, bucket, step, value, min_value, max_value) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	bucketNum := map[string]int{}
	for _, p := range points {
		b := bucketNum[p.Key]
		bucketNum[p.Key] = b + 1
		mn, mx := p.Value, p.Value
		if p.Min != nil {
			mn = *p.Min
		}
		if p.Max != nil {
			mx = *p.Max
		}
		if _, err := stmt.Exec(runID, p.Key, b, p.Step, p.Value, mn, mx); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func emitHistoryRow(step int, data string, keySet map[string]bool, emit func(ScalarPoint) error) error {
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
		if keySet != nil && !keySet[key] {
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
	RunID   string   `json:"r"`
	RunName string   `json:"n"`
	Key     string   `json:"k"`
	Step    float64  `json:"s"`
	Value   float64  `json:"v"`
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Index   int      `json:"i"`
}

type metricAccum struct {
	sum   float64
	min   float64
	max   float64
	count int
}

func (db *DB) StreamProjectHistoryScalars(projectID string, maxPoints int, emit func(ProjectScalarPoint) error) error {
	needsBucket := false
	bucketSize := 1
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
			needsBucket = true
			bucketSize = maxRunRows / maxPoints
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

	if !needsBucket {
		for rows.Next() {
			var runID, runName string
			var step int
			var data string
			if err := rows.Scan(&runID, &runName, &step, &data); err != nil {
				return err
			}
			if err := emitProjectHistoryRow(runID, runName, step, data, emit); err != nil {
				return err
			}
		}
		return rows.Err()
	}

	// Per-run bucket aggregation
	type runBucket struct {
		runName       string
		accum         map[string]*metricAccum
		stepSum       float64
		stepCount     int
		rowIdx        int
	}
	runBuckets := map[string]*runBucket{}

	flushRunBucket := func(runID string, rb *runBucket) error {
		if rb.stepCount == 0 {
			return nil
		}
		midStep := rb.stepSum / float64(rb.stepCount)
		for key, a := range rb.accum {
			avg := a.sum / float64(a.count)
			mn := a.min
			mx := a.max
			if err := emit(ProjectScalarPoint{
				RunID: runID, RunName: rb.runName,
				Key: key, Step: midStep, Value: avg, Min: &mn, Max: &mx, Index: int(midStep),
			}); err != nil {
				return err
			}
		}
		rb.accum = map[string]*metricAccum{}
		rb.stepSum = 0
		rb.stepCount = 0
		return nil
	}

	for rows.Next() {
		var runID, runName string
		var step int
		var data string
		if err := rows.Scan(&runID, &runName, &step, &data); err != nil {
			return err
		}

		rb, ok := runBuckets[runID]
		if !ok {
			rb = &runBucket{runName: runName, accum: map[string]*metricAccum{}}
			runBuckets[runID] = rb
		}

		// Flush bucket when full
		if rb.rowIdx > 0 && rb.rowIdx%bucketSize == 0 {
			if err := flushRunBucket(runID, rb); err != nil {
				return err
			}
		}

		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(data), &obj) != nil {
			rb.rowIdx++
			continue
		}

		xVal := float64(step)
		if raw, ok := obj["_step"]; ok {
			var v float64
			if json.Unmarshal(raw, &v) == nil {
				xVal = v
			}
		}

		rb.stepSum += xVal
		rb.stepCount++

		for key, raw := range obj {
			if len(key) > 0 && key[0] == '_' || key == "step" {
				continue
			}
			var v float64
			if json.Unmarshal(raw, &v) != nil {
				continue
			}
			a, exists := rb.accum[key]
			if !exists {
				rb.accum[key] = &metricAccum{sum: v, min: v, max: v, count: 1}
			} else {
				a.sum += v
				a.count++
				if v < a.min {
					a.min = v
				}
				if v > a.max {
					a.max = v
				}
			}
		}
		rb.rowIdx++
	}
	// Flush final buckets for all runs
	for runID, rb := range runBuckets {
		if err := flushRunBucket(runID, rb); err != nil {
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
