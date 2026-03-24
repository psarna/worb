package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type Project struct {
	ID        string
	Entity    string
	Name      string
	CreatedAt time.Time
}

type Run struct {
	ID                   string
	ProjectID            string
	Name                 string
	DisplayName          string
	Config               json.RawMessage
	Summary              json.RawMessage
	State                string
	Host                 string
	Program              string
	GitCommit            string
	Tags                 json.RawMessage
	Notes                string
	GroupName            string
	JobType              string
	SweepName            string
	HistoryLineCount     int
	EventsLineCount      int
	LogLineCount         int
	ReceivedHistoryCount int
	ForkRunID            *string
	ForkStep             *int
	CreatedAt            time.Time
	UpdatedAt            time.Time
	HeartbeatAt          time.Time
	Project              *Project
}

func (db *DB) EnsureProject(entity, name string) (*Project, error) {
	if entity == "" {
		entity = "local"
	}

	var p Project
	var createdAt flexTime
	err := db.QueryRow("SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND name = ? AND deleted_at IS NULL", entity, name).
		Scan(&p.ID, &p.Entity, &p.Name, &createdAt)
	p.CreatedAt = createdAt.Time
	if err == nil {
		return &p, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	p = Project{
		ID:     uuid.New().String(),
		Entity: entity,
		Name:   name,
	}
	_, err = db.Exec("INSERT INTO projects (id, entity, name) VALUES (?, ?, ?)", p.ID, p.Entity, p.Name)
	if err != nil {
		return nil, fmt.Errorf("insert project: %w", err)
	}
	return &p, nil
}

type UpsertRunParams struct {
	ID          string
	Entity      string
	Project     string
	Name        string
	DisplayName string
	Config      json.RawMessage
	Summary     json.RawMessage
	State       string
	Host        string
	Program     string
	GitCommit   string
	Tags        json.RawMessage
	Notes       string
	GroupName   string
	JobType     string
	SweepName   string
}

func (db *DB) UpsertRun(p UpsertRunParams) (*Run, error) {
	clientSentID := p.ID != ""
	if p.ID == "" {
		p.ID = uuid.New().String()
	}
	if p.Name == "" {
		if len(p.ID) >= 8 {
			p.Name = p.ID[:8]
		} else {
			p.Name = p.ID
		}
	}

	var existing string
	err := db.QueryRow("SELECT id FROM runs WHERE id = ? AND deleted_at IS NULL", p.ID).Scan(&existing)
	if err == sql.ErrNoRows && !clientSentID && p.Name != "" {
		proj, projErr := db.EnsureProject(p.Entity, p.Project)
		if projErr == nil {
			lookupErr := db.QueryRow("SELECT id FROM runs WHERE project_id = ? AND name = ? AND deleted_at IS NULL", proj.ID, p.Name).Scan(&existing)
			if lookupErr == nil {
				p.ID = existing
				err = nil
			}
		}
	}
	if err == sql.ErrNoRows {
		proj, projErr := db.EnsureProject(p.Entity, p.Project)
		if projErr != nil {
			return nil, fmt.Errorf("ensure project: %w", projErr)
		}
		if p.Config == nil {
			p.Config = json.RawMessage("{}")
		}
		if p.Summary == nil {
			p.Summary = json.RawMessage("{}")
		}
		if p.Tags == nil {
			p.Tags = json.RawMessage("[]")
		}
		if p.State == "" {
			p.State = "running"
		}
		_, err = db.Exec(`INSERT INTO runs (id, project_id, name, display_name, config, summary, state, host, program, git_commit, tags, notes, group_name, job_type, sweep_name)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			p.ID, proj.ID, p.Name, p.DisplayName, string(p.Config), string(p.Summary), p.State,
			p.Host, p.Program, p.GitCommit, string(p.Tags), p.Notes, p.GroupName, p.JobType, p.SweepName)
		if err != nil {
			return nil, fmt.Errorf("insert run: %w", err)
		}
	} else if err == nil {
		sets := "updated_at = current_timestamp, heartbeat_at = current_timestamp"
		args := []any{}
		if p.State != "" {
			sets += ", state = ?"
			args = append(args, p.State)
		}
		if p.Config != nil {
			sets += ", config = ?"
			args = append(args, string(p.Config))
		}
		if p.Summary != nil {
			sets += ", summary = ?"
			args = append(args, string(p.Summary))
		}
		if p.DisplayName != "" {
			sets += ", display_name = ?"
			args = append(args, p.DisplayName)
		}
		if p.Tags != nil {
			sets += ", tags = ?"
			args = append(args, string(p.Tags))
		}
		if p.Notes != "" {
			sets += ", notes = ?"
			args = append(args, p.Notes)
		}
		if p.Host != "" {
			sets += ", host = ?"
			args = append(args, p.Host)
		}
		if p.Program != "" {
			sets += ", program = ?"
			args = append(args, p.Program)
		}
		args = append(args, p.ID)
		_, err = db.Exec("UPDATE runs SET "+sets+" WHERE id = ?", args...)
		if err != nil {
			return nil, fmt.Errorf("update run: %w", err)
		}
	} else {
		return nil, err
	}

	return db.GetRun(p.ID)
}

func (db *DB) FinishRun(id string) error {
	_, err := db.Exec("UPDATE runs SET state = 'finished', updated_at = current_timestamp WHERE id = ?", id)
	return err
}

func (db *DB) IncrReceivedHistoryCount(id string, delta int) {
	_, err := db.Exec("UPDATE runs SET received_history_count = received_history_count + ? WHERE id = ?", delta, id)
	if err != nil {
		log.Printf("[store] IncrReceivedHistoryCount(%s, %d): %v", id, delta, err)
	}
}

func (db *DB) GetRun(id string) (*Run, error) {
	r := &Run{}
	var config, summary, tags sql.NullString
	var displayName, host, program, gitCommit, notes, groupName, jobType, sweepName sql.NullString
	var forkRunID sql.NullString
	var forkStep sql.NullInt64
	var createdAt, updatedAt, heartbeatAt flexTime
	err := db.QueryRow(fmt.Sprintf(`SELECT r.id, r.project_id, r.name, r.display_name,
		%s, %s, r.state,
		r.host, r.program, r.git_commit, %s, r.notes, r.group_name, r.job_type, r.sweep_name,
		r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
		r.fork_run_id, r.fork_step,
		r.created_at, r.updated_at, r.heartbeat_at
		FROM runs r WHERE r.id = ? AND r.deleted_at IS NULL`, db.castJSON("r.config"), db.castJSON("r.summary"), db.castJSON("r.tags")), id).
		Scan(&r.ID, &r.ProjectID, &r.Name, &displayName, &config, &summary, &r.State,
			&host, &program, &gitCommit, &tags, &notes, &groupName, &jobType, &sweepName,
			&r.HistoryLineCount, &r.EventsLineCount, &r.LogLineCount, &r.ReceivedHistoryCount,
			&forkRunID, &forkStep,
			&createdAt, &updatedAt, &heartbeatAt)
	r.CreatedAt = createdAt.Time
	r.UpdatedAt = updatedAt.Time
	r.HeartbeatAt = heartbeatAt.Time
	if err != nil {
		return nil, err
	}
	r.DisplayName = displayName.String
	r.Host = host.String
	r.Program = program.String
	r.GitCommit = gitCommit.String
	r.Notes = notes.String
	r.GroupName = groupName.String
	r.JobType = jobType.String
	r.SweepName = sweepName.String
	if forkRunID.Valid {
		r.ForkRunID = &forkRunID.String
	}
	if forkStep.Valid {
		v := int(forkStep.Int64)
		r.ForkStep = &v
	}
	if config.Valid {
		r.Config = json.RawMessage(config.String)
	}
	if summary.Valid {
		r.Summary = json.RawMessage(summary.String)
	}
	if tags.Valid {
		r.Tags = json.RawMessage(tags.String)
	}
	return r, nil
}

func (db *DB) GetRunByName(projectID, name string) (*Run, error) {
	var id string
	err := db.QueryRow("SELECT id FROM runs WHERE project_id = ? AND name = ? AND deleted_at IS NULL", projectID, name).Scan(&id)
	if err == sql.ErrNoRows {
		// Fallback: try matching by display_name
		err = db.QueryRow("SELECT id FROM runs WHERE project_id = ? AND display_name = ? AND deleted_at IS NULL", projectID, name).Scan(&id)
	}
	if err != nil {
		return nil, err
	}
	return db.GetRun(id)
}

func (db *DB) ResolveRunRef(projectID, ref string) (*Run, error) {
	run, err := db.GetRun(ref)
	if err == nil {
		if run.ProjectID != projectID {
			return nil, sql.ErrNoRows
		}
		return run, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	var nameCount int
	err = db.QueryRow("SELECT COUNT(*) FROM runs WHERE project_id = ? AND name = ? AND deleted_at IS NULL", projectID, ref).Scan(&nameCount)
	if err != nil {
		return nil, err
	}
	if nameCount == 1 {
		return db.GetRunByName(projectID, ref)
	}
	if nameCount > 1 {
		return nil, fmt.Errorf("ambiguous run reference %q: matched %d runs by name", ref, nameCount)
	}

	var displayNameCount int
	err = db.QueryRow("SELECT COUNT(*) FROM runs WHERE project_id = ? AND display_name = ? AND deleted_at IS NULL", projectID, ref).Scan(&displayNameCount)
	if err != nil {
		return nil, err
	}
	if displayNameCount == 1 {
		var id string
		err = db.QueryRow("SELECT id FROM runs WHERE project_id = ? AND display_name = ? AND deleted_at IS NULL", projectID, ref).Scan(&id)
		if err != nil {
			return nil, err
		}
		return db.GetRun(id)
	}
	if displayNameCount > 1 {
		return nil, fmt.Errorf("ambiguous run reference %q: matched %d runs by display_name", ref, displayNameCount)
	}

	return nil, sql.ErrNoRows
}

func (db *DB) ListRuns(projectID string) ([]*Run, error) {
	rows, err := db.Query(fmt.Sprintf(`SELECT r.id, r.project_id, r.name, r.display_name,
		%s, %s, r.state,
		r.host, r.program, r.git_commit, %s, r.notes, r.group_name, r.job_type, r.sweep_name,
		r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
		r.fork_run_id, r.fork_step,
		r.created_at, r.updated_at, r.heartbeat_at
		FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC`,
		db.castJSON("r.config"), db.castJSON("r.summary"), db.castJSON("r.tags")), projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		r := &Run{}
		var config, summary, tags sql.NullString
		var displayName, host, program, gitCommit, notes, groupName, jobType, sweepName sql.NullString
		var forkRunID sql.NullString
		var forkStep sql.NullInt64
		var createdAt, updatedAt, heartbeatAt flexTime
		if err := rows.Scan(&r.ID, &r.ProjectID, &r.Name, &displayName, &config, &summary, &r.State,
			&host, &program, &gitCommit, &tags, &notes, &groupName, &jobType, &sweepName,
			&r.HistoryLineCount, &r.EventsLineCount, &r.LogLineCount, &r.ReceivedHistoryCount,
			&forkRunID, &forkStep,
			&createdAt, &updatedAt, &heartbeatAt); err != nil {
			return nil, err
		}
		r.CreatedAt = createdAt.Time
		r.UpdatedAt = updatedAt.Time
		r.HeartbeatAt = heartbeatAt.Time
		r.DisplayName = displayName.String
		r.Host = host.String
		r.Program = program.String
		r.GitCommit = gitCommit.String
		r.Notes = notes.String
		r.GroupName = groupName.String
		r.JobType = jobType.String
		r.SweepName = sweepName.String
		if forkRunID.Valid {
			r.ForkRunID = &forkRunID.String
		}
		if forkStep.Valid {
			v := int(forkStep.Int64)
			r.ForkStep = &v
		}
		if config.Valid {
			r.Config = json.RawMessage(config.String)
		}
		if summary.Valid {
			r.Summary = json.RawMessage(summary.String)
		}
		if tags.Valid {
			r.Tags = json.RawMessage(tags.String)
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func (db *DB) ListRunsLite(projectID string) ([]*Run, error) {
	rows, err := db.Query(`SELECT r.id, r.project_id, r.name, r.display_name, r.state,
		r.history_line_count, r.created_at, r.updated_at, r.heartbeat_at
		FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		r := &Run{}
		var displayName sql.NullString
		var createdAt, updatedAt, heartbeatAt flexTime
		if err := rows.Scan(&r.ID, &r.ProjectID, &r.Name, &displayName, &r.State,
			&r.HistoryLineCount, &createdAt, &updatedAt, &heartbeatAt); err != nil {
			return nil, err
		}
		r.CreatedAt = createdAt.Time
		r.UpdatedAt = updatedAt.Time
		r.HeartbeatAt = heartbeatAt.Time
		r.DisplayName = displayName.String
		runs = append(runs, r)
	}
	return runs, nil
}

func (db *DB) DeleteRun(runID string) error {
	res, err := db.Exec("UPDATE runs SET deleted_at = current_timestamp WHERE id = ? AND deleted_at IS NULL", runID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("run not found: %s", runID)
	}
	return nil
}

func (db *DB) DeleteProject(projectID string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("UPDATE runs SET deleted_at = current_timestamp WHERE project_id = ? AND deleted_at IS NULL", projectID); err != nil {
		return fmt.Errorf("soft-delete runs: %w", err)
	}
	res, err := tx.Exec("UPDATE projects SET deleted_at = current_timestamp WHERE id = ? AND deleted_at IS NULL", projectID)
	if err != nil {
		return fmt.Errorf("soft-delete project: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("project not found: %s", projectID)
	}
	return tx.Commit()
}

func (db *DB) ListRunsFiltered(projectID string, displayName string) ([]*Run, error) {
	query := fmt.Sprintf(`SELECT r.id, r.project_id, r.name, r.display_name,
		%s, %s, r.state,
		r.host, r.program, r.git_commit, %s, r.notes, r.group_name, r.job_type, r.sweep_name,
		r.history_line_count, r.events_line_count, r.log_line_count, COALESCE(r.received_history_count, 0),
		r.fork_run_id, r.fork_step,
		r.created_at, r.updated_at, r.heartbeat_at
		FROM runs r WHERE r.project_id = ? AND r.deleted_at IS NULL`,
		db.castJSON("r.config"), db.castJSON("r.summary"), db.castJSON("r.tags"))
	args := []any{projectID}
	if displayName != "" {
		query += ` AND r.display_name = ?`
		args = append(args, displayName)
	}
	query += ` ORDER BY r.created_at DESC`

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		r := &Run{}
		var config, summary, tags sql.NullString
		var dn, host, program, gitCommit, notes, groupName, jobType, sweepName sql.NullString
		var forkRunID sql.NullString
		var forkStep sql.NullInt64
		var createdAt, updatedAt, heartbeatAt flexTime
		if err := rows.Scan(&r.ID, &r.ProjectID, &r.Name, &dn, &config, &summary, &r.State,
			&host, &program, &gitCommit, &tags, &notes, &groupName, &jobType, &sweepName,
			&r.HistoryLineCount, &r.EventsLineCount, &r.LogLineCount, &r.ReceivedHistoryCount,
			&forkRunID, &forkStep,
			&createdAt, &updatedAt, &heartbeatAt); err != nil {
			return nil, err
		}
		r.CreatedAt = createdAt.Time
		r.UpdatedAt = updatedAt.Time
		r.HeartbeatAt = heartbeatAt.Time
		r.DisplayName = dn.String
		r.Host = host.String
		r.Program = program.String
		r.GitCommit = gitCommit.String
		r.Notes = notes.String
		r.GroupName = groupName.String
		r.JobType = jobType.String
		r.SweepName = sweepName.String
		if forkRunID.Valid {
			r.ForkRunID = &forkRunID.String
		}
		if forkStep.Valid {
			v := int(forkStep.Int64)
			r.ForkStep = &v
		}
		if config.Valid {
			r.Config = json.RawMessage(config.String)
		}
		if summary.Valid {
			r.Summary = json.RawMessage(summary.String)
		}
		if tags.Valid {
			r.Tags = json.RawMessage(tags.String)
		}
		runs = append(runs, r)
	}
	return runs, nil
}

type ForkRunParams struct {
	ParentRunID     string
	ForkStep        int
	NewRunID        string          // if empty, generates a new UUID
	DisplayName     string          // optional
	ConfigOverrides json.RawMessage // merged on top of parent config
}

func (db *DB) ForkRun(p ForkRunParams) (*Run, error) {
	// Check parent run exists and has no unflushed WAL data
	parent, err := db.GetRun(p.ParentRunID)
	if err != nil {
		return nil, fmt.Errorf("parent run not found: %s", p.ParentRunID)
	}
	if parent.ReceivedHistoryCount != parent.HistoryLineCount {
		return nil, fmt.Errorf("parent run has unflushed data (%d received, %d flushed); finish the run or wait for WAL flush before forking",
			parent.ReceivedHistoryCount, parent.HistoryLineCount)
	}

	// Validate fork_step
	if p.ForkStep < 0 {
		return nil, fmt.Errorf("invalid fork_step: must be >= 0")
	}
	var maxStep int
	err = db.QueryRow("SELECT COALESCE(MAX(step), -1) FROM run_steps WHERE run_id = ?", p.ParentRunID).Scan(&maxStep)
	if err != nil {
		return nil, fmt.Errorf("query max step: %w", err)
	}
	if p.ForkStep > maxStep {
		return nil, fmt.Errorf("invalid fork_step %d: parent run has max step %d", p.ForkStep, maxStep)
	}

	// Generate new run ID
	newID := p.NewRunID
	if newID == "" {
		newID = uuid.New().String()
	}
	name := newID
	if len(name) >= 8 {
		name = name[:8]
	}

	// Merge config: parent base + overrides
	mergedConfig := parent.Config
	if p.ConfigOverrides != nil {
		mergedConfig = mergeConfigJSON(parent.Config, p.ConfigOverrides)
	}

	displayName := p.DisplayName
	if displayName == "" && parent.DisplayName != "" {
		displayName = fmt.Sprintf("fork of %s @%d", parent.DisplayName, p.ForkStep)
	}

	configStr := "{}"
	if mergedConfig != nil {
		configStr = string(mergedConfig)
	}
	tagsStr := "[]"
	if parent.Tags != nil {
		tagsStr = string(parent.Tags)
	}

	if p.NewRunID != "" {
		// Run already exists (created by UpsertRun) — update it with fork metadata
		_, err = db.Exec(`UPDATE runs SET config = ?, tags = ?, notes = ?, host = ?, program = ?,
			git_commit = ?, group_name = ?, job_type = ?, sweep_name = ?,
			fork_run_id = ?, fork_step = ?, updated_at = current_timestamp
			WHERE id = ?`,
			configStr, tagsStr, parent.Notes, parent.Host, parent.Program,
			parent.GitCommit, parent.GroupName, parent.JobType, parent.SweepName,
			p.ParentRunID, p.ForkStep, newID)
		if err != nil {
			return nil, fmt.Errorf("update forked run: %w", err)
		}
	} else {
		// Create a brand new run record
		_, err = db.Exec(`INSERT INTO runs (id, project_id, name, display_name, config, summary, state,
			host, program, git_commit, tags, notes, group_name, job_type, sweep_name, fork_run_id, fork_step)
			VALUES (?, ?, ?, ?, ?, '{}', 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			newID, parent.ProjectID, name, displayName, configStr,
			parent.Host, parent.Program, parent.GitCommit, tagsStr, parent.Notes,
			parent.GroupName, parent.JobType, parent.SweepName, p.ParentRunID, p.ForkStep)
		if err != nil {
			return nil, fmt.Errorf("insert forked run: %w", err)
		}
	}

	// Set history_line_count synchronously so the wandb client gets the correct
	// file offset and starts logging at fork_step+1 (not 0).
	var parentStepCount int
	err = db.QueryRow("SELECT COUNT(*) FROM run_steps WHERE run_id = ? AND step <= ?", p.ParentRunID, p.ForkStep).Scan(&parentStepCount)
	if err != nil {
		return nil, fmt.Errorf("count parent steps: %w", err)
	}
	_, err = db.Exec("UPDATE runs SET history_line_count = ? WHERE id = ?", parentStepCount, newID)
	if err != nil {
		return nil, fmt.Errorf("set history_line_count: %w", err)
	}

	// Enqueue WAL fork entry for async data copy
	err = db.wal.Append(walEntry{
		RunID:    newID,
		ForkFrom: &walForkOp{ParentRunID: p.ParentRunID, MaxStep: p.ForkStep},
	})
	if err != nil {
		return nil, fmt.Errorf("enqueue fork WAL entry: %w", err)
	}

	return db.GetRun(newID)
}

// mergeConfigJSON overlays overrides on top of base config JSON.
// Both are expected to be JSON objects. Keys in overrides replace keys in base.
func mergeConfigJSON(base, overrides json.RawMessage) json.RawMessage {
	var baseMap map[string]json.RawMessage
	var overMap map[string]json.RawMessage

	if json.Unmarshal(base, &baseMap) != nil {
		baseMap = map[string]json.RawMessage{}
	}
	if json.Unmarshal(overrides, &overMap) != nil {
		return base
	}

	for k, v := range overMap {
		baseMap[k] = v
	}

	merged, err := json.Marshal(baseMap)
	if err != nil {
		return base
	}
	return merged
}

func (db *DB) ListAllProjects() ([]*Project, error) {
	return db.ListProjects("")
}

func (db *DB) ListProjects(entity string) ([]*Project, error) {
	var rows *sql.Rows
	var err error
	if entity == "" {
		rows, err = db.Query("SELECT id, entity, name, created_at FROM projects WHERE deleted_at IS NULL ORDER BY created_at DESC")
	} else {
		rows, err = db.Query("SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND deleted_at IS NULL ORDER BY created_at DESC", entity)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []*Project
	for rows.Next() {
		p := &Project{}
		var ca flexTime
		if err := rows.Scan(&p.ID, &p.Entity, &p.Name, &ca); err != nil {
			return nil, err
		}
		p.CreatedAt = ca.Time
		projects = append(projects, p)
	}
	return projects, nil
}

func (db *DB) GetProject(id string) (*Project, error) {
	p := &Project{}
	var ca flexTime
	err := db.QueryRow("SELECT id, entity, name, created_at FROM projects WHERE id = ? AND deleted_at IS NULL", id).
		Scan(&p.ID, &p.Entity, &p.Name, &ca)
	if err != nil {
		return nil, err
	}
	p.CreatedAt = ca.Time
	return p, nil
}

type File struct {
	ID        string    `json:"id"`
	RunID     string    `json:"run_id"`
	Name      string    `json:"name"`
	URL       string    `json:"url"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

func (db *DB) InsertFile(id, runID, name, url string) error {
	_, err := db.Exec("INSERT OR IGNORE INTO files (id, run_id, name, url) VALUES (?, ?, ?, ?)", id, runID, name, url)
	return err
}

func (db *DB) UpdateFileSize(fileID string, size int64) {
	db.Exec("UPDATE files SET size = ? WHERE id = ?", size, fileID)
}

func (db *DB) ListFiles(runID string) ([]*File, error) {
	rows, err := db.Query("SELECT id, run_id, name, COALESCE(url, ''), COALESCE(size, 0), created_at FROM files WHERE run_id = ? ORDER BY name", runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []*File
	for rows.Next() {
		f := &File{}
		var ca flexTime
		if err := rows.Scan(&f.ID, &f.RunID, &f.Name, &f.URL, &f.Size, &ca); err != nil {
			return nil, err
		}
		f.CreatedAt = ca.Time
		files = append(files, f)
	}
	return files, nil
}

func (db *DB) GetProjectByName(entity, name string) (*Project, error) {
	p := &Project{}
	var ca flexTime
	err := db.QueryRow("SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND name = ? AND deleted_at IS NULL", entity, name).
		Scan(&p.ID, &p.Entity, &p.Name, &ca)
	if err != nil {
		return nil, err
	}
	p.CreatedAt = ca.Time
	return p, nil
}
