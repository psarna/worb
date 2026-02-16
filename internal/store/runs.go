package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
	ID               string
	ProjectID        string
	Name             string
	DisplayName      string
	Config           json.RawMessage
	Summary          json.RawMessage
	State            string
	Host             string
	Program          string
	GitCommit        string
	Tags             json.RawMessage
	Notes            string
	GroupName        string
	JobType          string
	SweepName        string
	HistoryLineCount int
	EventsLineCount  int
	LogLineCount     int
	CreatedAt        time.Time
	UpdatedAt        time.Time
	HeartbeatAt      time.Time
	Project          *Project
}

func (db *DB) EnsureProject(entity, name string) (*Project, error) {
	if entity == "" {
		entity = "local"
	}

	var p Project
	err := db.QueryRow("SELECT id, entity, name, created_at FROM projects WHERE entity = ? AND name = ?", entity, name).
		Scan(&p.ID, &p.Entity, &p.Name, &p.CreatedAt)
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
	err := db.QueryRow("SELECT id FROM runs WHERE id = ?", p.ID).Scan(&existing)
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

func (db *DB) GetRun(id string) (*Run, error) {
	r := &Run{}
	var config, summary, tags sql.NullString
	var displayName, host, program, gitCommit, notes, groupName, jobType, sweepName sql.NullString
	err := db.QueryRow(`SELECT r.id, r.project_id, r.name, r.display_name,
		CAST(r.config AS VARCHAR), CAST(r.summary AS VARCHAR), r.state,
		r.host, r.program, r.git_commit, CAST(r.tags AS VARCHAR), r.notes, r.group_name, r.job_type, r.sweep_name,
		r.history_line_count, r.events_line_count, r.log_line_count,
		r.created_at, r.updated_at, r.heartbeat_at
		FROM runs r WHERE r.id = ?`, id).
		Scan(&r.ID, &r.ProjectID, &r.Name, &displayName, &config, &summary, &r.State,
			&host, &program, &gitCommit, &tags, &notes, &groupName, &jobType, &sweepName,
			&r.HistoryLineCount, &r.EventsLineCount, &r.LogLineCount,
			&r.CreatedAt, &r.UpdatedAt, &r.HeartbeatAt)
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
	err := db.QueryRow("SELECT id FROM runs WHERE project_id = ? AND name = ?", projectID, name).Scan(&id)
	if err != nil {
		return nil, err
	}
	return db.GetRun(id)
}

func (db *DB) ListRuns(projectID string) ([]*Run, error) {
	rows, err := db.Query(`SELECT id FROM runs WHERE project_id = ? ORDER BY created_at DESC`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*Run
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		r, err := db.GetRun(id)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func (db *DB) ListProjects(entity string) ([]*Project, error) {
	if entity == "" {
		entity = "local"
	}
	rows, err := db.Query("SELECT id, entity, name, created_at FROM projects WHERE entity = ? ORDER BY created_at DESC", entity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []*Project
	for rows.Next() {
		p := &Project{}
		if err := rows.Scan(&p.ID, &p.Entity, &p.Name, &p.CreatedAt); err != nil {
			return nil, err
		}
		projects = append(projects, p)
	}
	return projects, nil
}

func (db *DB) GetProject(id string) (*Project, error) {
	p := &Project{}
	err := db.QueryRow("SELECT id, entity, name, created_at FROM projects WHERE id = ?", id).
		Scan(&p.ID, &p.Entity, &p.Name, &p.CreatedAt)
	if err != nil {
		return nil, err
	}
	return p, nil
}
