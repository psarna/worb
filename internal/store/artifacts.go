package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Artifact struct {
	ID        string
	RunID     string
	Type      string
	Name      string
	Digest    string
	State     string
	Metadata  json.RawMessage
	CreatedAt time.Time
}

func (db *DB) CreateArtifact(runID, artifactType, name string, metadata json.RawMessage) (*Artifact, error) {
	a := &Artifact{
		ID:    uuid.New().String(),
		RunID: runID,
		Type:  artifactType,
		Name:  name,
		State: "pending",
	}
	if metadata != nil {
		a.Metadata = metadata
	}

	_, err := db.Exec("INSERT INTO artifacts (id, run_id, type, name, metadata) VALUES (?, ?, ?, ?, ?)",
		a.ID, a.RunID, a.Type, a.Name, string(a.Metadata))
	if err != nil {
		return nil, fmt.Errorf("insert artifact: %w", err)
	}
	return a, nil
}

func (db *DB) GetArtifact(id string) (*Artifact, error) {
	a := &Artifact{}
	var metadata, digest sql.NullString
	err := db.QueryRow("SELECT id, run_id, type, name, digest, state, metadata, created_at FROM artifacts WHERE id = ?", id).
		Scan(&a.ID, &a.RunID, &a.Type, &a.Name, &digest, &a.State, &metadata, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	a.Digest = digest.String
	if metadata.Valid {
		a.Metadata = json.RawMessage(metadata.String)
	}
	return a, nil
}

func (db *DB) CommitArtifact(id, digest string) error {
	_, err := db.Exec("UPDATE artifacts SET state = 'committed', digest = ? WHERE id = ?", digest, id)
	return err
}
