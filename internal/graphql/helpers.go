package graphql

import (
	"encoding/json"

	"github.com/sarna/worb/internal/store"
)

func storeRunToGQL(r *store.Run) *Run {
	gqlRun := &Run{
		ID:   r.ID,
		Name: r.Name,
	}
	if r.DisplayName != "" {
		gqlRun.DisplayName = &r.DisplayName
	}
	if r.Config != nil {
		s := string(r.Config)
		gqlRun.Config = &s
	}
	if r.Summary != nil {
		s := string(r.Summary)
		gqlRun.SummaryMetrics = &s
	}
	if r.State != "" {
		gqlRun.State = &r.State
	}
	if r.Host != "" {
		gqlRun.Host = &r.Host
	}
	if r.Program != "" {
		gqlRun.Program = &r.Program
	}
	if r.GitCommit != "" {
		gqlRun.Commit = &r.GitCommit
	}
	if r.Notes != "" {
		gqlRun.Notes = &r.Notes
	}
	if r.GroupName != "" {
		gqlRun.Group = &r.GroupName
	}
	if r.JobType != "" {
		gqlRun.JobType = &r.JobType
	}
	if r.SweepName != "" {
		gqlRun.SweepName = &r.SweepName
	}
	gqlRun.HistoryLineCount = &r.HistoryLineCount
	gqlRun.EventsLineCount = &r.EventsLineCount
	gqlRun.LogLineCount = &r.LogLineCount

	if r.Tags != nil {
		var tags []string
		json.Unmarshal(r.Tags, &tags)
		gqlRun.Tags = tags
	}

	createdAt := r.CreatedAt.Format("2006-01-02T15:04:05")
	updatedAt := r.UpdatedAt.Format("2006-01-02T15:04:05")
	heartbeatAt := r.HeartbeatAt.Format("2006-01-02T15:04:05")
	gqlRun.CreatedAt = &createdAt
	gqlRun.UpdatedAt = &updatedAt
	gqlRun.HeartbeatAt = &heartbeatAt

	return gqlRun
}
