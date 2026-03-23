package graphql

import (
	"encoding/json"

	"github.com/sarna/worb/internal/store"
)

type branchPoint struct {
	RunID string
	Step  int
}

// extractBranchPoint parses _wandb.branch_point from wandb config JSON.
// The config format is: {"_wandb": {"value": {"branch_point": {"run_id": "...", "step": N}}}}
func extractBranchPoint(config json.RawMessage) *branchPoint {
	if config == nil {
		return nil
	}
	var cfgMap map[string]json.RawMessage
	if json.Unmarshal(config, &cfgMap) != nil {
		return nil
	}
	wandbRaw, ok := cfgMap["_wandb"]
	if !ok {
		return nil
	}

	// Try wrapped format: {"value": {"branch_point": {...}}}
	var wrapper struct {
		Value map[string]json.RawMessage `json:"value"`
	}
	if json.Unmarshal(wandbRaw, &wrapper) == nil && wrapper.Value != nil {
		if bpRaw, ok := wrapper.Value["branch_point"]; ok {
			var bp struct {
				RunID string `json:"run_id"`
				Step  int    `json:"step"`
			}
			if json.Unmarshal(bpRaw, &bp) == nil && bp.RunID != "" {
				return &branchPoint{RunID: bp.RunID, Step: bp.Step}
			}
		}
	}

	// Try plain format: {"branch_point": {...}}
	var plain map[string]json.RawMessage
	if json.Unmarshal(wandbRaw, &plain) == nil {
		if bpRaw, ok := plain["branch_point"]; ok {
			var bp struct {
				RunID string `json:"run_id"`
				Step  int    `json:"step"`
			}
			if json.Unmarshal(bpRaw, &bp) == nil && bp.RunID != "" {
				return &branchPoint{RunID: bp.RunID, Step: bp.Step}
			}
		}
	}

	return nil
}

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

	emptyArr := "[]"
	gqlRun.HistoryTail = &emptyArr
	gqlRun.EventsTail = &emptyArr

	readOnly := false
	gqlRun.ReadOnly = &readOnly
	gqlRun.User = &RunUser{}

	return gqlRun
}
