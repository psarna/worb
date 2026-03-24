package server

import (
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/sarna/worb/internal/store"
	"github.com/sarna/worb/ui"
)

var templates *template.Template

func init() {
	templates = template.Must(template.ParseFS(ui.Static, "templates/*.html"))
}

func (s *Server) uiIndex(w http.ResponseWriter, r *http.Request) {
	projects, _ := s.store.ListAllProjects()
	data := map[string]any{
		"Projects": projects,
		"Host":     s.config.Host,
		"Port":     s.config.Port,
	}
	if notFound := r.URL.Query().Get("notfound"); notFound != "" {
		data["NotFound"] = notFound
	}
	templates.ExecuteTemplate(w, "index.html", data)
}

func (s *Server) uiProject(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectID")
	project, err := s.store.GetProject(projectID)
	if err != nil {
		http.Redirect(w, r, "/?notfound="+r.URL.Path, http.StatusTemporaryRedirect)
		return
	}
	runs, _ := s.store.ListRuns(projectID)
	forkParentNames := map[string]string{}
	forkParentIDs := map[string]string{}
	forkSteps := map[string]string{}
	for _, run := range runs {
		if run.ForkRunID != nil {
			if parent, err := s.store.GetRun(*run.ForkRunID); err == nil {
				forkParentNames[run.ID] = runLabel(parent)
				forkParentIDs[run.ID] = parent.ID
			}
		}
		if run.ForkStep != nil {
			forkSteps[run.ID] = strconv.Itoa(*run.ForkStep)
		}
	}
	templates.ExecuteTemplate(w, "project.html", map[string]any{
		"Project":         project,
		"Runs":            runs,
		"ForkParentNames": forkParentNames,
		"ForkParentIDs":   forkParentIDs,
		"ForkSteps":       forkSteps,
	})
}

func (s *Server) uiRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	run, err := s.store.GetRun(runID)
	if err != nil {
		http.Redirect(w, r, "/?notfound="+r.URL.Path, http.StatusTemporaryRedirect)
		return
	}
	project, _ := s.store.GetProject(run.ProjectID)
	lineage := s.buildForkLineage(run)

	configJSON := template.JS("{}")
	if run.Config != nil {
		configJSON = template.JS(run.Config)
	}

	summaryJSON := template.JS("{}")
	if run.Summary != nil {
		summaryJSON = template.JS(run.Summary)
	}

	tagsJSON := template.JS("[]")
	if run.Tags != nil {
		tagsJSON = template.JS(run.Tags)
	}

	notesBytes, _ := json.Marshal(run.Notes)

	runMeta := map[string]string{
		"state":          run.State,
		"name":           run.Name,
		"display_name":   run.DisplayName,
		"host":           run.Host,
		"program":        run.Program,
		"git_commit":     run.GitCommit,
		"group_name":     run.GroupName,
		"job_type":       run.JobType,
		"sweep_name":     run.SweepName,
		"created_at":     run.CreatedAt.Format("Jan 2, 2006 3:04:05 PM"),
		"created_at_iso": run.CreatedAt.Format("2006-01-02T15:04:05Z"),
		"updated_at_iso": run.UpdatedAt.Format("2006-01-02T15:04:05Z"),
	}
	runMetaBytes, _ := json.Marshal(runMeta)
	lineagePreviewBytes, _ := json.Marshal(buildForkLineagePreview(lineage))

	templates.ExecuteTemplate(w, "run.html", map[string]any{
		"Run":             run,
		"Project":         project,
		"ForkLineage":     lineage,
		"ForkLineageJSON": template.JS(lineagePreviewBytes),
		"ConfigJSON":      configJSON,
		"SummaryJSON":     summaryJSON,
		"TagsJSON":        tagsJSON,
		"NotesJSON":       template.JS(notesBytes),
		"RunMetaJSON":     template.JS(runMetaBytes),
		"DBEngine":        s.config.DBEngine,
	})
}

func (s *Server) uiPerf(w http.ResponseWriter, r *http.Request) {
	templates.ExecuteTemplate(w, "perf.html", map[string]any{
		"Perf": s.store.SQLPerfSnapshot(),
	})
}

func runLabel(run *store.Run) string {
	if run == nil {
		return ""
	}
	if run.DisplayName != "" {
		return run.DisplayName
	}
	return run.Name
}

type forkLineageItem struct {
	Run             *store.Run
	ForkStepDisplay string
	Depth           int
}

func (s *Server) buildForkLineage(run *store.Run) []forkLineageItem {
	var lineage []forkLineageItem
	seen := map[string]bool{}
	current := run
	depth := 0
	for current != nil && current.ForkRunID != nil {
		parentID := *current.ForkRunID
		if seen[parentID] {
			break
		}
		seen[parentID] = true
		parent, err := s.store.GetRun(parentID)
		if err != nil || parent == nil {
			break
		}
		forkStepDisplay := ""
		if current.ForkStep != nil {
			forkStepDisplay = strconv.Itoa(*current.ForkStep)
		}
		lineage = append(lineage, forkLineageItem{
			Run:             parent,
			ForkStepDisplay: forkStepDisplay,
			Depth:           depth,
		})
		current = parent
		depth++
	}
	return lineage
}

func buildForkLineagePreview(lineage []forkLineageItem) []map[string]any {
	if len(lineage) == 0 {
		return nil
	}
	preview := make([]map[string]any, 0, len(lineage))
	for _, item := range lineage {
		run := item.Run
		preview = append(preview, map[string]any{
			"id":          run.ID,
			"name":        run.Name,
			"displayName": run.DisplayName,
			"label":       runLabel(run),
			"state":       run.State,
			"steps":       run.HistoryLineCount,
			"createdAt":   run.CreatedAt.Format("Jan 2, 2006 3:04:05 PM"),
			"forkStep":    item.ForkStepDisplay,
			"depth":       item.Depth,
			"summary":     previewSummary(run.Summary),
		})
	}
	return preview
}

func previewSummary(raw json.RawMessage) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil
	}
	out := map[string]string{}
	for _, key := range sortedPreviewKeys(data) {
		if val, ok := previewValue(data[key]); ok {
			out[key] = val
			if len(out) >= 6 {
				break
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sortedPreviewKeys(data map[string]any) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func previewValue(v any) (string, bool) {
	switch val := v.(type) {
	case string:
		return val, true
	case bool:
		return strconv.FormatBool(val), true
	case float64:
		return strconv.FormatFloat(val, 'g', 6, 64), true
	default:
		return "", false
	}
}
