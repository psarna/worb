package server

import (
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/go-chi/chi/v5"
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
	templates.ExecuteTemplate(w, "project.html", map[string]any{
		"Project": project,
		"Runs":    runs,
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

	templates.ExecuteTemplate(w, "run.html", map[string]any{
		"Run":         run,
		"Project":     project,
		"ConfigJSON":  configJSON,
		"SummaryJSON": summaryJSON,
		"TagsJSON":    tagsJSON,
		"NotesJSON":   template.JS(notesBytes),
		"RunMetaJSON": template.JS(runMetaBytes),
		"DBEngine":    s.config.DBEngine,
	})
}
