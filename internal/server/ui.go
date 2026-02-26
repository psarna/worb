package server

import (
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
	templates.ExecuteTemplate(w, "index.html", map[string]any{
		"Projects": projects,
		"Host":     s.config.Host,
		"Port":     s.config.Port,
	})
}

func (s *Server) uiProject(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectID")
	project, err := s.store.GetProject(projectID)
	if err != nil {
		http.Error(w, "project not found", http.StatusNotFound)
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
		http.Error(w, "run not found", http.StatusNotFound)
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

	templates.ExecuteTemplate(w, "run.html", map[string]any{
		"Run":         run,
		"Project":     project,
		"ConfigJSON":  configJSON,
		"SummaryJSON": summaryJSON,
	})
}
