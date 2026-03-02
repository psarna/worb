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

	templates.ExecuteTemplate(w, "run.html", map[string]any{
		"Run":         run,
		"Project":     project,
		"ConfigJSON":  configJSON,
		"SummaryJSON": summaryJSON,
		"DBEngine":    s.config.DBEngine,
	})
}
