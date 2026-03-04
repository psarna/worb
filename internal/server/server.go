package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	gqlgraphql "github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sarna/worb/internal/auth"
	"github.com/sarna/worb/internal/filestore"
	"github.com/sarna/worb/internal/filestream"
	"github.com/sarna/worb/internal/graphql"
	"github.com/sarna/worb/internal/store"
	"github.com/sarna/worb/ui"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type Config struct {
	Host     string
	Port     int
	DataDir  string
	DBEngine string
}

type Server struct {
	config Config
	store  *store.DB
	files  *filestore.Store
	router *chi.Mux
}

func New(cfg Config) (*Server, error) {
	db, err := store.New(cfg.DataDir, cfg.DBEngine)
	if err != nil {
		return nil, fmt.Errorf("init store: %w", err)
	}

	filesDir := filepath.Join(cfg.DataDir, "files")
	fs, err := filestore.New(filesDir)
	if err != nil {
		return nil, fmt.Errorf("init filestore: %w", err)
	}

	s := &Server{
		config: cfg,
		store:  db,
		files:  fs,
	}
	s.setupRoutes()
	return s, nil
}

func (s *Server) setupRoutes() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(auth.Middleware)
	r.Use(corsMiddleware)

	gqlResolver := &graphql.Resolver{
		Store: s.store,
	}
	gqlSrv := handler.NewDefaultServer(graphql.NewExecutableSchema(graphql.Config{Resolvers: gqlResolver}))
	gqlSrv.SetErrorPresenter(func(ctx context.Context, err error) *gqlerror.Error {
		gqlErr := gqlgraphql.DefaultErrorPresenter(ctx, err)
		log.Printf("GraphQL error: %s (path: %v, extensions: %v)", gqlErr.Message, gqlErr.Path, gqlErr.Extensions)
		return gqlErr
	})

	r.Handle("/graphql", graphql.BaseURLMiddleware(gqlSrv))
	r.Handle("/playground", playground.Handler("worb", "/graphql"))

	fsHandler := &filestream.Handler{Store: s.store}
	r.Post("/files/{entity}/{project}/{run}/file_stream", fsHandler.ServeHTTP)

	r.Put("/files/upload/{token}", s.handleFileUpload)
	r.Get("/files/upload/{token}", s.files.Download)

	r.Get("/api/v1/viewer", func(w http.ResponseWriter, r *http.Request) {
		entity := auth.EntityFromContext(r.Context())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"entity": entity,
			"flags":  "{}",
			"teams":  []any{},
		})
	})

	r.Get("/api/v1/reports", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]any{})
	})

	r.Route("/api", func(r chi.Router) {
		r.Get("/projects", s.apiListProjects)
		r.Get("/projects/{projectID}/runs", s.apiListRuns)
		r.Get("/projects/{projectID}/history", s.apiGetProjectHistory)
		r.Get("/runs/{runID}", s.apiGetRun)
		r.Get("/runs/{runID}/history", s.apiGetHistory)
		r.Get("/runs/{runID}/history/keys", s.apiGetHistoryKeys)
		r.Get("/runs/{runID}/histograms", s.apiGetHistograms)
		r.Get("/runs/{runID}/logs", s.apiGetLogs)
		r.Get("/runs/{runID}/files", s.apiGetFiles)
		r.Delete("/runs/{runID}", s.apiDeleteRun)
		r.Delete("/projects/{projectID}", s.apiDeleteProject)
		r.Post("/query", s.apiQuery)
	})

	r.Handle("/static/*", http.FileServer(http.FS(ui.Static)))
	r.Get("/", s.uiIndex)
	r.Get("/projects", http.RedirectHandler("/", http.StatusTemporaryRedirect).ServeHTTP)
	r.Get("/projects/", http.RedirectHandler("/", http.StatusTemporaryRedirect).ServeHTTP)
	r.Get("/projects/{projectID}", s.uiProject)
	r.Get("/runs/{runID}", s.uiRun)

	r.Get("/{entity}/{project}", s.wandbProjectRedirect)
	r.Get("/{entity}/{project}/runs/{runName}", s.wandbRunRedirect)

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/?notfound="+r.URL.Path, http.StatusTemporaryRedirect)
	})

	s.router = r
}

func (s *Server) Run() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	log.Printf("worb listening on http://%s (db: %s)", addr, s.config.DBEngine)
	return http.ListenAndServe(addr, s.router)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-CSRF-Token, X-Requested-With")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) apiListProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := s.store.ListProjects(auth.EntityFromContext(r.Context()))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(projects)
}

func (s *Server) apiListRuns(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectID")
	runs, err := s.store.ListRuns(projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(runs)
}

func (s *Server) apiGetRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	run, err := s.store.GetRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}

func (s *Server) apiDeleteRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	if err := s.store.DeleteRun(runID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (s *Server) apiDeleteProject(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectID")
	if err := s.store.DeleteProject(projectID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (s *Server) apiGetHistoryKeys(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	keys, err := s.store.GetHistoryKeys(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(keys)
}

func (s *Server) apiGetHistory(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	maxPoints, _ := strconv.Atoi(r.URL.Query().Get("maxPoints"))
	var filterKeys []string
	if keysParam := r.URL.Query().Get("keys"); keysParam != "" {
		filterKeys = strings.Split(keysParam, ",")
	}
	var xMin, xMax *float64
	if v := r.URL.Query().Get("xMin"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			xMin = &f
		}
	}
	if v := r.URL.Query().Get("xMax"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			xMax = &f
		}
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)
	n := 0

	err := s.store.StreamHistoryScalars(runID, maxPoints, filterKeys, xMin, xMax, func(p store.ScalarPoint) error {
		if err := enc.Encode(p); err != nil {
			return err
		}
		n++
		if canFlush && n%50 == 0 {
			flusher.Flush()
		}
		return nil
	})
	if err != nil {
		if n == 0 {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if canFlush {
		flusher.Flush()
	}
}

func (s *Server) apiGetProjectHistory(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectID")
	maxPoints, _ := strconv.Atoi(r.URL.Query().Get("maxPoints"))

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)
	n := 0

	err := s.store.StreamProjectHistoryScalars(projectID, maxPoints, func(p store.ProjectScalarPoint) error {
		if err := enc.Encode(p); err != nil {
			return err
		}
		n++
		if canFlush && n%50 == 0 {
			flusher.Flush()
		}
		return nil
	})
	if err != nil {
		if n == 0 {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if canFlush {
		flusher.Flush()
	}
}

func (s *Server) apiGetHistograms(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)
	n := 0

	err := s.store.StreamHistoryHistograms(runID, func(p store.HistogramPoint) error {
		if err := enc.Encode(p); err != nil {
			return err
		}
		n++
		if canFlush && n%50 == 0 {
			flusher.Flush()
		}
		return nil
	})
	if err != nil {
		if n == 0 {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if canFlush {
		flusher.Flush()
	}
}

func (s *Server) apiQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.SQL == "" {
		http.Error(w, "sql is required", http.StatusBadRequest)
		return
	}

	result, err := s.store.ExecuteQuery(req.SQL)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *Server) wandbProjectRedirect(w http.ResponseWriter, r *http.Request) {
	entity := chi.URLParam(r, "entity")
	projectName := chi.URLParam(r, "project")
	project, err := s.store.GetProjectByName(entity, projectName)
	if err != nil {
		http.Error(w, "project not found", http.StatusNotFound)
		return
	}
	http.Redirect(w, r, "/projects/"+project.ID, http.StatusFound)
}

func (s *Server) wandbRunRedirect(w http.ResponseWriter, r *http.Request) {
	entity := chi.URLParam(r, "entity")
	projectName := chi.URLParam(r, "project")
	runName := chi.URLParam(r, "runName")
	project, err := s.store.GetProjectByName(entity, projectName)
	if err != nil {
		http.Error(w, "project not found", http.StatusNotFound)
		return
	}
	run, err := s.store.GetRunByName(project.ID, runName)
	if err != nil {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	http.Redirect(w, r, "/runs/"+run.ID, http.StatusFound)
}

func (s *Server) handleFileUpload(w http.ResponseWriter, r *http.Request) {
	s.files.Upload(w, r)
	token := chi.URLParam(r, "token")
	if token != "" {
		if info, err := s.files.Stat(token); err == nil {
			s.store.UpdateFileSize(token, info.Size())
		}
	}
}

func (s *Server) apiGetFiles(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	files, err := s.store.ListFiles(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if files == nil {
		files = []*store.File{}
	}
	json.NewEncoder(w).Encode(files)
}

func (s *Server) apiGetLogs(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	logs, err := s.store.GetConsoleLogs(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}
