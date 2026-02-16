package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"

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
)

type Config struct {
	Port    int
	DataDir string
}

type Server struct {
	config Config
	store  *store.DB
	files  *filestore.Store
	router *chi.Mux
}

func New(cfg Config) (*Server, error) {
	db, err := store.New(cfg.DataDir)
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

	baseURL := fmt.Sprintf("http://localhost:%d", s.config.Port)

	gqlResolver := &graphql.Resolver{
		Store:   s.store,
		BaseURL: baseURL,
	}
	gqlSrv := handler.NewDefaultServer(graphql.NewExecutableSchema(graphql.Config{Resolvers: gqlResolver}))

	r.Handle("/graphql", gqlSrv)
	r.Handle("/playground", playground.Handler("worb", "/graphql"))

	fsHandler := &filestream.Handler{Store: s.store}
	r.Post("/files/{entity}/{project}/{run}/file_stream", fsHandler.ServeHTTP)

	r.Put("/files/upload/{token}", s.files.Upload)
	r.Get("/files/upload/{token}", s.files.Download)

	r.Get("/api/v1/viewer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"entity": "local",
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
		r.Get("/runs/{runID}", s.apiGetRun)
		r.Get("/runs/{runID}/history", s.apiGetHistory)
		r.Get("/runs/{runID}/logs", s.apiGetLogs)
	})

	r.Handle("/static/*", http.FileServer(http.FS(ui.Static)))
	r.Get("/", s.uiIndex)
	r.Get("/projects/{projectID}", s.uiProject)
	r.Get("/runs/{runID}", s.uiRun)

	s.router = r
}

func (s *Server) Run() error {
	addr := fmt.Sprintf(":%d", s.config.Port)
	log.Printf("worb listening on http://localhost:%d", s.config.Port)
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
	projects, err := s.store.ListProjects("local")
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

func (s *Server) apiGetHistory(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	history, err := s.store.GetHistory(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
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
