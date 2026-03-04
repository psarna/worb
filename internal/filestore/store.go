package filestore

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-chi/chi/v5"
)

type Store struct {
	Dir string
}

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create file store dir: %w", err)
	}
	return &Store{Dir: dir}, nil
}

func (s *Store) Upload(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")
	if token == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}

	path := filepath.Join(s.Dir, token)
	f, err := os.Create(path)
	if err != nil {
		http.Error(w, "create file", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	if _, err := io.Copy(f, r.Body); err != nil {
		http.Error(w, "write file", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Store) Stat(token string) (os.FileInfo, error) {
	return os.Stat(filepath.Join(s.Dir, token))
}

func (s *Store) Download(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")
	if token == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}

	path := filepath.Join(s.Dir, token)
	http.ServeFile(w, r, path)
}
