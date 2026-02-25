package filestream

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/sarna/worb/internal/store"
)

type Handler struct {
	Store *store.DB
}

type fileStreamRequest struct {
	Files    map[string]fileStreamFile `json:"files"`
	Offset   map[string]int            `json:"offset"`
	Complete bool                       `json:"complete"`
}

type fileStreamFile struct {
	Offset  int        `json:"offset"`
	Content []string   `json:"content"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	entity := chi.URLParam(r, "entity")
	project := chi.URLParam(r, "project")
	runName := chi.URLParam(r, "run")

	proj, err := h.Store.EnsureProject(entity, project)
	if err != nil {
		http.Error(w, fmt.Sprintf("ensure project: %v", err), http.StatusInternalServerError)
		return
	}

	run, err := h.Store.GetRunByName(proj.ID, runName)
	if err != nil {
		http.Error(w, fmt.Sprintf("find run %s: %v", runName, err), http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	var req fileStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, fmt.Sprintf("parse body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Complete {
		if err := h.Store.FinishRun(run.ID); err != nil {
			log.Printf("finish run: %v", err)
		}
	}

	for filename, file := range req.Files {
		switch {
		case filename == "wandb-history.jsonl":
			for i, line := range file.Content {
				step := file.Offset + i
				if strings.TrimSpace(line) == "" {
					continue
				}
				if err := h.Store.InsertHistory(run.ID, step, json.RawMessage(line)); err != nil {
					log.Printf("insert history step %d: %v", step, err)
				}
			}

		case filename == "wandb-summary.json":
			if len(file.Content) > 0 {
				last := file.Content[len(file.Content)-1]
				if strings.TrimSpace(last) != "" {
					if err := h.Store.UpdateRunSummary(run.ID, json.RawMessage(last)); err != nil {
						log.Printf("update summary: %v", err)
					}
				}
			}

		case filename == "wandb-events.jsonl":
			for i, line := range file.Content {
				lineNum := file.Offset + i
				if strings.TrimSpace(line) == "" {
					continue
				}
				if err := h.Store.InsertSystemEvent(run.ID, lineNum, json.RawMessage(line)); err != nil {
					log.Printf("insert event %d: %v", lineNum, err)
				}
			}

		case filename == "output.log":
			for i, line := range file.Content {
				lineNum := file.Offset + i
				if err := h.Store.InsertConsoleLog(run.ID, lineNum, line, "stdout"); err != nil {
					log.Printf("insert log %d: %v", lineNum, err)
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"exitcode": nil,
		"limits":   map[string]any{},
	})
}
