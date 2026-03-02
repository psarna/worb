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
			var batch []struct {
				Step int
				Data json.RawMessage
			}
			for i, line := range file.Content {
				if strings.TrimSpace(line) == "" {
					continue
				}
				batch = append(batch, struct {
					Step int
					Data json.RawMessage
				}{Step: file.Offset + i, Data: json.RawMessage(line)})
			}
			if len(batch) > 0 {
				log.Printf("[filestream] history batch: offset=%d lines=%d steps=%d..%d", file.Offset, len(batch), batch[0].Step, batch[len(batch)-1].Step)
			}
			if err := h.Store.InsertHistoryBatch(run.ID, batch); err != nil {
				log.Printf("insert history batch: %v", err)
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
			var batch []struct {
				LineNum int
				Data    json.RawMessage
			}
			for i, line := range file.Content {
				if strings.TrimSpace(line) == "" {
					continue
				}
				batch = append(batch, struct {
					LineNum int
					Data    json.RawMessage
				}{LineNum: file.Offset + i, Data: json.RawMessage(line)})
			}
			if err := h.Store.InsertSystemEventBatch(run.ID, batch); err != nil {
				log.Printf("insert events batch: %v", err)
			}

		case filename == "output.log":
			var batch []struct {
				LineNum int
				Line    string
			}
			for i, line := range file.Content {
				batch = append(batch, struct {
					LineNum int
					Line    string
				}{LineNum: file.Offset + i, Line: line})
			}
			if err := h.Store.InsertConsoleLogBatch(run.ID, batch); err != nil {
				log.Printf("insert logs batch: %v", err)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"exitcode": nil,
		"limits":   map[string]any{},
	})
}
