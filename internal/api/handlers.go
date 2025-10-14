package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/alexdev-tb/code-sandbox-api/internal/executor"
)

type Handler struct {
	executor executor.Executor
}

type ExecuteRequest struct {
	Language string `json:"language"`
	Code     string `json:"code"`
	Stdin    string `json:"stdin,omitempty"`
	Timeout  string `json:"timeout,omitempty"`
}

type ExecuteResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

type JobStatusResponse struct {
	ID          string           `json:"id"`
	Status      string           `json:"status"`
	CreatedAt   time.Time        `json:"createdAt"`
	UpdatedAt   time.Time        `json:"updatedAt"`
	StartedAt   *time.Time       `json:"startedAt,omitempty"`
	CompletedAt *time.Time       `json:"completedAt,omitempty"`
	Result      *executor.Result `json:"result,omitempty"`
	Error       string           `json:"error,omitempty"`
	Request     executor.Request `json:"request"`
}

var ErrUnsupportedLanguage = errors.New("unsupported language")

func NewHandler(exec executor.Executor) *Handler {
	return &Handler{executor: exec}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) Execute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()

	var payload ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if err := validateExecuteRequest(payload); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, ErrUnsupportedLanguage) {
			status = http.StatusNotImplemented
		}
		writeError(w, status, err.Error())
		return
	}

	timeout, err := parseExecutionTimeout(payload.Timeout)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	job, err := h.executor.Submit(ctx, executor.Request{
		Language: payload.Language,
		Code:     payload.Code,
		Stdin:    payload.Stdin,
		Timeout:  timeout,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeError(w, http.StatusGatewayTimeout, "execution timed out")
			return
		}
		writeError(w, http.StatusInternalServerError, "execution failed")
		return
	}

	resp := ExecuteResponse{
		ID:        job.ID,
		Status:    string(job.Status),
		CreatedAt: job.CreatedAt,
	}

	writeJSON(w, http.StatusAccepted, resp)
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()

	id := strings.TrimPrefix(r.URL.Path, "/v1/execute/")
	if strings.TrimSpace(id) == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	job, err := h.executor.Get(ctx, id)
	if err != nil {
		if errors.Is(err, executor.ErrJobNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load job")
		return
	}

	resp := JobStatusResponse{
		ID:          job.ID,
		Status:      string(job.Status),
		CreatedAt:   job.CreatedAt,
		UpdatedAt:   job.UpdatedAt,
		StartedAt:   job.StartedAt,
		CompletedAt: job.CompletedAt,
		Result:      job.Result,
		Error:       job.Error,
		Request:     job.Request,
	}

	writeJSON(w, http.StatusOK, resp)
}

func validateExecuteRequest(req ExecuteRequest) error {
	if strings.TrimSpace(req.Language) == "" {
		return errors.New("language is required")
	}
	if strings.TrimSpace(req.Code) == "" {
		return errors.New("code is required")
	}

	switch strings.ToLower(req.Language) {
	case "go", "python":
		return nil
	default:
		return ErrUnsupportedLanguage
	}
}

func parseExecutionTimeout(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout value")
	}
	if d <= 0 {
		return 0, fmt.Errorf("timeout must be greater than zero")
	}
	return d, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
