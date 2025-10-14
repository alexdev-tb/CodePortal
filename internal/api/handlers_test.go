package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alexdev-tb/code-sandbox-api/internal/executor"
)

type stubExecutor struct {
	submitJob executor.Job
	submitErr error
	getJob    executor.Job
	getErr    error

	mu            sync.Mutex
	lastSubmitReq executor.Request
}

func (s *stubExecutor) Submit(_ context.Context, req executor.Request) (executor.Job, error) {
	s.mu.Lock()
	s.lastSubmitReq = req
	s.mu.Unlock()
	return s.submitJob, s.submitErr
}

func (s *stubExecutor) Get(_ context.Context, _ string) (executor.Job, error) {
	return s.getJob, s.getErr
}

func TestHealth(t *testing.T) {
	h := NewHandler(&stubExecutor{})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	h.Health(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected JSON content type, got %q", ct)
	}
}

func TestExecuteSuccess(t *testing.T) {
	exec := &stubExecutor{
		submitJob: executor.Job{
			ID:        "job-123",
			Status:    executor.StatusQueued,
			CreatedAt: time.Unix(0, 0).UTC(),
		},
	}

	h := NewHandler(exec)

	payload := ExecuteRequest{Language: "go", Code: "package main"}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/execute", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.Execute(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected %d, got %d", http.StatusAccepted, rec.Code)
	}

	var resp ExecuteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.ID != "job-123" {
		t.Fatalf("unexpected job id: %q", resp.ID)
	}

	if resp.Status != string(executor.StatusQueued) {
		t.Fatalf("unexpected status: %q", resp.Status)
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.lastSubmitReq.Timeout != 0 {
		t.Fatalf("expected zero timeout when not provided, got %s", exec.lastSubmitReq.Timeout)
	}
}

func TestExecuteWithStdin(t *testing.T) {
	exec := &stubExecutor{
		submitJob: executor.Job{
			ID:        "job-stdin",
			Status:    executor.StatusQueued,
			CreatedAt: time.Unix(0, 0).UTC(),
		},
	}

	h := NewHandler(exec)

	payload := ExecuteRequest{Language: "python", Code: "print(input())", Stdin: "hello\n"}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/execute", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.Execute(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected %d, got %d", http.StatusAccepted, rec.Code)
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.lastSubmitReq.Stdin != payload.Stdin {
		t.Fatalf("expected stdin %q, got %q", payload.Stdin, exec.lastSubmitReq.Stdin)
	}
}

func TestExecuteUnsupportedLanguage(t *testing.T) {
	h := NewHandler(&stubExecutor{})

	payload := ExecuteRequest{Language: "brainfuck", Code: "+-"}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/v1/execute", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.Execute(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected %d, got %d", http.StatusNotImplemented, rec.Code)
	}
}

func TestGetJobNotFound(t *testing.T) {
	h := NewHandler(&stubExecutor{getErr: executor.ErrJobNotFound})

	req := httptest.NewRequest(http.MethodGet, "/v1/execute/missing", nil)
	rec := httptest.NewRecorder()
	h.GetJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d", http.StatusNotFound, rec.Code)
	}
}

func TestGetJobSuccess(t *testing.T) {
	job := executor.Job{
		ID:        "job-123",
		Status:    executor.StatusSucceeded,
		CreatedAt: time.Unix(0, 0).UTC(),
		UpdatedAt: time.Unix(10, 0).UTC(),
		Result: &executor.Result{
			Stdout:      "hello\n",
			Stderr:      "",
			ExitCode:    0,
			Duration:    100 * time.Millisecond,
			CompletedAt: time.Unix(10, 0).UTC(),
		},
		CompletedAt: ptrTime(time.Unix(10, 0).UTC()),
	}

	h := NewHandler(&stubExecutor{getJob: job})

	req := httptest.NewRequest(http.MethodGet, "/v1/execute/job-123", nil)
	rec := httptest.NewRecorder()
	h.GetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}

	var resp JobStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.Result == nil || resp.Result.Stdout != "hello\n" {
		t.Fatalf("expected stdout, got %+v", resp.Result)
	}
}

func TestExecuteTimeoutParsing(t *testing.T) {
	exec := &stubExecutor{
		submitJob: executor.Job{ID: "job-123", Status: executor.StatusQueued, CreatedAt: time.Unix(0, 0).UTC()},
	}

	h := NewHandler(exec)

	payload := ExecuteRequest{Language: "python", Code: "print('hi')", Timeout: "2s"}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/execute", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.Execute(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected %d, got %d", http.StatusAccepted, rec.Code)
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.lastSubmitReq.Timeout != 2*time.Second {
		t.Fatalf("expected timeout 2s, got %s", exec.lastSubmitReq.Timeout)
	}
}

func TestExecuteTimeoutInvalid(t *testing.T) {
	h := NewHandler(&stubExecutor{})

	payload := ExecuteRequest{Language: "go", Code: "package main", Timeout: "-1"}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/execute", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.Execute(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
