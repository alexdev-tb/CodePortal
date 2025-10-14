package executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type memoryStore struct {
	mu   sync.Mutex
	jobs map[string]Job
}

func newMemoryStore() *memoryStore {
	return &memoryStore{jobs: make(map[string]Job)}
}

func (m *memoryStore) Save(_ context.Context, job Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
	return nil
}

func (m *memoryStore) Update(_ context.Context, job Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
	return nil
}

func (m *memoryStore) Get(_ context.Context, id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	return job, nil
}

type stubRunner struct {
	mu          sync.Mutex
	result      Result
	err         error
	lastTimeout time.Duration
	lastRequest Request
}

func (s *stubRunner) Run(_ context.Context, _ string, req Request, timeout time.Duration) (Result, error) {
	s.mu.Lock()
	s.lastTimeout = timeout
	s.lastRequest = req
	s.mu.Unlock()
	res := s.result
	if res.CompletedAt.IsZero() {
		res.CompletedAt = time.Now().UTC()
	}
	return res, s.err
}

func TestServiceExecuteSuccess(t *testing.T) {
	store := newMemoryStore()
	runner := &stubRunner{result: Result{ExitCode: 0, Duration: 10 * time.Millisecond}}
	svc := NewService(store, runner, time.Second)

	job, err := svc.Submit(context.Background(), Request{Language: "go", Code: "package main"})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	waitFor(t, func() bool {
		loaded, err := store.Get(context.Background(), job.ID)
		if err != nil {
			return false
		}
		return loaded.Status == StatusSucceeded && loaded.Result != nil
	})

	finalJob, err := store.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if finalJob.Result.ExitCode != 0 {
		t.Fatalf("unexpected exit code: %d", finalJob.Result.ExitCode)
	}

	if finalJob.Request.Timeout != time.Second {
		t.Fatalf("expected stored timeout %s, got %s", time.Second, finalJob.Request.Timeout)
	}
}

func TestServiceExecuteFailure(t *testing.T) {
	store := newMemoryStore()
	runner := &stubRunner{
		result: Result{ExitCode: 1, Duration: 5 * time.Millisecond},
		err:    errors.New("process exited with status 1"),
	}
	svc := NewService(store, runner, time.Second)

	job, err := svc.Submit(context.Background(), Request{Language: "python", Code: "print('hi')"})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	waitFor(t, func() bool {
		loaded, err := store.Get(context.Background(), job.ID)
		if err != nil {
			return false
		}
		return loaded.Status == StatusFailed && loaded.Error != ""
	})

	finalJob, err := store.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if finalJob.Result.ExitCode != 1 {
		t.Fatalf("unexpected exit code: %d", finalJob.Result.ExitCode)
	}

	if finalJob.Error == "" {
		t.Fatalf("expected error message for failed job")
	}

	if finalJob.Request.Timeout != time.Second {
		t.Fatalf("expected stored timeout %s, got %s", time.Second, finalJob.Request.Timeout)
	}
}

func TestServiceHonorsCustomTimeout(t *testing.T) {
	store := newMemoryStore()
	runner := &stubRunner{result: Result{ExitCode: 0, Duration: 5 * time.Millisecond}}
	svc := NewService(store, runner, 5*time.Second)

	customTimeout := 1500 * time.Millisecond
	job, err := svc.Submit(context.Background(), Request{Language: "go", Code: "package main", Timeout: customTimeout})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	waitFor(t, func() bool {
		loaded, err := store.Get(context.Background(), job.ID)
		if err != nil {
			return false
		}
		return loaded.Status != StatusQueued
	})

	finalJob, err := store.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if finalJob.Request.Timeout != customTimeout {
		t.Fatalf("expected stored timeout %s, got %s", customTimeout, finalJob.Request.Timeout)
	}

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.lastTimeout != customTimeout {
		t.Fatalf("expected runner timeout %s, got %s", customTimeout, runner.lastTimeout)
	}
}

func TestServicePropagatesIOAcrossLanguages(t *testing.T) {
	store := newMemoryStore()
	runner := &stubRunner{result: Result{Stdout: "hi\n", Stderr: "", ExitCode: 0, Duration: 8 * time.Millisecond}}
	svc := NewService(store, runner, 5*time.Second)

	req := Request{Language: "python", Code: "print(input())", Stdin: "hi\n"}
	job, err := svc.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	waitFor(t, func() bool {
		loaded, err := store.Get(context.Background(), job.ID)
		if err != nil {
			return false
		}
		return loaded.Status == StatusSucceeded && loaded.Result != nil
	})

	finalJob, err := store.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	runner.mu.Lock()
	lastReq := runner.lastRequest
	runner.mu.Unlock()

	if lastReq.Language != req.Language {
		t.Fatalf("expected runner language %q, got %q", req.Language, lastReq.Language)
	}

	if lastReq.Stdin != req.Stdin {
		t.Fatalf("expected runner to receive stdin %q, got %q", req.Stdin, lastReq.Stdin)
	}

	if finalJob.Result.Stdout != runner.result.Stdout {
		t.Fatalf("expected stdout %q, got %q", runner.result.Stdout, finalJob.Result.Stdout)
	}

	if finalJob.Result.Stderr != runner.result.Stderr {
		t.Fatalf("expected stderr %q, got %q", runner.result.Stderr, finalJob.Result.Stderr)
	}
}

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within timeout")
}
