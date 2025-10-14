package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Request struct {
	Language string        `json:"language"`
	Code     string        `json:"code"`
	Stdin    string        `json:"stdin"`
	Timeout  time.Duration `json:"timeout,omitempty"`
}

type Result struct {
	Stdout      string        `json:"stdout"`
	Stderr      string        `json:"stderr"`
	ExitCode    int           `json:"exitCode"`
	Duration    time.Duration `json:"duration"`
	CompletedAt time.Time     `json:"completedAt"`
}

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID          string     `json:"id"`
	Request     Request    `json:"request"`
	Status      Status     `json:"status"`
	Result      *Result    `json:"result,omitempty"`
	Error       string     `json:"error,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

var ErrJobNotFound = errors.New("job not found")

type JobStore interface {
	Save(ctx context.Context, job Job) error
	Update(ctx context.Context, job Job) error
	Get(ctx context.Context, id string) (Job, error)
}

type Executor interface {
	Submit(ctx context.Context, req Request) (Job, error)
	Get(ctx context.Context, id string) (Job, error)
}

type Service struct {
	store   JobStore
	runner  SandboxRunner
	timeout time.Duration
}

type SandboxRunner interface {
	Run(ctx context.Context, jobID string, req Request, timeout time.Duration) (Result, error)
}

func NewService(store JobStore, runner SandboxRunner, timeout time.Duration) *Service {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Service{store: store, runner: runner, timeout: timeout}
}

func (s *Service) Submit(ctx context.Context, req Request) (Job, error) {
	effectiveTimeout := req.Timeout
	if effectiveTimeout <= 0 {
		effectiveTimeout = s.timeout
	}
	req.Timeout = effectiveTimeout

	job := Job{
		ID:        uuid.New().String(),
		Request:   req,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := s.store.Save(ctx, job); err != nil {
		return Job{}, fmt.Errorf("save job: %w", err)
	}

	go s.execute(job.ID)

	return job, nil
}

func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	job, err := s.store.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	return job, nil
}

func (s *Service) execute(id string) {
	ctx := context.Background()
	job, err := s.store.Get(ctx, id)
	if err != nil {
		log.Printf("executor: fetch job %s: %v", id, err)
		return
	}

	now := time.Now().UTC()
	job.Status = StatusRunning
	job.StartedAt = ptrTime(now)
	job.UpdatedAt = now

	if err := s.store.Update(ctx, job); err != nil {
		log.Printf("executor: update running job %s: %v", id, err)
		return
	}

	runTimeout := job.Request.Timeout
	if runTimeout <= 0 {
		runTimeout = s.timeout
		job.Request.Timeout = runTimeout
	}

	result, runErr := s.runner.Run(ctx, job.ID, job.Request, runTimeout)

	job.Result = &result
	if !result.CompletedAt.IsZero() {
		job.CompletedAt = ptrTime(result.CompletedAt)
		job.UpdatedAt = result.CompletedAt
	} else {
		job.UpdatedAt = time.Now().UTC()
	}

	if runErr != nil {
		job.Status = StatusFailed
		job.Error = runErr.Error()
	} else if result.ExitCode == 0 {
		job.Status = StatusSucceeded
		job.Error = ""
	} else {
		job.Status = StatusFailed
		job.Error = fmt.Sprintf("process exited with status %d", result.ExitCode)
	}

	if runErr != nil {
		log.Printf("executor: job %s failed: %v", id, runErr)
	}

	if err := s.store.Update(ctx, job); err != nil {
		log.Printf("executor: finalize job %s: %v", id, err)
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

type RedisJobStore struct {
	client *redis.Client
	prefix string
}

func NewRedisJobStore(client *redis.Client) *RedisJobStore {
	return &RedisJobStore{client: client, prefix: "job:"}
}

func (s *RedisJobStore) Save(ctx context.Context, job Job) error {
	key := s.key(job.ID)
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	if err := s.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (s *RedisJobStore) Update(ctx context.Context, job Job) error {
	return s.Save(ctx, job)
}

func (s *RedisJobStore) Get(ctx context.Context, id string) (Job, error) {
	key := s.key(id)
	raw, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return Job{}, ErrJobNotFound
		}
		return Job{}, fmt.Errorf("redis get: %w", err)
	}

	var job Job
	if err := json.Unmarshal(raw, &job); err != nil {
		return Job{}, fmt.Errorf("unmarshal job: %w", err)
	}
	return job, nil
}

func (s *RedisJobStore) key(id string) string {
	return s.prefix + id
}
