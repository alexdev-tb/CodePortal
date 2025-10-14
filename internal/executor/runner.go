package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type RunnerConfig struct {
	Container    string
	JobDir       string
	DockerBinary string
	Network      string
	ExecUser     string
}

type DockerRunner struct {
	container string
	jobDir    string
	dockerBin string
	network   string
	execUser  string
	languages map[string]languageSpec
}

type languageSpec struct {
	extension string
	args      []string
}

func NewDockerRunner(cfg RunnerConfig) *DockerRunner {
	container := strings.TrimSpace(cfg.Container)
	if container == "" {
		container = "code-sandbox-runner"
	}

	jobDir := strings.TrimSpace(cfg.JobDir)
	if jobDir == "" {
		jobDir = "/tmp/jobs"
	}

	dockerBin := strings.TrimSpace(cfg.DockerBinary)
	if dockerBin == "" {
		dockerBin = "docker"
	}

	network := strings.TrimSpace(cfg.Network)
	execUser := strings.TrimSpace(cfg.ExecUser)
	if execUser == "" {
		uid := os.Getuid()
		gid := os.Getgid()
		if uid >= 0 && gid >= 0 {
			execUser = fmt.Sprintf("%d:%d", uid, gid)
		}
	}

	return &DockerRunner{
		container: container,
		jobDir:    jobDir,
		dockerBin: dockerBin,
		network:   network,
		execUser:  execUser,
		languages: map[string]languageSpec{
			"go": {
				extension: "go",
				args:      []string{"go", "run"},
			},
			"python": {
				extension: "py",
				args:      []string{"python3"},
			},
		},
	}
}

func (r *DockerRunner) Run(ctx context.Context, jobID string, req Request, timeout time.Duration) (Result, error) {
	if err := r.ensureContainer(ctx); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, err
	}

	language := strings.ToLower(strings.TrimSpace(req.Language))
	spec, ok := r.languages[language]
	if !ok {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("unsupported language %q", req.Language)
	}

	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	jobPath := filepath.Join(r.jobDir, jobID)
	if err := os.MkdirAll(jobPath, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare job dir: %w", err)
	}

	sourcePath := filepath.Join(jobPath, fmt.Sprintf("main.%s", spec.extension))
	if err := os.WriteFile(sourcePath, []byte(req.Code), 0o666); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("write source: %w", err)
	}

	cacheDir := filepath.Join(jobPath, "cache")
	if err := os.MkdirAll(cacheDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare cache dir: %w", err)
	}

	modCacheDir := filepath.Join(jobPath, "gomod")
	if err := os.MkdirAll(modCacheDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare module cache dir: %w", err)
	}

	tmpDir := filepath.Join(jobPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare temp dir: %w", err)
	}

	pyCacheDir := filepath.Join(jobPath, "pycache")
	if err := os.MkdirAll(pyCacheDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare python cache dir: %w", err)
	}

	defer r.cleanup(jobPath)

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmdArgs := []string{"exec", "-i"}

	if r.execUser != "" {
		cmdArgs = append(cmdArgs, "--user", r.execUser)
	}

	envVars := []string{
		fmt.Sprintf("TMPDIR=%s", tmpDir),
	}
	if language == "go" {
		envVars = append(envVars,
			fmt.Sprintf("GOCACHE=%s", cacheDir),
			fmt.Sprintf("GOMODCACHE=%s", modCacheDir),
			fmt.Sprintf("GOTMPDIR=%s", tmpDir),
			"GO111MODULE=off",
		)
	}
	if language == "python" {
		envVars = append(envVars,
			fmt.Sprintf("PYTHONPYCACHEPREFIX=%s", pyCacheDir),
		)
	}

	for _, env := range envVars {
		cmdArgs = append(cmdArgs, "--env", env)
	}

	cmdArgs = append(cmdArgs, "--workdir", jobPath)
	cmdArgs = append(cmdArgs, r.container)
	cmdArgs = append(cmdArgs, spec.args...)
	cmdArgs = append(cmdArgs, filepath.Join(jobPath, filepath.Base(sourcePath)))

	cmd := exec.CommandContext(execCtx, r.dockerBin, cmdArgs...)

	if req.Stdin != "" {
		cmd.Stdin = strings.NewReader(req.Stdin)
	}

	var stdoutBuf bytes.Buffer
	var stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	started := time.Now()
	err := cmd.Run()
	duration := time.Since(started)
	completed := time.Now().UTC()

	exitCode := 0
	if err != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			exitCode = -1
		} else if execErr := (*exec.ExitError)(nil); errors.As(err, &execErr) {
			exitCode = execErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	result := Result{
		Stdout:      stdoutBuf.String(),
		Stderr:      stderrBuf.String(),
		ExitCode:    exitCode,
		Duration:    duration,
		CompletedAt: completed,
	}

	if err != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			return result, fmt.Errorf("execution timed out after %s", timeout)
		}
		if execErr := (*exec.ExitError)(nil); errors.As(err, &execErr) {
			return result, fmt.Errorf("process exited with status %d", exitCode)
		}
		return result, fmt.Errorf("execution error: %w", err)
	}

	return result, nil
}

func (r *DockerRunner) cleanup(path string) {
	if err := os.RemoveAll(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Printf("executor: cleanup %s: %v", path, err)
	}
}

func (r *DockerRunner) ensureContainer(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, r.dockerBin, "inspect", "-f", "{{.State.Running}}", r.container)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect sandbox container: %w", err)
	}

	state := strings.TrimSpace(string(output))
	if state == "true" {
		return nil
	}

	if state == "false" {
		startCmd := exec.CommandContext(ctx, r.dockerBin, "start", r.container)
		if startErr := startCmd.Run(); startErr != nil {
			return fmt.Errorf("sandbox container %q not running and failed to start: %w", r.container, startErr)
		}
		return nil
	}

	return fmt.Errorf("sandbox container %q in unexpected state %q", r.container, state)
}
