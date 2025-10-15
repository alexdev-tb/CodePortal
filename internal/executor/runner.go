package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RunnerConfig struct {
	Container          string
	LanguageContainers map[string][]string
	JobDir             string
	DockerBinary       string
	Network            string
	ExecUser           string
}

type DockerRunner struct {
	jobDir          string
	dockerBin       string
	network         string
	execUser        string
	languages       map[string]languageSpec
	pools           map[string]*containerPool
	fallback        *containerPool
	cleanupQ        chan cleanupRequest
	limits          map[string]ContainerLimits
	limitsMu        sync.RWMutex
	dead            map[string]struct{}
	deadMu          sync.RWMutex
	prewarming      map[string]struct{}
	prewarmMu       sync.RWMutex
	prewarmGate     atomic.Bool
	discoveryGate   atomic.Bool
	discoveryCancel context.CancelFunc
}

const (
	colorReset   = "\033[0m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorCyan    = "\033[36m"
	colorRed     = "\033[31m"
	colorMagenta = "\033[35m"
)

const (
	cleanupQueueSize   = 128
	cleanupMaxAttempts = 5
)

const (
	heartbeatInterval         = 2 * time.Second
	heartbeatProbeTimeout     = 750 * time.Millisecond
	heartbeatFailureThreshold = 3
	prewarmInterval           = 30 * time.Second
	prewarmTimeout            = 5 * time.Second
	containerRecoveryTimeout  = 10 * time.Second
	discoveryInterval         = 20 * time.Second
)

var (
	errContainerNotRunning   = errors.New("container not running")
	errContainerUnresponsive = errors.New("container unresponsive")
)

type PoolSnapshot struct {
	Language   string
	Containers []string
	Total      int
	Available  int
	Fallback   bool
	Limits     ContainerLimits
}

type cleanupRequest struct {
	path    string
	attempt int
}

type languageSpec struct {
	extension string
	args      []string
}

var languageAliases = map[string]string{
	"go":         "go",
	"golang":     "go",
	"python":     "python",
	"py":         "python",
	"node":       "node",
	"nodejs":     "node",
	"javascript": "node",
	"js":         "node",
}

type ContainerLimits struct {
	CPUs            float64
	MemoryBytes     int64
	PidsLimit       int64
	ReadOnlyRoot    bool
	NoNewPrivileges bool
	SeccompProfile  string
	AppArmorProfile string
	CapDrop         []string
	NetworkMode     string
	Tmpfs           []string
	Ulimits         map[string]UlimitRange
}

type UlimitRange struct {
	Soft int64
	Hard int64
}

func FormatLimits(limits ContainerLimits) string {
	parts := make([]string, 0, 8)

	if limits.CPUs > 0 {
		parts = append(parts, fmt.Sprintf("cpu=%.2f", limits.CPUs))
	}
	if limits.MemoryBytes > 0 {
		parts = append(parts, fmt.Sprintf("mem=%s", formatBytes(limits.MemoryBytes)))
	}
	if limits.PidsLimit > 0 {
		parts = append(parts, fmt.Sprintf("pids=%d", limits.PidsLimit))
	}
	if limits.ReadOnlyRoot {
		parts = append(parts, "ro-root")
	}
	if limits.NoNewPrivileges {
		parts = append(parts, "no-new-privs")
	}
	if limits.SeccompProfile != "" {
		parts = append(parts, fmt.Sprintf("seccomp=%s", limits.SeccompProfile))
	}
	if limits.AppArmorProfile != "" {
		parts = append(parts, fmt.Sprintf("apparmor=%s", limits.AppArmorProfile))
	}
	if len(limits.CapDrop) > 0 {
		parts = append(parts, fmt.Sprintf("cap-drop=%s", strings.Join(limits.CapDrop, ",")))
	}
	if limits.NetworkMode != "" {
		parts = append(parts, fmt.Sprintf("net=%s", limits.NetworkMode))
	}
	if len(limits.Tmpfs) > 0 {
		parts = append(parts, fmt.Sprintf("tmpfs=%d", len(limits.Tmpfs)))
	}
	if len(limits.Ulimits) > 0 {
		ulimits := make([]string, 0, len(limits.Ulimits))
		for name, rng := range limits.Ulimits {
			ulimits = append(ulimits, fmt.Sprintf("%s=%d:%d", name, rng.Soft, rng.Hard))
		}
		sort.Strings(ulimits)
		parts = append(parts, fmt.Sprintf("ulimits=[%s]", strings.Join(ulimits, ",")))
	}

	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

func formatBytes(n int64) string {
	if n <= 0 {
		return "0B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	value := float64(n)
	suffix := []string{"KB", "MB", "GB", "TB", "PB", "EB"}
	exp := 0
	for value >= unit && exp < len(suffix)-1 {
		value /= unit
		exp++
	}
	return fmt.Sprintf("%.1f%s", value, suffix[exp])
}

func canonicalLanguageName(lang string) (string, bool) {
	trimmed := strings.ToLower(strings.TrimSpace(lang))
	if trimmed == "" {
		return "", false
	}
	canonical, ok := languageAliases[trimmed]
	return canonical, ok
}

func NewDockerRunner(cfg RunnerConfig) *DockerRunner {
	container := strings.TrimSpace(cfg.Container)

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

	langConfig := normalizeLanguageMap(cfg.LanguageContainers)
	if len(langConfig) == 0 {
		if detected, err := detectContainerPools(dockerBin); err != nil {
			log.Printf("%s[WARN]%s failed to auto-detect sandbox containers: %v", colorYellow, colorReset, err)
		} else if len(detected) > 0 {
			log.Printf("%s[BOOT]%s auto-detected %d language pool(s)", colorMagenta, colorReset, len(detected))
			langConfig = detected
		}
	}

	pools := make(map[string]*containerPool)
	for lang, names := range langConfig {
		lowerLang := strings.ToLower(strings.TrimSpace(lang))
		if lowerLang == "" {
			continue
		}

		var cleaned []string
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name != "" {
				cleaned = append(cleaned, name)
			}
		}
		if len(cleaned) == 0 {
			continue
		}

		pools[lowerLang] = newContainerPool(cleaned)
	}

	if container == "" {
		container = selectFallbackContainer(langConfig)
	}

	var fallbackPool *containerPool
	if container != "" {
		fallbackPool = newContainerPool([]string{container})
		if fallbackPool.available == nil {
			fallbackPool = nil
		}
	}

	r := &DockerRunner{
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
			"node": {
				extension: "js",
				args:      []string{"node"},
			},
		},
		pools:      pools,
		fallback:   fallbackPool,
		cleanupQ:   make(chan cleanupRequest, cleanupQueueSize),
		limits:     make(map[string]ContainerLimits),
		dead:       make(map[string]struct{}),
		prewarming: make(map[string]struct{}),
	}

	for _, pool := range pools {
		r.captureContainerLimits(pool.snapshotNames())
	}
	if fallbackPool != nil {
		r.captureContainerLimits(fallbackPool.snapshotNames())
	}

	r.startCleanupWorker()
	r.purgeOrphanedJobDirs()
	r.startPrewarmLoop()
	r.startDiscoveryLoop()

	return r
}

func (r *DockerRunner) Run(ctx context.Context, jobID string, req Request, timeout time.Duration) (Result, error) {
	canonical, ok := canonicalLanguageName(req.Language)
	if !ok {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("unsupported language %q", req.Language)
	}

	spec, ok := r.languages[canonical]
	if !ok {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("unsupported language %q", req.Language)
	}

	pool := r.pools[canonical]
	if pool == nil {
		pool = r.fallback
	}
	if pool == nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("no containers configured for language %q", req.Language)
	}

	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	maxAttempts := pool.capacity()
	if maxAttempts == 0 {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("no containers available for language %q", req.Language)
	}
	maxAttempts++

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, err, retry := r.runOnce(ctx, jobID, req, timeout, pool, canonical, spec)
		if !retry {
			return result, err
		}
		lastErr = err
		remaining := maxAttempts - attempt
		log.Printf("%s[WARN]%s job %s (%s) retrying with a different container (remaining=%d): %v", colorYellow, colorReset, jobID, canonical, remaining, err)
		if ctx.Err() != nil {
			break
		}
	}

	now := time.Now().UTC()
	if lastErr == nil {
		lastErr = fmt.Errorf("all containers unavailable")
	}
	return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("exhausted containers for language %q: %w", req.Language, lastErr)
}

func (r *DockerRunner) runOnce(ctx context.Context, jobID string, req Request, timeout time.Duration, pool *containerPool, canonical string, spec languageSpec) (Result, error, bool) {
	containerName, err := r.acquireContainer(ctx, pool, canonical, jobID)
	if err != nil {
		now := time.Now().UTC()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("context canceled while waiting for container: %w", err), false
		}
		if errors.Is(err, errNoContainersConfigured) {
			return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("no containers available for language %q", req.Language), false
		}
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("acquire container: %w", err), false
	}

	unhealthy := false
	defer func() {
		if unhealthy {
			r.recoverContainer(containerName, canonical, pool)
			return
		}
		if released := pool.release(containerName); released {
			log.Printf("%s[RELEASE]%s job %s (%s) released container %s (%d/%d available)", colorCyan, colorReset, jobID, canonical, containerName, pool.availableCount(), pool.capacity())
		} else {
			log.Printf("%s[WARN]%s job %s (%s) release dropped for container %s", colorYellow, colorReset, jobID, canonical, containerName)
		}
	}()

	if r.isContainerDead(containerName) {
		unhealthy = true
		r.disableContainer(pool, containerName)
		return Result{}, fmt.Errorf("container %s marked dead", containerName), true
	}

	r.captureContainerLimits([]string{containerName})

	if err := r.ensureContainer(ctx, containerName); err != nil {
		unhealthy = true
		r.disableContainer(pool, containerName)
		return Result{}, err, true
	}
	r.markContainerAlive(containerName)

	jobPath := filepath.Join(r.jobDir, jobID)
	if err := os.MkdirAll(jobPath, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare job dir: %w", err), false
	}

	sourcePath := filepath.Join(jobPath, fmt.Sprintf("main.%s", spec.extension))
	if err := os.WriteFile(sourcePath, []byte(req.Code), 0o666); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("write source: %w", err), false
	}

	cacheDir := filepath.Join(jobPath, "cache")
	if err := os.MkdirAll(cacheDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare cache dir: %w", err), false
	}

	modCacheDir := filepath.Join(jobPath, "gomod")
	if err := os.MkdirAll(modCacheDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare module cache dir: %w", err), false
	}

	tmpDir := filepath.Join(jobPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare temp dir: %w", err), false
	}

	pyCacheDir := filepath.Join(jobPath, "pycache")
	if err := os.MkdirAll(pyCacheDir, 0o777); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare python cache dir: %w", err), false
	}

	defer r.cleanup(jobPath)

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	heartbeatCtx, heartbeatCancel := context.WithCancel(execCtx)
	monitorErrCh := make(chan error, 1)
	go r.monitorContainer(heartbeatCtx, containerName, cancel, monitorErrCh)
	defer heartbeatCancel()

	cmdArgs := []string{"exec", "-i"}

	if r.execUser != "" {
		cmdArgs = append(cmdArgs, "--user", r.execUser)
	}

	envVars := []string{
		fmt.Sprintf("TMPDIR=%s", tmpDir),
	}
	if canonical == "go" {
		envVars = append(envVars,
			fmt.Sprintf("GOCACHE=%s", cacheDir),
			fmt.Sprintf("GOMODCACHE=%s", modCacheDir),
			fmt.Sprintf("GOTMPDIR=%s", tmpDir),
			"GO111MODULE=off",
		)
	}
	if canonical == "python" {
		envVars = append(envVars,
			fmt.Sprintf("PYTHONPYCACHEPREFIX=%s", pyCacheDir),
		)
	}
	if canonical == "node" {
		envVars = append(envVars,
			fmt.Sprintf("HOME=%s", jobPath),
			"NODE_OPTIONS=--max-old-space-size=192",
		)
	}

	for _, env := range envVars {
		cmdArgs = append(cmdArgs, "--env", env)
	}

	cmdArgs = append(cmdArgs, "--workdir", jobPath)
	cmdArgs = append(cmdArgs, containerName)
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
	runErr := cmd.Run()
	duration := time.Since(started)
	completed := time.Now().UTC()
	heartbeatCancel()
	var heartbeatErr error
	select {
	case heartbeatErr = <-monitorErrCh:
	default:
	}
	if heartbeatErr != nil {
		unhealthy = true
		if runErr == nil {
			runErr = heartbeatErr
		} else {
			runErr = fmt.Errorf("%w; heartbeat error: %v", runErr, heartbeatErr)
		}
	}

	exitCode := 0
	if heartbeatErr != nil {
		exitCode = -1
	} else if runErr != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			exitCode = -1
		} else if execErr := (*exec.ExitError)(nil); errors.As(runErr, &execErr) {
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
	if heartbeatErr != nil {
		if result.Stderr != "" {
			result.Stderr += "\n"
		}
		result.Stderr += fmt.Sprintf("container %s became unhealthy: %v", containerName, heartbeatErr)
		r.disableContainer(pool, containerName)
		return result, fmt.Errorf("container %s became unhealthy: %w", containerName, heartbeatErr), true
	}

	if runErr != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			return result, fmt.Errorf("execution timed out after %s", timeout), false
		}
		if execErr := (*exec.ExitError)(nil); errors.As(runErr, &execErr) {
			return result, fmt.Errorf("process exited with status %d", exitCode), false
		}
		return result, fmt.Errorf("execution error: %w", runErr), false
	}

	return result, nil, false
}

func (r *DockerRunner) acquireContainer(ctx context.Context, pool *containerPool, language, jobID string) (string, error) {
	if pool == nil {
		return "", errNoContainersConfigured
	}

	total := pool.capacity()
	availableBefore := pool.availableCount()
	if availableBefore == 0 {
		log.Printf("%s[QUEUE]%s job %s (%s) waiting for container (%d/%d available)", colorYellow, colorReset, jobID, language, availableBefore, total)
	}

	for {
		name, err := pool.acquire(ctx)
		if err != nil {
			log.Printf("%s[ERROR]%s job %s (%s) failed to acquire container: %v", colorRed, colorReset, jobID, language, err)
			return "", err
		}
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		if r.isContainerDead(trimmed) {
			log.Printf("%s[WARN]%s job %s (%s) skipped dead container %s", colorYellow, colorReset, jobID, language, trimmed)
			r.disableContainer(pool, trimmed)
			continue
		}
		if r.isContainerPrewarming(trimmed) {
			if released := pool.release(trimmed); !released {
				log.Printf("%s[WARN]%s job %s (%s) dropped prewarming container %s during assignment", colorYellow, colorReset, jobID, language, trimmed)
			}
			if err := ctx.Err(); err != nil {
				return "", err
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		remaining := pool.availableCount()
		log.Printf("%s[RUN]%s job %s (%s) assigned to container %s (%d/%d available)", colorGreen, colorReset, jobID, language, trimmed, remaining, total)
		return trimmed, nil
	}
}

func (r *DockerRunner) cleanup(path string) {
	cleanPath := filepath.Clean(path)
	if cleanPath == "." || cleanPath == "" {
		return
	}

	req := cleanupRequest{path: cleanPath, attempt: 1}
	if ok := r.enqueueCleanup(req); !ok {
		go r.processCleanup(req)
	}
}

func (r *DockerRunner) ensureContainer(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(ctx, r.dockerBin, "inspect", "-f", "{{.State.Running}}", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		errorMsg := fmt.Errorf("inspect sandbox container %s: %w", containerName, err)
		log.Printf("%s[ERROR]%s %v", colorRed, colorReset, errorMsg)
		return errorMsg
	}

	state := strings.TrimSpace(string(output))
	if state == "true" {
		return nil
	}

	if state == "false" {
		startCmd := exec.CommandContext(ctx, r.dockerBin, "start", containerName)
		if startErr := startCmd.Run(); startErr != nil {
			errorMsg := fmt.Errorf("sandbox container %q not running and failed to start: %w", containerName, startErr)
			log.Printf("%s[ERROR]%s %v", colorRed, colorReset, errorMsg)
			return errorMsg
		}
		log.Printf("%s[BOOT]%s started sandbox container %s", colorMagenta, colorReset, containerName)
		return nil
	}

	errState := fmt.Errorf("sandbox container %q in unexpected state %q", containerName, state)
	log.Printf("%s[WARN]%s %v", colorYellow, colorReset, errState)
	return errState
}

func (r *DockerRunner) monitorContainer(ctx context.Context, containerName string, cancel context.CancelFunc, out chan<- error) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	failures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeCtx, probeCancel := context.WithTimeout(context.Background(), heartbeatProbeTimeout)
			err := r.probeContainer(probeCtx, containerName)
			probeCancel()
			if err != nil {
				failures++
				if failures >= heartbeatFailureThreshold {
					log.Printf("%s[WARN]%s container %s heartbeat failed (%d/%d): %v", colorYellow, colorReset, containerName, failures, heartbeatFailureThreshold, err)
					cancel()
					select {
					case out <- err:
					default:
					}
					return
				}
				continue
			}
			failures = 0
		}
	}
}

func (r *DockerRunner) probeContainer(ctx context.Context, containerName string) error {
	state, err := r.fetchContainerState(ctx, containerName)
	if err != nil {
		return err
	}

	if !state.Running || state.Dead || strings.ToLower(state.Status) == "exited" {
		return fmt.Errorf("%w (status=%s)", errContainerNotRunning, state.Status)
	}
	if state.Health != nil {
		healthStatus := strings.ToLower(strings.TrimSpace(state.Health.Status))
		if healthStatus != "" && healthStatus != "healthy" && healthStatus != "starting" {
			return fmt.Errorf("%w (health=%s)", errContainerUnresponsive, state.Health.Status)
		}
	}

	execCtx, execCancel := context.WithTimeout(ctx, heartbeatProbeTimeout)
	defer execCancel()
	if err := r.execHeartbeat(execCtx, containerName); err != nil {
		return fmt.Errorf("%w: %v", errContainerUnresponsive, err)
	}

	return nil
}

type containerState struct {
	Running bool   `json:"Running"`
	Status  string `json:"Status"`
	Dead    bool   `json:"Dead"`
	Health  *struct {
		Status string `json:"Status"`
	} `json:"Health"`
}

func (r *DockerRunner) fetchContainerState(ctx context.Context, containerName string) (containerState, error) {
	cmd := exec.CommandContext(ctx, r.dockerBin, "inspect", "-f", "{{json .State}}", containerName)
	output, err := cmd.Output()
	if err != nil {
		return containerState{}, fmt.Errorf("docker inspect state %s: %w", containerName, err)
	}

	var state containerState
	if err := json.Unmarshal(output, &state); err != nil {
		return containerState{}, fmt.Errorf("parse state for %s: %w", containerName, err)
	}
	return state, nil
}

func (r *DockerRunner) execHeartbeat(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(ctx, r.dockerBin, "exec", containerName, "/bin/sh", "-c", "true")
	if err := cmd.Run(); err != nil {
		fallback := exec.CommandContext(ctx, r.dockerBin, "exec", containerName, "true")
		if fbErr := fallback.Run(); fbErr != nil {
			return err
		}
	}
	return nil
}

func (r *DockerRunner) restartContainer(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(ctx, r.dockerBin, "restart", containerName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker restart %s: %w", containerName, err)
	}
	return nil
}

func (r *DockerRunner) removeContainer(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(ctx, r.dockerBin, "rm", "-f", containerName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker rm %s: %w", containerName, err)
	}
	return nil
}

func (r *DockerRunner) recoverContainer(containerName, language string, pool *containerPool) {
	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), containerRecoveryTimeout)
	defer recoverCancel()

	if err := r.restartContainer(recoverCtx, containerName); err != nil {
		log.Printf("%s[ERROR]%s failed to restart container %s (%s): %v", colorRed, colorReset, containerName, language, err)
		r.disableContainer(pool, containerName)
		if rmErr := r.removeContainer(recoverCtx, containerName); rmErr != nil {
			log.Printf("%s[WARN]%s failed to remove container %s after restart error: %v", colorYellow, colorReset, containerName, rmErr)
		}
		return
	}

	if err := r.ensureContainer(recoverCtx, containerName); err != nil {
		log.Printf("%s[WARN]%s container %s (%s) still unhealthy after restart: %v", colorYellow, colorReset, containerName, language, err)
		r.disableContainer(pool, containerName)
		return
	}
	r.markContainerAlive(containerName)

	r.captureContainerLimits([]string{containerName})
	if pool != nil {
		pool.add(containerName)
		log.Printf("%s[RESTART]%s recycled container %s for language %s (%d/%d available)", colorGreen, colorReset, containerName, language, pool.availableCount(), pool.capacity())
	}
}

func (r *DockerRunner) startPrewarmLoop() {
	go func() {
		r.triggerPrewarm()
		ticker := time.NewTicker(prewarmInterval)
		defer ticker.Stop()
		for range ticker.C {
			r.triggerPrewarm()
		}
	}()
}

func (r *DockerRunner) triggerPrewarm() {
	if !r.prewarmGate.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer r.prewarmGate.Store(false)
		r.prewarmAll()
	}()
}

func (r *DockerRunner) startDiscoveryLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	r.discoveryCancel = cancel
	go r.discoveryLoop(ctx)
}

func (r *DockerRunner) discoveryLoop(ctx context.Context) {
	r.triggerDiscovery()
	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.triggerDiscovery()
		}
	}
}

func (r *DockerRunner) triggerDiscovery() {
	if !r.discoveryGate.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer r.discoveryGate.Store(false)
		r.refreshDiscoveredPools()
	}()
}

func (r *DockerRunner) refreshDiscoveredPools() {
	detected, err := detectContainerPools(r.dockerBin)
	if err != nil {
		log.Printf("%s[WARN]%s auto-discovery failed: %v", colorYellow, colorReset, err)
		return
	}

	if len(detected) == 0 {
		return
	}

	for lang, names := range detected {
		if len(names) == 0 {
			continue
		}
		pool := r.pools[lang]
		if pool == nil {
			pool = newContainerPool(names)
			if pool != nil && pool.available != nil {
				r.pools[lang] = pool
				log.Printf("%s[DISCOVER]%s registered new pool for %s (%d container(s))", colorMagenta, colorReset, lang, len(names))
				r.captureContainerLimits(names)
				for _, name := range names {
					r.markContainerAlive(name)
				}
			}
			continue
		}
		added := pool.add(names...)
		if added > 0 {
			r.captureContainerLimits(names)
			for _, name := range names {
				r.markContainerAlive(name)
			}
			log.Printf("%s[DISCOVER]%s detected %d new container(s) for %s", colorMagenta, colorReset, added, lang)
		}
	}

	r.disableMissingContainers(detected)
}

func (r *DockerRunner) disableMissingContainers(current map[string][]string) {
	active := make(map[string]struct{})
	for _, names := range current {
		for _, raw := range names {
			name := strings.TrimSpace(raw)
			if name == "" {
				continue
			}
			active[name] = struct{}{}
		}
	}

	for lang, pool := range r.pools {
		if pool == nil {
			continue
		}
		existing := pool.snapshotNames()
		for _, name := range existing {
			trimmed := strings.TrimSpace(name)
			if trimmed == "" {
				continue
			}
			if _, ok := active[trimmed]; ok {
				continue
			}
			if pool.disable(trimmed) {
				r.markContainerDead(trimmed)
				r.finishPrewarm(trimmed)
				r.limitsMu.Lock()
				delete(r.limits, trimmed)
				r.limitsMu.Unlock()
				log.Printf("%s[WARN]%s removed container %s from %s pool after disappearance", colorYellow, colorReset, trimmed, lang)
			}
		}
	}

	if r.fallback != nil {
		existing := r.fallback.snapshotNames()
		for _, name := range existing {
			trimmed := strings.TrimSpace(name)
			if trimmed == "" {
				continue
			}
			if _, ok := active[trimmed]; ok {
				continue
			}
			if r.fallback.disable(trimmed) {
				r.markContainerDead(trimmed)
				r.finishPrewarm(trimmed)
				r.limitsMu.Lock()
				delete(r.limits, trimmed)
				r.limitsMu.Unlock()
				log.Printf("%s[WARN]%s removed fallback container %s after disappearance", colorYellow, colorReset, trimmed)
			}
		}
	}
}

func (r *DockerRunner) prewarmAll() {
	names := r.allContainerNames()
	if len(names) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, name := range names {
		if r.isContainerDead(name) {
			continue
		}
		if !r.beginPrewarm(name) {
			continue
		}
		wg.Add(1)
		go func(containerName string) {
			defer wg.Done()
			r.prewarmContainer(containerName)
		}(name)
	}

	wg.Wait()
}

func (r *DockerRunner) beginPrewarm(name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}

	r.prewarmMu.Lock()
	defer r.prewarmMu.Unlock()
	if _, exists := r.prewarming[trimmed]; exists {
		return false
	}
	r.prewarming[trimmed] = struct{}{}
	return true
}

func (r *DockerRunner) finishPrewarm(name string) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return
	}
	r.prewarmMu.Lock()
	delete(r.prewarming, trimmed)
	r.prewarmMu.Unlock()
}

func (r *DockerRunner) isContainerPrewarming(name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}
	r.prewarmMu.RLock()
	_, ok := r.prewarming[trimmed]
	r.prewarmMu.RUnlock()
	return ok
}

func (r *DockerRunner) prewarmContainer(name string) {
	defer r.finishPrewarm(name)

	ctx, cancel := context.WithTimeout(context.Background(), prewarmTimeout)
	defer cancel()

	if err := r.ensureContainer(ctx, name); err != nil {
		log.Printf("%s[WARN]%s prewarm ensure failed for container %s: %v", colorYellow, colorReset, name, err)
		return
	}

	r.markContainerAlive(name)
}

func (r *DockerRunner) allContainerNames() []string {
	seen := make(map[string]struct{})
	var names []string
	for _, pool := range r.pools {
		for _, name := range pool.snapshotNames() {
			if name == "" {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	if r.fallback != nil {
		for _, name := range r.fallback.snapshotNames() {
			if name == "" {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	return names
}

func (r *DockerRunner) PoolSnapshots() []PoolSnapshot {
	keys := make([]string, 0, len(r.pools))
	for lang := range r.pools {
		keys = append(keys, lang)
	}
	sort.Strings(keys)

	snapshots := make([]PoolSnapshot, 0, len(keys)+1)
	for _, lang := range keys {
		pool := r.pools[lang]
		if pool == nil || pool.capacity() == 0 {
			continue
		}
		names := pool.snapshotNames()
		snapshots = append(snapshots, PoolSnapshot{
			Language:   lang,
			Containers: names,
			Total:      pool.capacity(),
			Available:  pool.availableCount(),
			Limits:     r.aggregateLimits(names),
		})
	}

	if r.fallback != nil && r.fallback.capacity() > 0 {
		names := r.fallback.snapshotNames()
		snapshots = append(snapshots, PoolSnapshot{
			Language:   "fallback",
			Containers: names,
			Total:      r.fallback.capacity(),
			Available:  r.fallback.availableCount(),
			Fallback:   true,
			Limits:     r.aggregateLimits(names),
		})
	}

	return snapshots
}

func normalizeLanguageMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return map[string][]string{}
	}
	out := make(map[string][]string)
	for lang, names := range in {
		canonical, ok := canonicalLanguageName(lang)
		if !ok {
			continue
		}
		combined := append(out[canonical], names...)
		cleaned := dedupeNames(combined)
		if len(cleaned) == 0 {
			continue
		}
		out[canonical] = cleaned
	}
	return out
}

func detectContainerPools(dockerBin string) (map[string][]string, error) {
	cmd := exec.Command(dockerBin, "ps", "--filter", "label=sandbox.language", "--format", `{{.Label "sandbox.language"}}	{{.Names}}`)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w", err)
	}

	trimmed := strings.TrimSpace(string(output))
	if trimmed == "" {
		return map[string][]string{}, nil
	}

	results := make(map[string][]string)
	lines := strings.Split(trimmed, "\n")
	for _, line := range lines {
		parts := strings.SplitN(strings.TrimSpace(line), "\t", 2)
		if len(parts) != 2 {
			continue
		}

		lang, ok := canonicalLanguageName(parts[0])
		if !ok {
			continue
		}

		nameFields := strings.Split(parts[1], ",")
		for _, raw := range nameFields {
			trimmedName := strings.TrimSpace(raw)
			if trimmedName == "" {
				continue
			}
			results[lang] = append(results[lang], trimmedName)
		}
	}

	for lang, names := range results {
		results[lang] = dedupeNames(names)
		if len(results[lang]) == 0 {
			delete(results, lang)
		}
	}

	return results, nil
}

func (r *DockerRunner) captureContainerLimits(names []string) {
	var pending []string

	r.limitsMu.RLock()
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if r.isContainerDead(name) {
			continue
		}
		if _, exists := r.limits[name]; !exists {
			pending = append(pending, name)
		}
	}
	r.limitsMu.RUnlock()

	for _, name := range pending {
		info, err := fetchContainerLimits(r.dockerBin, name)
		if err != nil {
			log.Printf("%s[WARN]%s failed to inspect container %s: %v", colorYellow, colorReset, name, err)
			continue
		}
		r.limitsMu.Lock()
		if _, exists := r.limits[name]; !exists {
			r.limits[name] = info
		}
		r.limitsMu.Unlock()
		r.markContainerAlive(name)
	}
}

func (r *DockerRunner) aggregateLimits(names []string) ContainerLimits {
	r.limitsMu.RLock()
	defer r.limitsMu.RUnlock()

	var agg ContainerLimits
	first := true
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		info, ok := r.limits[name]
		if !ok {
			continue
		}
		if first {
			agg = info
			agg.CapDrop = append([]string(nil), info.CapDrop...)
			agg.Tmpfs = append([]string(nil), info.Tmpfs...)
			agg.Ulimits = cloneUlimitMap(info.Ulimits)
			first = false
			continue
		}
		agg.CPUs = minFloat(agg.CPUs, info.CPUs)
		agg.MemoryBytes = minInt64Positive(agg.MemoryBytes, info.MemoryBytes)
		agg.PidsLimit = minInt64Positive(agg.PidsLimit, info.PidsLimit)
		agg.ReadOnlyRoot = agg.ReadOnlyRoot && info.ReadOnlyRoot
		agg.NoNewPrivileges = agg.NoNewPrivileges && info.NoNewPrivileges
		agg.SeccompProfile = selectCommonString(agg.SeccompProfile, info.SeccompProfile)
		agg.AppArmorProfile = selectCommonString(agg.AppArmorProfile, info.AppArmorProfile)
		agg.CapDrop = intersectStrings(agg.CapDrop, info.CapDrop)
		agg.NetworkMode = selectCommonString(agg.NetworkMode, info.NetworkMode)
		agg.Tmpfs = intersectStrings(agg.Tmpfs, info.Tmpfs)
		agg.Ulimits = mergeUlimits(agg.Ulimits, info.Ulimits)
	}
	return agg
}

type rawContainerInspect struct {
	HostConfig struct {
		NanoCPUs        int64             `json:"NanoCpus"`
		CPUQuota        int64             `json:"CpuQuota"`
		CPUPeriod       int64             `json:"CpuPeriod"`
		Memory          int64             `json:"Memory"`
		PidsLimit       int64             `json:"PidsLimit"`
		SecurityOpt     []string          `json:"SecurityOpt"`
		CapDrop         []string          `json:"CapDrop"`
		ReadonlyRootfs  bool              `json:"ReadonlyRootfs"`
		NoNewPrivileges bool              `json:"NoNewPrivileges"`
		NetworkMode     string            `json:"NetworkMode"`
		Tmpfs           map[string]string `json:"Tmpfs"`
		Ulimits         []struct {
			Name string `json:"Name"`
			Soft int64  `json:"Soft"`
			Hard int64  `json:"Hard"`
		} `json:"Ulimits"`
	} `json:"HostConfig"`
	AppArmorProfile string `json:"AppArmorProfile"`
}

func fetchContainerLimits(dockerBin, name string) (ContainerLimits, error) {
	cmd := exec.Command(dockerBin, "inspect", name)
	output, err := cmd.Output()
	if err != nil {
		return ContainerLimits{}, fmt.Errorf("docker inspect %s: %w", name, err)
	}

	var decoded []rawContainerInspect
	if err := json.Unmarshal(output, &decoded); err != nil {
		return ContainerLimits{}, fmt.Errorf("parse inspect for %s: %w", name, err)
	}
	if len(decoded) == 0 {
		return ContainerLimits{}, fmt.Errorf("no inspect data for %s", name)
	}
	info := decoded[0]

	return ContainerLimits{
		CPUs:            calculateCPUs(info.HostConfig.NanoCPUs, info.HostConfig.CPUQuota, info.HostConfig.CPUPeriod),
		MemoryBytes:     info.HostConfig.Memory,
		PidsLimit:       info.HostConfig.PidsLimit,
		ReadOnlyRoot:    info.HostConfig.ReadonlyRootfs,
		NoNewPrivileges: info.HostConfig.NoNewPrivileges || hasSecurityOpt(info.HostConfig.SecurityOpt, "no-new-privileges:true"),
		SeccompProfile:  extractSecurityOpt(info.HostConfig.SecurityOpt, "seccomp"),
		AppArmorProfile: info.AppArmorProfile,
		CapDrop:         append([]string(nil), info.HostConfig.CapDrop...),
		NetworkMode:     info.HostConfig.NetworkMode,
		Tmpfs:           mapTmpfs(info.HostConfig.Tmpfs),
		Ulimits:         mapUlimits(info.HostConfig.Ulimits),
	}, nil
}

func calculateCPUs(nanoCPUs, quota, period int64) float64 {
	if nanoCPUs > 0 {
		return float64(nanoCPUs) / 1_000_000_000
	}
	if quota > 0 && period > 0 {
		return float64(quota) / float64(period)
	}
	return 0
}

func hasSecurityOpt(opts []string, target string) bool {
	for _, opt := range opts {
		if opt == target {
			return true
		}
	}
	return false
}

func extractSecurityOpt(opts []string, prefix string) string {
	needle := prefix + "="
	for _, opt := range opts {
		if strings.HasPrefix(opt, needle) {
			return strings.TrimPrefix(opt, needle)
		}
	}
	return ""
}

func mapTmpfs(tmpfs map[string]string) []string {
	if len(tmpfs) == 0 {
		return nil
	}
	var paths []string
	for path, opts := range tmpfs {
		if opts != "" {
			paths = append(paths, fmt.Sprintf("%s (%s)", path, opts))
			continue
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func mapUlimits(src []struct {
	Name string `json:"Name"`
	Soft int64  `json:"Soft"`
	Hard int64  `json:"Hard"`
}) map[string]UlimitRange {
	if len(src) == 0 {
		return nil
	}
	result := make(map[string]UlimitRange, len(src))
	for _, item := range src {
		result[strings.ToLower(item.Name)] = UlimitRange{Soft: item.Soft, Hard: item.Hard}
	}
	return result
}

func minFloat(a, b float64) float64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	return math.Min(a, b)
}

func minInt64Positive(a, b int64) int64 {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

func selectCommonString(current, next string) string {
	if current == "" {
		return next
	}
	if next == "" {
		return current
	}
	if current == next {
		return current
	}
	return "mixed"
}

func intersectStrings(a, b []string) []string {
	if len(a) == 0 {
		return append([]string(nil), b...)
	}
	if len(b) == 0 {
		return append([]string(nil), a...)
	}
	set := make(map[string]struct{}, len(b))
	for _, item := range b {
		set[item] = struct{}{}
	}
	var result []string
	for _, item := range a {
		if _, ok := set[item]; ok {
			result = append(result, item)
		}
	}
	return result
}

func mergeUlimits(a, b map[string]UlimitRange) map[string]UlimitRange {
	if len(a) == 0 {
		if len(b) == 0 {
			return nil
		}
		result := make(map[string]UlimitRange, len(b))
		for k, v := range b {
			result[k] = v
		}
		return result
	}
	if len(b) == 0 {
		return a
	}
	for name, limits := range b {
		if existing, ok := a[name]; ok {
			a[name] = UlimitRange{
				Soft: minInt64Positive(existing.Soft, limits.Soft),
				Hard: minInt64Positive(existing.Hard, limits.Hard),
			}
			continue
		}
		a[name] = limits
	}
	return a
}

func cloneUlimitMap(in map[string]UlimitRange) map[string]UlimitRange {
	if len(in) == 0 {
		return nil
	}
	cloned := make(map[string]UlimitRange, len(in))
	for k, v := range in {
		cloned[k] = v
	}
	return cloned
}

func (r *DockerRunner) markContainerDead(name string) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return
	}
	r.deadMu.Lock()
	r.dead[trimmed] = struct{}{}
	r.deadMu.Unlock()
}

func (r *DockerRunner) markContainerAlive(name string) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return
	}
	r.deadMu.Lock()
	delete(r.dead, trimmed)
	r.deadMu.Unlock()
}

func (r *DockerRunner) isContainerDead(name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}
	r.deadMu.RLock()
	_, dead := r.dead[trimmed]
	r.deadMu.RUnlock()
	return dead
}

func selectFallbackContainer(pools map[string][]string) string {
	if len(pools) == 0 {
		return ""
	}
	keys := make([]string, 0, len(pools))
	for k := range pools {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		names := pools[key]
		for _, name := range names {
			trimmed := strings.TrimSpace(name)
			if trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func (r *DockerRunner) disableContainer(pool *containerPool, name string) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return
	}
	if pool != nil && pool.disable(trimmed) {
		log.Printf("%s[WARN]%s disabled container %s", colorYellow, colorReset, trimmed)
	}
	r.markContainerDead(trimmed)
	r.finishPrewarm(trimmed)
	r.limitsMu.Lock()
	delete(r.limits, trimmed)
	r.limitsMu.Unlock()
}

func (r *DockerRunner) startCleanupWorker() {
	go func() {
		for req := range r.cleanupQ {
			r.processCleanup(req)
		}
	}()
}

func (r *DockerRunner) enqueueCleanup(req cleanupRequest) bool {
	if r.cleanupQ == nil {
		return false
	}
	select {
	case r.cleanupQ <- req:
		return true
	default:
		return false
	}
}

func (r *DockerRunner) processCleanup(req cleanupRequest) {
	if req.path == "" {
		return
	}

	err := os.RemoveAll(req.path)
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		log.Printf("%s[CLEAN]%s removed job artifacts at %s", colorMagenta, colorReset, req.path)
		return
	}

	if req.attempt >= cleanupMaxAttempts {
		log.Printf("%s[ERROR]%s failed to cleanup %s after %d attempts: %v", colorRed, colorReset, req.path, req.attempt, err)
		return
	}

	delay := time.Duration(req.attempt) * time.Second
	log.Printf("%s[RETRY]%s cleanup retry for %s in %s (attempt %d/%d)", colorYellow, colorReset, req.path, delay, req.attempt+1, cleanupMaxAttempts)
	time.Sleep(delay)
	req.attempt++
	if ok := r.enqueueCleanup(req); !ok {
		go r.processCleanup(req)
	}
}

func (r *DockerRunner) purgeOrphanedJobDirs() {
	entries, err := os.ReadDir(r.jobDir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("%s[WARN]%s failed to scan job dir %s: %v", colorYellow, colorReset, r.jobDir, err)
		}
		return
	}

	if len(entries) == 0 {
		return
	}

	log.Printf("%s[CLEAN]%s found %d orphaned job artifact(s) to purge", colorMagenta, colorReset, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		req := cleanupRequest{
			path:    filepath.Join(r.jobDir, entry.Name()),
			attempt: 1,
		}
		if ok := r.enqueueCleanup(req); !ok {
			go r.processCleanup(req)
		}
	}
}
