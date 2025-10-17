package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
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
	ContainerJobDir    string
	DockerBinary       string
	Network            string
	ExecUser           string
}

type DockerRunner struct {
	jobDir           string
	containerJobRoot string
	dockerBin        string
	network          string
	execUser         string
	languages        map[string]languageSpec
	pools            map[string]*containerPool
	fallback         *containerPool
	cleanupQ         chan cleanupRequest
	limits           map[string]ContainerLimits
	limitsMu         sync.RWMutex
	dead             map[string]struct{}
	deadMu           sync.RWMutex
	prewarming       map[string]struct{}
	prewarmMu        sync.RWMutex
	prewarmGate      atomic.Bool
	discoveryGate    atomic.Bool
	discoveryCancel  context.CancelFunc
	overheadStats    map[string]*overheadTracker
	overheadMu       sync.RWMutex
	prewarmState     map[string]*prewarmStatus
	prewarmStateMu   sync.Mutex
	tmpBaselines     map[string]map[string]struct{}
	tmpBaselineMu    sync.RWMutex
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
	heartbeatInterval         = 2 * time.Second
	heartbeatProbeTimeout     = 750 * time.Millisecond
	heartbeatFailureThreshold = 3
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
		jobDir = filepath.Join(os.TempDir(), "codeportal-jobs")
	}
	containerJobRoot := strings.TrimSpace(cfg.ContainerJobDir)
	if containerJobRoot == "" {
		containerJobRoot = "/tmp"
	}
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		log.Printf("%s[WARN]%s ensure job staging dir %s: %v", colorYellow, colorReset, jobDir, err)
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
		jobDir:           jobDir,
		containerJobRoot: containerJobRoot,
		dockerBin:        dockerBin,
		network:          network,
		execUser:         execUser,
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
		pools:         pools,
		fallback:      fallbackPool,
		cleanupQ:      make(chan cleanupRequest, cleanupQueueSize),
		limits:        make(map[string]ContainerLimits),
		dead:          make(map[string]struct{}),
		prewarming:    make(map[string]struct{}),
		overheadStats: make(map[string]*overheadTracker),
		prewarmState:  make(map[string]*prewarmStatus),
		tmpBaselines:  make(map[string]map[string]struct{}),
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
	if tmpCtx, tmpCancel := context.WithTimeout(ctx, time.Second); tmpCancel != nil {
		r.ensureTmpBaseline(tmpCtx, containerName)
		tmpCancel()
	}
	baselineCtx, baselineCancel := context.WithTimeout(ctx, time.Second)
	r.ensureTmpBaseline(baselineCtx, containerName)
	baselineCancel()

	hostJobPath := filepath.Join(r.jobDir, jobID)
	if err := os.MkdirAll(hostJobPath, 0o755); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare job dir: %w", err), false
	}

	sourceFilename := fmt.Sprintf("main.%s", spec.extension)
	hostSourcePath := filepath.Join(hostJobPath, sourceFilename)
	if err := os.WriteFile(hostSourcePath, []byte(req.Code), 0o644); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("write source: %w", err), false
	}

	hostCacheDir := filepath.Join(hostJobPath, "cache")
	if err := os.MkdirAll(hostCacheDir, 0o755); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare cache dir: %w", err), false
	}

	hostModCacheDir := filepath.Join(hostJobPath, "gomod")
	if err := os.MkdirAll(hostModCacheDir, 0o755); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare module cache dir: %w", err), false
	}

	hostTmpDir := filepath.Join(hostJobPath, "tmp")
	if err := os.MkdirAll(hostTmpDir, 0o755); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare temp dir: %w", err), false
	}

	hostPyCacheDir := filepath.Join(hostJobPath, "pycache")
	if err := os.MkdirAll(hostPyCacheDir, 0o755); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare python cache dir: %w", err), false
	}

	defer r.cleanup(hostJobPath)

	containerJobPath := path.Join(r.containerJobRoot, jobID)
	workspaceReady := false
	defer func() {
		if !workspaceReady {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		if err := r.dockerExec(cleanupCtx, containerName, "rm", "-rf", containerJobPath); err != nil {
			log.Printf("%s[WARN]%s cleanup container workspace %s/%s: %v", colorYellow, colorReset, containerName, containerJobPath, err)
		}
		r.restoreContainerTmp(cleanupCtx, containerName)
	}()

	if err := r.prepareContainerWorkspace(ctx, containerName, containerJobPath); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("prepare container workspace: %w", err), false
	}
	workspaceReady = true
	if err := r.copyWorkspaceToContainer(ctx, hostJobPath, containerName, containerJobPath); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("seal workspace: %w", err), false
	}

	containerSourcePath := path.Join(containerJobPath, sourceFilename)
	containerCacheDir := path.Join(containerJobPath, "cache")
	containerModCacheDir := path.Join(containerJobPath, "gomod")
	containerTmpDir := path.Join(containerJobPath, "tmp")
	containerPyCacheDir := path.Join(containerJobPath, "pycache")

	estimatedOverhead := r.estimateOverhead(canonical)
	effectiveTimeout, timeoutOverhead := computeExecutionBudget(timeout, estimatedOverhead)

	execCtx, cancel := context.WithTimeout(ctx, effectiveTimeout)
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
		fmt.Sprintf("TMPDIR=%s", containerTmpDir),
	}
	if canonical == "go" {
		envVars = append(envVars,
			fmt.Sprintf("GOCACHE=%s", containerCacheDir),
			fmt.Sprintf("GOMODCACHE=%s", containerModCacheDir),
			fmt.Sprintf("GOTMPDIR=%s", containerTmpDir),
			"GO111MODULE=off",
		)
	}
	if canonical == "python" {
		envVars = append(envVars,
			fmt.Sprintf("PYTHONPYCACHEPREFIX=%s", containerPyCacheDir),
		)
	}
	if canonical == "node" {
		envVars = append(envVars,
			fmt.Sprintf("HOME=%s", containerJobPath),
			"NODE_OPTIONS=--max-old-space-size=192",
		)
	}

	for _, env := range envVars {
		cmdArgs = append(cmdArgs, "--env", env)
	}

	cmdArgs = append(cmdArgs, "--workdir", containerJobPath)
	cmdArgs = append(cmdArgs, containerName)
	cmdArgs = append(cmdArgs, spec.args...)
	cmdArgs = append(cmdArgs, containerSourcePath)

	cmd := exec.CommandContext(execCtx, r.dockerBin, cmdArgs...)

	if req.Stdin != "" {
		cmd.Stdin = strings.NewReader(req.Stdin)
	}

	var stdoutBuf bytes.Buffer
	var stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	started := time.Now()
	startErr := cmd.Start()
	bootstrapDuration := time.Since(started)
	var runErr error
	if startErr != nil {
		runErr = startErr
	} else {
		runErr = cmd.Wait()
	}
	duration := time.Since(started)
	r.recordOverhead(canonical, bootstrapDuration)
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
			overheadRounded := timeoutOverhead.Round(10 * time.Millisecond)
			return result, fmt.Errorf("execution timed out: limit=%s runtime=%s overhead=%s", timeout, duration.Round(time.Millisecond), overheadRounded), false
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
				r.clearPrewarmState(trimmed)
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
				r.clearPrewarmState(trimmed)
				r.limitsMu.Lock()
				delete(r.limits, trimmed)
				r.limitsMu.Unlock()
				log.Printf("%s[WARN]%s removed fallback container %s after disappearance", colorYellow, colorReset, trimmed)
			}
		}
	}
}

func (r *DockerRunner) prepareContainerWorkspace(ctx context.Context, containerName, containerPath string) error {
	if err := r.dockerExec(ctx, containerName, "mkdir", "-p", r.containerJobRoot); err != nil {
		return fmt.Errorf("ensure job root %s: %w", r.containerJobRoot, err)
	}
	if err := r.dockerExec(ctx, containerName, "rm", "-rf", containerPath); err != nil {
		return fmt.Errorf("purge workspace %s: %w", containerPath, err)
	}
	if err := r.dockerExec(ctx, containerName, "mkdir", "-p", containerPath); err != nil {
		return fmt.Errorf("create workspace %s: %w", containerPath, err)
	}
	return nil
}

func (r *DockerRunner) ensureTmpBaseline(ctx context.Context, containerName string) {
	r.tmpBaselineMu.RLock()
	if _, ok := r.tmpBaselines[containerName]; ok {
		r.tmpBaselineMu.RUnlock()
		return
	}
	r.tmpBaselineMu.RUnlock()

	lst, err := r.listContainerTmp(ctx, containerName)
	if err != nil {
		log.Printf("%s[WARN]%s snapshot tmp baseline for %s failed: %v", colorYellow, colorReset, containerName, err)
		return
	}
	baseline := make(map[string]struct{}, len(lst))
	for _, entry := range lst {
		baseline[entry] = struct{}{}
	}
	r.tmpBaselineMu.Lock()
	r.tmpBaselines[containerName] = baseline
	r.tmpBaselineMu.Unlock()
}

func (r *DockerRunner) restoreContainerTmp(ctx context.Context, containerName string) {
	r.tmpBaselineMu.RLock()
	baseline, ok := r.tmpBaselines[containerName]
	r.tmpBaselineMu.RUnlock()
	if !ok {
		return
	}

	lst, err := r.listContainerTmp(ctx, containerName)
	if err != nil {
		log.Printf("%s[WARN]%s restore tmp for %s failed: %v", colorYellow, colorReset, containerName, err)
		return
	}

	toRemove := make([]string, 0, len(lst))
	for _, entry := range lst {
		if _, keep := baseline[entry]; !keep {
			toRemove = append(toRemove, path.Join("/tmp", entry))
		}
	}
	if len(toRemove) == 0 {
		return
	}
	args := append([]string{"rm", "-rf"}, toRemove...)
	if err := r.dockerExec(ctx, containerName, args...); err != nil {
		log.Printf("%s[WARN]%s cleanup extra tmp entries for %s: %v", colorYellow, colorReset, containerName, err)
	}
}

func (r *DockerRunner) listContainerTmp(ctx context.Context, containerName string) ([]string, error) {
	cmd := exec.CommandContext(ctx, r.dockerBin, "exec", containerName, "ls", "-A", "/tmp")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker exec ls /tmp: %w", err)
	}
	trimmed := strings.TrimSpace(string(output))
	if trimmed == "" {
		return nil, nil
	}
	entries := strings.Split(trimmed, "\n")
	return entries, nil
}

func (r *DockerRunner) copyWorkspaceToContainer(ctx context.Context, hostPath, containerName, containerPath string) error {
	tarCmd := exec.CommandContext(ctx, "tar", "-C", hostPath, "-cf", "-", ".")
	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("tar workspace: %w", err)
	}
	var tarErrBuf bytes.Buffer
	tarCmd.Stderr = &tarErrBuf

	dockerArgs := []string{"exec", "-i", containerName, "tar", "-C", containerPath, "-xf", "-"}
	dockerCmd := exec.CommandContext(ctx, r.dockerBin, dockerArgs...)
	dockerCmd.Stdin = tarStdout
	var dockerErrBuf bytes.Buffer
	dockerCmd.Stderr = &dockerErrBuf

	if err := tarCmd.Start(); err != nil {
		return fmt.Errorf("tar workspace: %w", err)
	}
	if err := dockerCmd.Start(); err != nil {
		_ = tarCmd.Wait()
		msg := strings.TrimSpace(dockerErrBuf.String())
		if msg != "" {
			return fmt.Errorf("stream workspace into container: %w (%s)", err, msg)
		}
		return fmt.Errorf("stream workspace into container: %w", err)
	}

	tarErr := tarCmd.Wait()
	dockerErr := dockerCmd.Wait()
	if tarErr != nil {
		msg := strings.TrimSpace(tarErrBuf.String())
		if msg != "" {
			return fmt.Errorf("tar workspace: %w (%s)", tarErr, msg)
		}
		return fmt.Errorf("tar workspace: %w", tarErr)
	}
	if dockerErr != nil {
		msg := strings.TrimSpace(dockerErrBuf.String())
		if msg != "" {
			return fmt.Errorf("extract workspace in container: %w (%s)", dockerErr, msg)
		}
		return fmt.Errorf("extract workspace in container: %w", dockerErr)
	}

	return nil
}

func (r *DockerRunner) dockerExec(ctx context.Context, containerName string, args ...string) error {
	fullArgs := append([]string{"exec", containerName}, args...)
	cmd := exec.CommandContext(ctx, r.dockerBin, fullArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed != "" {
			return fmt.Errorf("docker exec %s %v: %w (%s)", containerName, args, err, trimmed)
		}
		return fmt.Errorf("docker exec %s %v: %w", containerName, args, err)
	}
	return nil
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
	r.clearPrewarmState(trimmed)
	r.limitsMu.Lock()
	delete(r.limits, trimmed)
	r.limitsMu.Unlock()
}
