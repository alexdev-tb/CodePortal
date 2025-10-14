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
	"sort"
	"strings"
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
	jobDir    string
	dockerBin string
	network   string
	execUser  string
	languages map[string]languageSpec
	pools     map[string]*containerPool
	fallback  *containerPool
	cleanupQ  chan cleanupRequest
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

type PoolSnapshot struct {
	Language   string
	Containers []string
	Total      int
	Available  int
	Fallback   bool
}

type cleanupRequest struct {
	path    string
	attempt int
}

type languageSpec struct {
	extension string
	args      []string
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
		},
		pools:    pools,
		fallback: fallbackPool,
		cleanupQ: make(chan cleanupRequest, cleanupQueueSize),
	}

	r.startCleanupWorker()
	r.purgeOrphanedJobDirs()

	return r
}

func (r *DockerRunner) Run(ctx context.Context, jobID string, req Request, timeout time.Duration) (Result, error) {
	language := strings.ToLower(strings.TrimSpace(req.Language))
	spec, ok := r.languages[language]
	if !ok {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("unsupported language %q", req.Language)
	}

	pool := r.pools[language]
	if pool == nil {
		pool = r.fallback
	}
	if pool == nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("no containers configured for language %q", req.Language)
	}

	containerName, err := r.acquireContainer(ctx, pool, language, jobID)
	if err != nil {
		now := time.Now().UTC()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("context canceled while waiting for container: %w", err)
		}
		if errors.Is(err, errNoContainersConfigured) {
			return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("no containers available for language %q", req.Language)
		}
		return Result{ExitCode: -1, CompletedAt: now}, fmt.Errorf("acquire container: %w", err)
	}
	defer func() {
		if released := pool.release(containerName); released {
			log.Printf("%s[RELEASE]%s job %s (%s) released container %s (%d/%d available)", colorCyan, colorReset, jobID, language, containerName, pool.availableCount(), pool.capacity())
		} else {
			log.Printf("%s[WARN]%s job %s (%s) release dropped for container %s", colorYellow, colorReset, jobID, language, containerName)
		}
	}()

	if err := r.ensureContainer(ctx, containerName); err != nil {
		now := time.Now().UTC()
		return Result{ExitCode: -1, CompletedAt: now}, err
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

	exitCode := 0
	if runErr != nil {
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

	if runErr != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			return result, fmt.Errorf("execution timed out after %s", timeout)
		}
		if execErr := (*exec.ExitError)(nil); errors.As(runErr, &execErr) {
			return result, fmt.Errorf("process exited with status %d", exitCode)
		}
		return result, fmt.Errorf("execution error: %w", runErr)
	}

	return result, nil
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

	name, err := pool.acquire(ctx)
	if err != nil {
		log.Printf("%s[ERROR]%s job %s (%s) failed to acquire container: %v", colorRed, colorReset, jobID, language, err)
		return "", err
	}

	remaining := pool.availableCount()
	log.Printf("%s[RUN]%s job %s (%s) assigned to container %s (%d/%d available)", colorGreen, colorReset, jobID, language, name, remaining, total)
	return name, nil
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
		snapshots = append(snapshots, PoolSnapshot{
			Language:   lang,
			Containers: pool.snapshotNames(),
			Total:      pool.capacity(),
			Available:  pool.availableCount(),
		})
	}

	if r.fallback != nil && r.fallback.capacity() > 0 {
		snapshots = append(snapshots, PoolSnapshot{
			Language:   "fallback",
			Containers: r.fallback.snapshotNames(),
			Total:      r.fallback.capacity(),
			Available:  r.fallback.availableCount(),
			Fallback:   true,
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
		lowerLang := strings.ToLower(strings.TrimSpace(lang))
		if lowerLang == "" {
			continue
		}
		cleaned := dedupeNames(names)
		if len(cleaned) == 0 {
			continue
		}
		out[lowerLang] = cleaned
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

		lang := strings.ToLower(strings.TrimSpace(parts[0]))
		if lang == "" {
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
