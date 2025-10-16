package executor

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	cleanupQueueSize   = 128
	cleanupMaxAttempts = 5
)

type cleanupRequest struct {
	path    string
	attempt int
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
