package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/alexdev-tb/code-sandbox-api/internal/api"
	"github.com/alexdev-tb/code-sandbox-api/internal/config"
	"github.com/alexdev-tb/code-sandbox-api/internal/executor"
	"github.com/alexdev-tb/code-sandbox-api/internal/server"
)

const (
	colorReset   = "\033[0m"
	colorMagenta = "\033[35m"
	colorCyan    = "\033[36m"
	colorYellow  = "\033[33m"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	store := executor.NewRedisJobStore(rdb)
	runner := executor.NewDockerRunner(executor.RunnerConfig{
		Container:          cfg.Sandbox.Container,
		LanguageContainers: cfg.Sandbox.Languages,
		JobDir:             cfg.Sandbox.JobDir,
		DockerBinary:       cfg.Sandbox.Docker.Binary,
		Network:            cfg.Sandbox.Docker.Network,
		ExecUser:           cfg.Sandbox.Docker.User,
	})
	logPoolDetails(runner.PoolSnapshots())
	execService := executor.NewService(store, runner, cfg.Sandbox.Timeout)
	handlers := api.NewHandler(execService)
	router := api.NewRouter(handlers)

	srv := server.New(cfg.HTTP, router)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		if err == server.ErrServerClosed {
			log.Println("server shutdown gracefully")
			return
		}
		log.Printf("server error: %v", err)
		os.Exit(1)
	}
}

func logPoolDetails(snapshots []executor.PoolSnapshot) {
	if len(snapshots) == 0 {
		log.Printf("%s[BOOT]%s no sandbox containers detected", colorYellow, colorReset)
		return
	}

	log.Printf("%s[BOOT]%s detected %d sandbox pool(s)", colorMagenta, colorReset, len(snapshots))
	for _, snap := range snapshots {
		label := snap.Language
		if snap.Fallback {
			label = "fallback"
		}
		log.Printf("%s[BOOT]%s %-9s containers=%v total=%d available=%d limits=%s", colorCyan, colorReset, label, snap.Containers, snap.Total, snap.Available, formatLimits(snap.Limits))
	}
}

func formatLimits(l executor.ContainerLimits) string {
	return executor.FormatLimits(l)
}
