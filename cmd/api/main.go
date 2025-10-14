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
		Container:    cfg.Sandbox.Container,
		JobDir:       cfg.Sandbox.JobDir,
		DockerBinary: cfg.Sandbox.Docker.Binary,
		Network:      cfg.Sandbox.Docker.Network,
		ExecUser:     cfg.Sandbox.Docker.User,
	})
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
