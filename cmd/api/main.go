package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/alexdev-tb/CodePortal/internal/api"
	"github.com/alexdev-tb/CodePortal/internal/auth"
	"github.com/alexdev-tb/CodePortal/internal/config"
	"github.com/alexdev-tb/CodePortal/internal/executor"
	"github.com/alexdev-tb/CodePortal/internal/server"
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

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	// Connect to PostgreSQL
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://codeportal:codeportal-dev@localhost:5432/codeportal_accounts?sslmode=disable"
		log.Printf("%s[DB]%s using default database URL (set DATABASE_URL env var for production)", colorYellow, colorReset)
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test database connection
	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	// Set up execution service
	store := executor.NewRedisJobStore(rdb)
	runner := executor.NewDockerRunner(executor.RunnerConfig{
		Container:          cfg.Sandbox.Container,
		LanguageContainers: cfg.Sandbox.Languages,
		JobDir:             cfg.Sandbox.JobDir,
		ContainerJobDir:    cfg.Sandbox.ContainerJobDir,
		DockerBinary:       cfg.Sandbox.Docker.Binary,
		Network:            cfg.Sandbox.Docker.Network,
		ExecUser:           cfg.Sandbox.Docker.User,
	})
	logPoolDetails(runner.PoolSnapshots())
	execService := executor.NewService(store, runner, cfg.Sandbox.Timeout)

	// Set up authentication service with PostgreSQL
	userStore, err := auth.NewPostgresStore(db)
	if err != nil {
		log.Fatalf("failed to initialize user store: %v", err)
	}

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = "development-secret-key-change-in-production"
		log.Printf("%s[AUTH]%s using default JWT secret (change JWT_SECRET env var for production)", colorYellow, colorReset)
	}
	authService := auth.NewService(userStore, jwtSecret)

	// Set up handlers
	apiHandlers := api.NewHandler(execService)
	authHandlers := auth.NewHandler(authService)
	router := api.NewRouter(apiHandlers, authHandlers)

	srv := server.New(cfg.HTTP, router)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("%s[AUTH]%s authentication service initialized with PostgreSQL", colorCyan, colorReset)
	log.Printf("%s[DB]%s database connection established", colorCyan, colorReset)

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
