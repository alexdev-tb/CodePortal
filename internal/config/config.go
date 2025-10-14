package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type HTTP struct {
	Host            string
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type Config struct {
	HTTP    HTTP
	Redis   Redis
	Sandbox Sandbox
}

type Redis struct {
	Addr     string
	Password string
	DB       int
}

type Sandbox struct {
	Container string
	JobDir    string
	Docker    Docker
	Timeout   time.Duration
}

type Docker struct {
	Binary  string
	Network string
	User    string
}

func FromEnv() (Config, error) {
	http := HTTP{
		Host:            getEnv("HTTP_HOST", "0.0.0.0"),
		Port:            getInt("HTTP_PORT", 8080),
		ReadTimeout:     getDuration("HTTP_READ_TIMEOUT", 5*time.Second),
		WriteTimeout:    getDuration("HTTP_WRITE_TIMEOUT", 10*time.Second),
		ShutdownTimeout: getDuration("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second),
	}

	redis := Redis{
		Addr:     getEnv("REDIS_ADDR", "172.18.0.1:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       getInt("REDIS_DB", 0),
	}

	sandbox := Sandbox{
		Container: getEnv("SANDBOX_CONTAINER", "code-sandbox-runner"),
		JobDir:    getEnv("SANDBOX_JOB_DIR", "/tmp/jobs"),
		Timeout:   getDuration("SANDBOX_TIMEOUT", 3*time.Second),
		Docker: Docker{
			Binary:  getEnv("SANDBOX_DOCKER_BIN", "docker"),
			Network: getEnv("SANDBOX_NETWORK", ""),
			User:    getEnv("SANDBOX_DOCKER_USER", ""),
		},
	}
	if sandbox.Timeout <= 0 {
		sandbox.Timeout = 3 * time.Second
	}

	if http.Port <= 0 || http.Port > 65535 {
		return Config{}, fmt.Errorf("invalid port: %d", http.Port)
	}

	return Config{HTTP: http, Redis: redis, Sandbox: sandbox}, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func getInt(key string, fallback int) int {
	value := getEnv(key, "")
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func getDuration(key string, fallback time.Duration) time.Duration {
	value := getEnv(key, "")
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return d
}
