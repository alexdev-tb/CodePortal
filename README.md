# Code Sandbox API

Stage 2 of the Go-based code execution service. This iteration introduces asynchronous execution with persistent job tracking backed by Redis, while keeping the HTTP surface minimal for incremental development. The structure is designed to support future extensions like real sandbox integration and observability.

## Project Layout

```
.
├── cmd/
│   └── api/             # Service entrypoint
├── internal/
│   ├── api/             # HTTP handlers and routing
│   ├── config/          # Environment-driven runtime configuration
│   ├── executor/        # Asynchronous execution orchestration & job store
│   └── server/          # HTTP server wiring and lifecycle helpers
└── README.md
```

## Getting Started

1. **Start the sandbox runner** (inside the dev container):
   ```bash
   docker compose up -d runner
   ```
   The `code-sandbox-runner` container exposes Go and Python runtimes with a shared `/tmp` volume for job files.
1. **Start Redis** (inside the dev container):
   ```bash
   docker compose up -d redis
   ```
1. **Run the API**:
   ```bash
   go run ./cmd/api
   ```
1. **Call the health endpoint**:
   ```bash
   curl http://localhost:8080/health
   ```
1. **Submit execution job**:
    ```bash
    curl -X POST http://localhost:8080/v1/execute \
       -H "Content-Type: application/json" \
          -d '{"language":"go","code":"package main\nfunc main() { println(\"hi\") }"}'
    ```
    The response returns the queued job identifier and creation timestamp. Supported languages are `go` and `python`. You may provide optional `stdin` and `timeout` values—`timeout` uses Go duration syntax and overrides the service default set via `SANDBOX_TIMEOUT`.
1. **Poll job status**:
   ```bash
   curl http://localhost:8080/v1/execute/<job-id>
   ```
   Responses include job metadata, the most recent status, and execution output once the job completes.

## Configuration

Environment variables:

- `HTTP_HOST`: Address to bind (default `0.0.0.0`).
- `HTTP_PORT`: Port to listen on (default `8080`).
- `HTTP_READ_TIMEOUT`: HTTP read timeout (default `5s`).
- `HTTP_WRITE_TIMEOUT`: HTTP write timeout (default `10s`).
- `HTTP_SHUTDOWN_TIMEOUT`: Graceful shutdown timeout (default `15s`).
- `REDIS_ADDR`: Redis host:port (default `redis:6379`).
- `REDIS_PASSWORD`: Redis password (default empty).
- `REDIS_DB`: Redis database index (default `0`).
- `SANDBOX_CONTAINER`: Name of the persistent execution container (default `code-sandbox-runner`).
- `SANDBOX_JOB_DIR`: Host dd used for job files (default `/tmp/jobs`).
- `SANDBOX_TIMEOUT`: Maximum wall-clock runtime per job (default `3s`).
- `SANDBOX_DOCKER_BIN`: Docker CLI binary to invoke (default `docker`).
- `SANDBOX_NETWORK`: Optional Docker network to attach when exec'ing (default empty).
- `SANDBOX_DOCKER_USER`: UID(:GID) passed to `docker exec --user` (default current process user).

Durations accept Go's duration syntax (e.g., `10s`, `500ms`).

## Testing

```bash
go test ./...
```

## Next Steps

- Harden sandbox isolation (cgroups, seccomp) and monitor runtime resource usage.
- Instrument the service with structured logging and tracing.
- Wire up CI and automation.
