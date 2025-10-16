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

1. **Start the sandbox runners** (inside the dev container):
   ```bash
   docker compose up -d \
       runner-go-1 runner-go-2 \
       runner-python-1 runner-python-2 \
       runner-node-1 runner-node-2
   ```
   Go runners embed only the Go toolchain, Python runners embed only CPython, and Node runners embed only Node.js. The API automatically balances work across the pool based on language, so Go jobs route to `runner-go-*`, Python jobs to `runner-python-*`, and JavaScript/Node jobs to `runner-node-*`. All runners share the `/tmp/jobs` volume to exchange job files with the host. Containers are discovered dynamically by reading the `sandbox.language` Docker label, so additional language-specific runners can be added without configuration changes as long as they apply the label (for example `sandbox.language=node`).
1. **Start Redis** (inside the dev container):
   ```bash
   docker compose up -d redis
   ```
1. **Start PostgreSQL for account data** (inside the dev container):
   ```bash
   docker compose up -d postgres
   ```
   Credentials are for local development only. Data persists in the `postgres-data` volume so the authentication tables survive container restarts.
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
      -d '{"language":"javascript","code":"console.log(\"hi\")"}'
   ```
   The response returns the queued job identifier and creation timestamp. Supported languages are `go`, `python`, and `javascript` (aliases `js`, `node`, `nodejs`). You may provide optional `stdin` and `timeout` values—`timeout` uses Go duration syntax and overrides the service default set via `SANDBOX_TIMEOUT`.
1. **Poll job status**:

   ```bash
   curl http://localhost:8080/v1/execute/<job-id>
   ```

   Responses include job metadata, the most recent status, and execution output once the job completes.

   ## Authentication Portal Frontend

   The marketing site and authentication portal live in `website-codeportal`. It currently serves register and login forms that will submit to the API once the account endpoints land.

   ```bash
   cd website-codeportal
   npm install
   npm run dev
   ```

   Replace the placeholder submit handlers with real API calls after wiring the Go service to PostgreSQL.

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
- `SANDBOX_CONTAINER`: Optional fallback container used when no language-specific pool is detected.
- `SANDBOX_LANGUAGE_CONTAINERS`: Optional comma-separated mapping of languages to container pools. When unset, the service auto-discovers containers by reading the `sandbox.language` label from running Docker containers (expected values: `go`, `python`, `node`). Each language entry uses `|` to separate container names.
- `SANDBOX_JOB_DIR`: Host directory used for job files (default `/tmp/jobs`).
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
- Implement account service backed by PostgreSQL and connect the frontend auth flows.
