# storm dev tasks.

# List all recipes with their descriptions.
default:
    @just --list

# Run every release gate in order: vet, lint, test, build.
check: vet lint test build

# Static analysis of every package with go vet.
vet:
    go vet ./...

# Lint every package; settings live in .golangci.yml.
lint:
    golangci-lint run

# Compile-check every package; the output is discarded.
build:
    go build -o /dev/null ./...

# Format every Go source in place with gofmt.
fmt:
    gofmt -w .

# Regenerate the typed columns from model directives.
generate:
    go generate ./...

# Run the suite against SQLite (no setup needed).
test:
    go test ./...

pg_dsn      := "postgres://storm:storm@127.0.0.1:54329/storm?sslmode=disable"
pg_audit    := "postgres://storm:storm@127.0.0.1:54329/storm_audit?sslmode=disable"
mysql_dsn   := "storm:storm@tcp(127.0.0.1:33069)/storm?parseTime=true&multiStatements=true"
mysql_audit := "storm:storm@tcp(127.0.0.1:33069)/storm_audit?parseTime=true"
maria_dsn   := "storm:storm@tcp(127.0.0.1:33070)/storm?parseTime=true&multiStatements=true"
maria_audit := "storm:storm@tcp(127.0.0.1:33070)/storm_audit?parseTime=true"
ch_dsn      := "clickhouse://default:storm@127.0.0.1:19000/storm?dial_timeout=5s"
mongo_dsn   := "mongodb://127.0.0.1:17017/?directConnection=true"
qdrant_url  := "http://127.0.0.1:16333"

# Run the suite against Postgres with pgvector.
test-postgres:
    STORM_TEST_DRIVER=pgx STORM_TEST_DSN='{{pg_dsn}}' STORM_TEST_AUDIT_DSN='{{pg_audit}}' go test -count=1 -p 1 ./...

# Run the suite against MySQL 8.
test-mysql:
    STORM_TEST_DRIVER=mysql STORM_TEST_DSN='{{mysql_dsn}}' STORM_TEST_AUDIT_DSN='{{mysql_audit}}' go test -count=1 -p 1 ./...

# Run the suite against MariaDB.
test-mariadb:
    STORM_TEST_DRIVER=mysql STORM_TEST_DSN='{{maria_dsn}}' STORM_TEST_AUDIT_DSN='{{maria_audit}}' go test -count=1 -p 1 ./...

# Run the DuckDB tests; the engine is embedded, no container needed.
test-duckdb:
    go test -count=1 -p 1 -run TestDuckDB ./example/models/

# Run the ClickHouse tests.
test-clickhouse:
    STORM_TEST_CLICKHOUSE='{{ch_dsn}}' go test -count=1 -p 1 -run TestClickHouse ./example/models/

# Run the MongoDB tests.
test-mongo:
    STORM_TEST_MONGO='{{mongo_dsn}}' go test -count=1 -p 1 -run TestMongo ./example/models/

# Run the Qdrant tests.
test-qdrant:
    STORM_TEST_QDRANT='{{qdrant_url}}' go test -count=1 -p 1 -run TestQdrant ./example/models/

# Run the DuckDB, ClickHouse, MongoDB and Qdrant tests.
test-stores: test-duckdb test-clickhouse test-mongo test-qdrant

# Run the suite against every engine.
test-all: test test-postgres test-mysql test-mariadb test-stores
