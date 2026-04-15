# TPC-C Benchmark for PostgreSQL

A C++23 implementation of the [TPC-C](http://www.tpc.org/tpcc/) benchmark for PostgreSQL.
Ported from a YDB CLI implementation. Uses custom futures/promises with C++20 coroutines,
libpqxx for PostgreSQL access, and an optional ftxui-based terminal UI.

## Dependencies

Requires Clang 16+ (tested with Clang 20) and the PostgreSQL client library:
```
sudo apt install libpq-dev
```

All other dependencies (fmt, spdlog, gflags, libpqxx, ftxui, googletest) are bundled
as git submodules and built automatically.

## Building

```
git submodule update --init
cmake -B build -DCMAKE_CXX_COMPILER=clang++-20 -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

## Running

### Quick start

```
# Create a database
createdb tpcc

# Create schema and indexes
./build/tpcc --command=init

# Load data (10 warehouses, parallel import)
./build/tpcc --command=import --warehouses=10 --load-threads=8

# Verify loaded data
./build/tpcc --command=check --warehouses=10 --after-import

# Run the benchmark (5 minutes)
./build/tpcc --command=run --warehouses=10 --duration=300

# Run consistency checks after benchmark
./build/tpcc --command=check --warehouses=10

# Clean up
./build/tpcc --command=clean
```

### Connection

By default connects to `host=localhost dbname=tpcc user=postgres`.
Override with `--connection`:
```
./build/tpcc --command=run --connection="host=myhost dbname=tpcc user=bench password=secret"
```

### Options

Run `./build/tpcc --help` for the full list. Key options:

| Flag | Default | Description |
|------|---------|-------------|
| `--warehouses` | 1 | Number of warehouses (scales data and terminals) |
| `--duration` | 600 | Benchmark duration in seconds |
| `--warmup` | 0 | Warmup period before measurement starts |
| `--threads` | auto | Coroutine threads |
| `--max-inflight` | auto | Max concurrent transactions |
| `--io-threads` | 4 | I/O threads for libpqxx |
| `--no-delays` | false | Disable TPC-C keying/think time delays |
| `--no-tui` | false | Disable terminal UI (console output instead) |
| `--after-import` | false | Check: verify freshly loaded data (stricter invariants) |

## Testing

### Unit tests

Built automatically when Google Test is found:
```
cmake --build build --target tpcc_tests -j$(nproc)
./build/tpcc_tests
```

### Integration tests against PostgreSQL

A Docker Compose file is provided to spin up a PostgreSQL 18 instance:
```
docker compose up -d
```

Run the end-to-end smoke test (init, import, check, run, check, clean):
```
PGHOST=localhost PGUSER=postgres PGPASSWORD=postgres \
    TPCC_BIN=./build/tpcc tests/smoke_test.sh
```

The smoke test defaults to 10 warehouses and 120 seconds with standard TPC-C
keying/think time delays. Override via environment variables:
```
TPCC_WAREHOUSES=5 TPCC_DURATION=60 tests/smoke_test.sh
```

Per-transaction correctness tests (requires Google Test and a running PostgreSQL):
```
cmake --build build --target tpcc_pg_tests -j$(nproc)
TPCC_TEST_CONNECTION="host=localhost dbname=tpcc_test user=postgres password=postgres" \
    ./build/tpcc_pg_tests
```

### Simulation mode

Test the coroutine/IO infrastructure without a real database:
```
# Pure sleep simulation (no DB connection needed)
./build/tpcc --command=run --simulate-ms=50 --duration=10 --no-tui

# SELECT 1 simulation (needs a running PostgreSQL)
./build/tpcc --command=run --simulate-select1=5 --duration=10
```
