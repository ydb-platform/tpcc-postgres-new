# TPC-C Benchmark for PostgreSQL

A C++20 implementation of the [TPC-C](http://www.tpc.org/tpcc/) benchmark for PostgreSQL.
Ported from a YDB CLI implementation. Uses custom futures/promises with C++20 coroutines,
libpqxx for PostgreSQL access, and an optional ftxui-based terminal UI.

## Dependencies

Requires Clang 16+ (tested with Clang 20) and a running PostgreSQL instance.

### fmt

Build [fmt](https://github.com/fmtlib/fmt) from source:
```
cd ~/packages
git clone --branch 11.1.4 --depth 1 https://github.com/fmtlib/fmt.git
cd fmt
cmake -B build -DCMAKE_CXX_COMPILER=clang++-20 \
    -DCMAKE_INSTALL_PREFIX=$HOME/packages/fmt/install \
    -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
cmake --install build
```

### spdlog

Build [spdlog](https://github.com/gabime/spdlog) v1.15.3 from source (with external fmt):
```
cd ~/packages
git clone --branch v1.15.3 --depth 1 https://github.com/gabime/spdlog.git
cd spdlog
cmake -B build -DCMAKE_CXX_COMPILER=clang++-20 \
    -DCMAKE_INSTALL_PREFIX=$HOME/packages/spdlog/install \
    -DSPDLOG_FMT_EXTERNAL=ON \
    -DCMAKE_PREFIX_PATH="$HOME/packages/fmt/install" \
    -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
cmake --install build
```

### gflags

Install via apt or build from source:
```
sudo apt install libgflags-dev
```

### libpqxx

Build libpqxx 7.x from source (requires libpq-dev):
```
cd ~/packages
git clone --branch 7.10.0 --depth 1 https://github.com/jtv/libpqxx.git
cd libpqxx
cmake -B build -DCMAKE_CXX_COMPILER=clang++-20 \
    -DCMAKE_INSTALL_PREFIX=$HOME/packages/libpqxx/install \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TEST=OFF -DBUILD_DOC=OFF
cmake --build build -j$(nproc)
cmake --install build
```

### ftxui (optional)

Provides a live terminal UI during benchmark runs. Without it, stats are printed to the console.
```
cd ~/packages
git clone --branch v6.1.9 --depth 1 https://github.com/ArthurSonzogni/FTXUI.git ftxui
cd ftxui
cmake -B build -DCMAKE_CXX_COMPILER=clang++-20 \
    -DCMAKE_INSTALL_PREFIX=$HOME/packages/ftxui/install \
    -DCMAKE_BUILD_TYPE=Release \
    -DFTXUI_BUILD_EXAMPLES=OFF -DFTXUI_BUILD_DOCS=OFF -DFTXUI_BUILD_TESTS=OFF
cmake --build build -j$(nproc)
cmake --install build
```

## Building

```
cmake -B build -DCMAKE_CXX_COMPILER=clang++-20 \
    -DCMAKE_PREFIX_PATH="$HOME/packages/fmt/install;$HOME/packages/spdlog/install;$HOME/packages/libpqxx/install;$HOME/packages/ftxui/install" \
    -DCMAKE_BUILD_TYPE=Release

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

### Simulation mode

Test the coroutine/IO infrastructure without a real database:
```
# Pure sleep simulation (no DB connection needed)
./build/tpcc --command=run --simulate-ms=50 --duration=10 --no-tui

# SELECT 1 simulation (needs a running PostgreSQL)
./build/tpcc --command=run --simulate-select1=5 --duration=10
```
