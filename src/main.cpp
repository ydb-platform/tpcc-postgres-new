#include "init.h"
#include "import.h"
#include "runner.h"
#include "clean.h"
#include "check.h"
#include "path_checker.h"
#include "log.h"
#include "util.h"

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <string>

DEFINE_string(connection, "host=localhost dbname=tpcc user=postgres", "PostgreSQL connection string");
DEFINE_string(path, "", "PostgreSQL schema for benchmark tables (default: public)");
DEFINE_string(command, "run", "Command to execute: init, import, run, clean, check");

DEFINE_int32(warehouses, 1, "Number of warehouses");
DEFINE_int32(warmup, 0, "Warmup duration in seconds");
DEFINE_int32(duration, 600, "Benchmark run duration in seconds");
DEFINE_int32(threads, 0, "Number of coroutine threads (0 = auto)");
DEFINE_int32(max_inflight, 0, "Max inflight transactions (0 = auto)");
DEFINE_int32(io_threads, 4, "Number of I/O threads for libpqxx");
DEFINE_int32(load_threads, 0, "Number of threads for data import (0 = auto)");
DEFINE_bool(no_delays, false, "Disable keying and think time delays");
DEFINE_bool(high_res_histogram, false, "Use high resolution histograms");
DEFINE_int32(simulate_ms, 0, "Simulation mode: sleep N ms per transaction instead of real TPC-C (0 = disabled)");
DEFINE_int32(simulate_select1, 0, "Simulation mode: run N SELECT 1 queries per transaction instead of real TPC-C (0 = disabled)");
DEFINE_string(log_level, "info", "Log level: trace, debug, info, warn, error");
DEFINE_bool(no_tui, false, "Disable terminal UI");
DEFINE_bool(after_import, false, "Check mode: verify freshly loaded data (stricter invariants)");

namespace {

void PrintHelp() {
    std::cout <<
        "tpcc - TPC-C benchmark for PostgreSQL\n"
        "\n"
        "Usage: tpcc [options]\n"
        "\n"
        "Commands (--command):\n"
        "  init      Create TPC-C schema (tables, indexes)\n"
        "  import    Load TPC-C data into the database\n"
        "  run       Run the TPC-C benchmark (default)\n"
        "  clean     Drop all TPC-C tables\n"
        "  check     Run TPC-C consistency checks\n"
        "\n"
        "Options:\n"
        "  --connection    PostgreSQL connection string\n"
        "                  (default: \"host=localhost dbname=tpcc user=postgres\")\n"
        "  --path          PostgreSQL schema for benchmark tables (default: public)\n"
        "  --command       Command to execute (default: \"run\")\n"
        "  --warehouses    Number of warehouses (default: 1)\n"
        "  --warmup        Warmup duration in seconds (default: 0)\n"
        "  --duration      Benchmark run duration in seconds (default: 600)\n"
        "  --threads       Number of coroutine threads, 0 = auto (default: 0)\n"
        "  --max-inflight  Max inflight transactions, 0 = auto (default: 0)\n"
        "  --io-threads    Number of I/O threads for libpqxx (default: 4)\n"
        "  --load-threads  Number of threads for data import, 0 = auto (default: 0)\n"
        "  --no-delays     Disable keying and think time delays (default: false)\n"
        "  --high-res-histogram  Use high resolution histograms (default: false)\n"
        "  --log-level     Log level: trace, debug, info, warn, error (default: \"info\")\n"
        "  --no-tui        Disable terminal UI (default: false)\n"
        "  --after-import  Check: verify freshly loaded data (default: false)\n"
        "\n"
        "Simulation (for testing without real TPC-C transactions):\n"
        "  --simulate-ms       Sleep N ms per transaction (default: 0 = disabled)\n"
        "  --simulate-select1  Run N SELECT 1 queries per transaction (default: 0 = disabled)\n"
        "\n"
        "Examples:\n"
        "  tpcc --command=init --connection=\"host=localhost dbname=tpcc\"\n"
        "  tpcc --command=import --warehouses=10 --load-threads=8\n"
        "  tpcc --command=run --warehouses=10 --duration=300 --threads=4\n"
        "  tpcc --command=check --warehouses=10\n"
        "  tpcc --command=check --warehouses=10 --after-import\n";
}

spdlog::level::level_enum ParseLogLevel(const std::string& level) {
    if (level == "trace") return spdlog::level::trace;
    if (level == "debug") return spdlog::level::debug;
    if (level == "info") return spdlog::level::info;
    if (level == "warn" || level == "warning") return spdlog::level::warn;
    if (level == "error" || level == "err") return spdlog::level::err;
    return spdlog::level::info;
}

void RunInit() {
    NTPCC::CheckDbForInit(FLAGS_connection, FLAGS_path);
    NTPCC::InitSync(FLAGS_connection, FLAGS_path);
}

void RunImport() {
    NTPCC::CheckDbForImport(FLAGS_connection, FLAGS_path);
    NTPCC::TImportConfig config;
    config.ConnectionString = FLAGS_connection;
    config.Path = FLAGS_path;
    config.WarehouseCount = FLAGS_warehouses;
    config.LoadThreadCount = FLAGS_load_threads;
    config.UseTui = !FLAGS_no_tui;
    NTPCC::ImportSync(config);
}

void RunBenchmark() {
    NTPCC::TRunConfig config;
    config.ConnectionString = FLAGS_connection;
    config.Path = FLAGS_path;
    config.WarehouseCount = FLAGS_warehouses;
    config.WarmupDuration = std::chrono::seconds(FLAGS_warmup);
    config.RunDuration = std::chrono::seconds(FLAGS_duration);
    config.ThreadCount = FLAGS_threads;
    config.MaxInflight = FLAGS_max_inflight;
    config.IOThreads = FLAGS_io_threads;
    config.NoDelays = FLAGS_no_delays;
    config.HighResHistogram = FLAGS_high_res_histogram;
    config.SimulateTransactionMs = FLAGS_simulate_ms;
    config.SimulateTransactionSelect1 = FLAGS_simulate_select1;
    config.UseTui = !FLAGS_no_tui;

    if (!config.IsSimulationMode()) {
        NTPCC::CheckDbForRun(FLAGS_connection, FLAGS_warehouses, FLAGS_path);
    }

    NTPCC::RunSync(config);
}

void RunClean() {
    NTPCC::CleanSync(FLAGS_connection, FLAGS_path);
}

void RunCheck() {
    NTPCC::CheckDbForRun(FLAGS_connection, FLAGS_warehouses, FLAGS_path);
    NTPCC::CheckSync(FLAGS_connection, FLAGS_warehouses, FLAGS_after_import, FLAGS_path);
}

} // anonymous

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--help" || arg == "-help" || arg == "--helpshort" || arg == "-h") {
            PrintHelp();
            return 0;
        }
    }

    gflags::SetUsageMessage("TPC-C benchmark for PostgreSQL");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    NTPCC::InitLogging();
    spdlog::set_level(ParseLogLevel(FLAGS_log_level));

    try {
        if (FLAGS_command == "init") {
            LOG_I("Initializing TPC-C schema...");
            RunInit();
            LOG_I("Schema initialization complete");
        } else if (FLAGS_command == "import") {
            LOG_I("Importing TPC-C data ({} warehouses)...", FLAGS_warehouses);
            RunImport();
            LOG_I("Data import complete");
        } else if (FLAGS_command == "run") {
            LOG_I("Running TPC-C benchmark...");
            RunBenchmark();
        } else if (FLAGS_command == "clean") {
            LOG_I("Cleaning TPC-C tables...");
            RunClean();
            LOG_I("Clean complete");
        } else if (FLAGS_command == "check") {
            LOG_I("Running TPC-C consistency checks...");
            RunCheck();
            LOG_I("Consistency checks complete");
        } else {
            std::cerr << "Unknown command: " << FLAGS_command << "\n";
            std::cerr << "Valid commands: init, import, run, clean, check\n";
            return 1;
        }
    } catch (const std::exception& ex) {
        LOG_E("Fatal error: {}", ex.what());
        return 1;
    }

    return NTPCC::GetGlobalErrorVariable().load() ? 1 : 0;
}
