#!/usr/bin/env bash
#
# Multi-warehouse stress test for TPC-C.
# Runs a longer benchmark with more warehouses and verifies consistency.
#
# Prerequisites: same as smoke_test.sh
#
# Usage:
#   tests/stress_test.sh
#   TPCC_BIN=./build-asan/tpcc tests/stress_test.sh
#
# Environment variables:
#   TPCC_BIN        Path to tpcc binary         (default: ./build/tpcc)
#   TPCC_WAREHOUSES Number of warehouses         (default: 5)
#   TPCC_DURATION   Benchmark duration, minutes  (default: 1)

set -euo pipefail

TPCC_BIN="${TPCC_BIN:-./build/tpcc}"
TPCC_WAREHOUSES="${TPCC_WAREHOUSES:-5}"
TPCC_DURATION="${TPCC_DURATION:-1}"

DB_NAME="tpcc_stress_$$"
CONNECTION="host=${PGHOST:-localhost} port=${PGPORT:-5432} dbname=${DB_NAME} user=${PGUSER:-postgres}"

cleanup() {
    echo "--- Cleaning up ---"
    "${TPCC_BIN}" clean --connection="${CONNECTION}" 2>/dev/null || true
    dropdb --if-exists "${DB_NAME}" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== TPC-C Stress Test ==="
echo "Binary:     ${TPCC_BIN}"
echo "Database:   ${DB_NAME}"
echo "Warehouses: ${TPCC_WAREHOUSES}"
echo "Duration:   ${TPCC_DURATION} min"
echo ""

if [[ ! -x "${TPCC_BIN}" ]]; then
    echo "ERROR: ${TPCC_BIN} not found or not executable" >&2
    exit 1
fi

echo "--- Creating database ---"
createdb "${DB_NAME}"

echo "--- Initializing schema ---"
"${TPCC_BIN}" init --connection="${CONNECTION}"

echo "--- Importing data (${TPCC_WAREHOUSES} warehouses) ---"
"${TPCC_BIN}" import --warehouses="${TPCC_WAREHOUSES}" --connection="${CONNECTION}"

echo "--- Checking after import ---"
"${TPCC_BIN}" check --warehouses="${TPCC_WAREHOUSES}" --after_import --connection="${CONNECTION}"

echo "--- Running benchmark (${TPCC_DURATION} min, no delays, no TUI) ---"
"${TPCC_BIN}" run \
    --warehouses="${TPCC_WAREHOUSES}" \
    --duration="${TPCC_DURATION}" \
    --no_delays \
    --no_tui \
    --connection="${CONNECTION}"

echo "--- Checking after benchmark ---"
"${TPCC_BIN}" check --warehouses="${TPCC_WAREHOUSES}" --connection="${CONNECTION}"

echo ""
echo "=== Stress test PASSED ==="
