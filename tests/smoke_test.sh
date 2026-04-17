#!/usr/bin/env bash
#
# End-to-end smoke test for TPC-C benchmark against a real PostgreSQL instance.
#
# Prerequisites:
#   - PostgreSQL is running and accessible (createdb/dropdb work)
#   - The tpcc binary has been built (default: ./build/tpcc)
#
# Usage:
#   tests/smoke_test.sh                      # use defaults
#   TPCC_BIN=./build-debug/tpcc tests/smoke_test.sh
#   PGHOST=myhost PGUSER=bench tests/smoke_test.sh
#
# Environment variables:
#   TPCC_BIN        Path to tpcc binary         (default: ./build/tpcc)
#   TPCC_WAREHOUSES Number of warehouses         (default: 1)
#   TPCC_DURATION   Benchmark duration, minutes  (default: 2)
#   PGHOST/PGUSER/PGPORT  Standard libpq env vars for createdb/dropdb

set -euo pipefail

TPCC_BIN="${TPCC_BIN:-./build/tpcc}"
TPCC_WAREHOUSES="${TPCC_WAREHOUSES:-10}"
TPCC_DURATION="${TPCC_DURATION:-2}"

DB_NAME="tpcc_smoke_$$"
CONNECTION="host=${PGHOST:-localhost} port=${PGPORT:-5432} dbname=${DB_NAME} user=${PGUSER:-postgres}"

cleanup() {
    echo "--- Cleaning up ---"
    "${TPCC_BIN}" clean --connection="${CONNECTION}" 2>/dev/null || true
    dropdb --if-exists "${DB_NAME}" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== TPC-C Smoke Test ==="
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

echo "--- Importing data (${TPCC_WAREHOUSES} warehouse(s)) ---"
"${TPCC_BIN}" import --warehouses="${TPCC_WAREHOUSES}" --no_tui --connection="${CONNECTION}"

echo "--- Checking after import ---"
"${TPCC_BIN}" check --warehouses="${TPCC_WAREHOUSES}" --after_import --connection="${CONNECTION}"

echo "--- Running benchmark (${TPCC_DURATION} min, no TUI) ---"
"${TPCC_BIN}" run \
    --warehouses="${TPCC_WAREHOUSES}" \
    --duration="${TPCC_DURATION}" \
    --no_tui \
    --connection="${CONNECTION}"

echo "--- Checking after benchmark ---"
"${TPCC_BIN}" check --warehouses="${TPCC_WAREHOUSES}" --connection="${CONNECTION}"

echo ""
echo "=== Smoke test PASSED ==="
