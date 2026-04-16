#!/usr/bin/env bash
#
# Integration test for the --path option.
# Verifies that TPC-C tables can be placed in a custom PostgreSQL schema.
#
# Prerequisites: same as smoke_test.sh (running PostgreSQL, createdb/dropdb)
#
# Usage:
#   tests/path_test.sh
#   TPCC_BIN=./build/tpcc tests/path_test.sh

set -euo pipefail

TPCC_BIN="${TPCC_BIN:-./build/tpcc}"
TPCC_WAREHOUSES="${TPCC_WAREHOUSES:-1}"
TPCC_DURATION="${TPCC_DURATION:-10}"

DB_NAME="tpcc_path_test_$$"
SCHEMA="tpcc_bench"
CONNECTION="host=${PGHOST:-localhost} port=${PGPORT:-5432} dbname=${DB_NAME} user=${PGUSER:-postgres}"

cleanup() {
    echo "--- Cleaning up ---"
    "${TPCC_BIN}" --command=clean --path="${SCHEMA}" --connection="${CONNECTION}" 2>/dev/null || true
    dropdb --if-exists "${DB_NAME}" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== TPC-C --path Integration Test ==="
echo "Binary:     ${TPCC_BIN}"
echo "Database:   ${DB_NAME}"
echo "Schema:     ${SCHEMA}"
echo "Warehouses: ${TPCC_WAREHOUSES}"
echo "Duration:   ${TPCC_DURATION}s"
echo ""

if [[ ! -x "${TPCC_BIN}" ]]; then
    echo "ERROR: ${TPCC_BIN} not found or not executable" >&2
    exit 1
fi

echo "--- Creating database ---"
createdb "${DB_NAME}"

# Regression guard: place a sentinel table in public with a TPC-C-collision name.
# If the unqualified DROP TABLE / DDL in init or clean ever falls back to public
# (because of a search_path including public), this sentinel will be wiped.
echo "--- Planting sentinel public.customer ---"
psql -q "${DB_NAME}" -c "CREATE TABLE public.customer (sentinel int);"
psql -q "${DB_NAME}" -c "INSERT INTO public.customer VALUES (4242);"

check_sentinel() {
    local stage="$1"
    local row
    row=$(psql -tA "${DB_NAME}" -c \
        "SELECT sentinel FROM public.customer" 2>/dev/null || true)
    if [[ "${row}" != "4242" ]]; then
        echo "FAIL: sentinel public.customer was wiped during ${stage} (got '${row}')" >&2
        exit 1
    fi
}

echo "--- Init with --path=${SCHEMA} ---"
"${TPCC_BIN}" --command=init --path="${SCHEMA}" --connection="${CONNECTION}"
check_sentinel "init"

# Verify tables are in the custom schema, and only the sentinel is in public.
TABLE_COUNT=$(psql -tA "${DB_NAME}" -c \
    "SELECT COUNT(*) FROM information_schema.tables
     WHERE table_schema = '${SCHEMA}' AND table_type = 'BASE TABLE'")
PUBLIC_COUNT=$(psql -tA "${DB_NAME}" -c \
    "SELECT COUNT(*) FROM information_schema.tables
     WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")

echo "Tables in '${SCHEMA}': ${TABLE_COUNT}, in 'public': ${PUBLIC_COUNT}"
if [[ "${TABLE_COUNT}" -lt 9 ]]; then
    echo "FAIL: Expected at least 9 tables in schema '${SCHEMA}', got ${TABLE_COUNT}" >&2
    exit 1
fi
if [[ "${PUBLIC_COUNT}" -ne 1 ]]; then
    echo "FAIL: Expected exactly 1 table in 'public' (the sentinel), got ${PUBLIC_COUNT}" >&2
    exit 1
fi

echo "--- Import with --path=${SCHEMA} ---"
"${TPCC_BIN}" --command=import --warehouses="${TPCC_WAREHOUSES}" --no_tui \
    --path="${SCHEMA}" --connection="${CONNECTION}"
check_sentinel "import"

echo "--- Check after import with --path=${SCHEMA} ---"
"${TPCC_BIN}" --command=check --warehouses="${TPCC_WAREHOUSES}" --after_import \
    --path="${SCHEMA}" --connection="${CONNECTION}"

echo "--- Run benchmark with --path=${SCHEMA} ---"
"${TPCC_BIN}" --command=run \
    --warehouses="${TPCC_WAREHOUSES}" \
    --duration="${TPCC_DURATION}" \
    --warmup=5 \
    --no_tui \
    --path="${SCHEMA}" \
    --connection="${CONNECTION}"
check_sentinel "run"

echo "--- Check after benchmark with --path=${SCHEMA} ---"
"${TPCC_BIN}" --command=check --warehouses="${TPCC_WAREHOUSES}" \
    --path="${SCHEMA}" --connection="${CONNECTION}"

echo "--- Clean with --path=${SCHEMA} ---"
"${TPCC_BIN}" --command=clean --path="${SCHEMA}" --connection="${CONNECTION}"
check_sentinel "clean"

# Verify schema was dropped
SCHEMA_EXISTS=$(psql -tA "${DB_NAME}" -c \
    "SELECT COUNT(*) FROM information_schema.schemata
     WHERE schema_name = '${SCHEMA}'")
if [[ "${SCHEMA_EXISTS}" -gt 0 ]]; then
    echo "FAIL: Schema '${SCHEMA}' still exists after clean" >&2
    exit 1
fi

echo ""
echo "=== --path integration test PASSED ==="
