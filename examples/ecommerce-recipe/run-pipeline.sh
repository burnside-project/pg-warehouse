#!/usr/bin/env bash
# =============================================================================
# run-pipeline.sh — E-Commerce Recipe Pipeline Orchestrator
#
# Runs silver → feat SQL pipelines against the local DuckDB warehouse.
# Stops CDC before running (DuckDB single-writer lock), restarts after.
#
# Usage:
#   ./examples/ecommerce-recipe/run-pipeline.sh                  # full pipeline
#   ./examples/ecommerce-recipe/run-pipeline.sh --silver-only    # silver layer only
#   ./examples/ecommerce-recipe/run-pipeline.sh --feat-only      # feat layer only
#   ./examples/ecommerce-recipe/run-pipeline.sh --preview        # preview feat (10 rows)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RECIPE_DIR="$SCRIPT_DIR"
CONFIG="${CONFIG:-${PROJECT_DIR}/pg-warehouse.yml}"
BINARY="${PROJECT_DIR}/pg-warehouse"
STATE_DB="${PROJECT_DIR}/.pgwh/state.db"
OUTPUT_DIR="${PROJECT_DIR}/out"

# Defaults
RUN_SILVER=true
RUN_FEAT=true
PREVIEW_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --silver-only) RUN_SILVER=true; RUN_FEAT=false; shift ;;
        --feat-only)   RUN_SILVER=false; RUN_FEAT=true; shift ;;
        --preview)     PREVIEW_ONLY=true; RUN_SILVER=false; RUN_FEAT=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Helpers ────────────────────────────────────────────────────────────────

CDC_PID=""

log() { echo "[ecommerce-pipeline] $(date '+%H:%M:%S') $*"; }

find_cdc_pid() {
    CDC_PID=$(pgrep -f 'pg-warehouse cdc' 2>/dev/null || true)
}

stop_cdc() {
    find_cdc_pid
    if [[ -n "$CDC_PID" ]]; then
        log "Stopping CDC (PID $CDC_PID)..."
        kill -SIGINT "$CDC_PID" 2>/dev/null || true
        for i in $(seq 1 15); do
            if ! kill -0 "$CDC_PID" 2>/dev/null; then
                log "CDC stopped."
                break
            fi
            sleep 1
        done
        if kill -0 "$CDC_PID" 2>/dev/null; then
            log "CDC did not stop gracefully, sending SIGKILL..."
            kill -9 "$CDC_PID" 2>/dev/null || true
        fi
    else
        log "CDC is not running."
    fi

    # Clear stale lock
    if [[ -f "$STATE_DB" ]]; then
        sqlite3 "$STATE_DB" 'DELETE FROM lock_state;' 2>/dev/null || true
    fi
}

restart_cdc() {
    log "Restarting CDC..."
    nohup "$BINARY" cdc start --config "$CONFIG" > "${PROJECT_DIR}/cdc.log" 2>&1 &
    CDC_PID=$!
    log "CDC restarted (PID $CDC_PID). Log: cdc.log"
}

# Trap: always restart CDC on exit
cleanup() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        log "Pipeline FAILED (exit code $exit_code)."
    fi
    find_cdc_pid
    if [[ -z "$CDC_PID" ]]; then
        restart_cdc
    fi
    exit $exit_code
}
trap cleanup EXIT

# Derive target table from SQL filepath
target_table_from_path() {
    local filepath="$1"
    local filename
    filename=$(basename "$filepath" .sql)
    local schema
    schema=$(basename "$(dirname "$filepath")")
    local table_name
    table_name=$(echo "$filename" | sed 's/^[0-9]*_//')
    echo "${schema}.${table_name}"
}

# ─── Main ───────────────────────────────────────────────────────────────────

log "Starting e-commerce pipeline..."
mkdir -p "$OUTPUT_DIR"

stop_cdc

# ─── Silver Layer ───────────────────────────────────────────────────────────

if [[ "$RUN_SILVER" == "true" ]]; then
    log "=== Silver Layer ==="
    for sql_file in "$RECIPE_DIR"/sql/silver/*.sql; do
        [[ -f "$sql_file" ]] || continue
        target=$(target_table_from_path "$sql_file")
        log "Running: $target ($(basename "$sql_file"))"
        "$BINARY" run \
            --config "$CONFIG" \
            --sql-file "$sql_file" \
            --target-table "$target"
    done
    log "Silver layer complete."
fi

# ─── Feat Layer ─────────────────────────────────────────────────────────────

if [[ "$RUN_FEAT" == "true" ]]; then
    log "=== Feat Layer ==="
    for sql_file in "$RECIPE_DIR"/sql/feat/*.sql; do
        [[ -f "$sql_file" ]] || continue
        target=$(target_table_from_path "$sql_file")
        table_name=$(echo "$target" | cut -d. -f2)

        if [[ "$PREVIEW_ONLY" == "true" ]]; then
            log "Preview: $target ($(basename "$sql_file"))"
            "$BINARY" preview \
                --config "$CONFIG" \
                --sql-file "$sql_file" \
                --limit 10
        else
            log "Running: $target ($(basename "$sql_file"))"
            "$BINARY" run \
                --config "$CONFIG" \
                --sql-file "$sql_file" \
                --target-table "$target" \
                --output "${OUTPUT_DIR}/${table_name}.parquet" \
                --file-type parquet
        fi
    done
    log "Feat layer complete."
fi

# ─── Done ───────────────────────────────────────────────────────────────────

if [[ "$PREVIEW_ONLY" == "true" ]]; then
    log "Preview complete."
else
    log "Pipeline complete. Parquet files in: $OUTPUT_DIR/"
    ls -lh "$OUTPUT_DIR"/*.parquet 2>/dev/null || true
fi
