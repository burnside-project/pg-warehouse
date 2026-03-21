#!/usr/bin/env bash
# =============================================================================
# run-pipeline.sh — E-Commerce Recipe Pipeline Orchestrator
#
# Runs the medallion pipeline: refresh raw → silver transforms → feat exports.
# In multi-DuckDB mode, CDC keeps running — no stop/restart needed.
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
OUTPUT_DIR="${PROJECT_DIR}/out"

# Defaults
RUN_REFRESH=true
RUN_SILVER=true
RUN_FEAT=true
RUN_PROMOTE=false
PREVIEW_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --silver-only)  RUN_SILVER=true; RUN_FEAT=false; RUN_REFRESH=true; shift ;;
        --feat-only)    RUN_SILVER=false; RUN_FEAT=true; RUN_REFRESH=false; shift ;;
        --preview)      PREVIEW_ONLY=true; RUN_SILVER=false; RUN_FEAT=true; RUN_REFRESH=false; shift ;;
        --promote)      RUN_PROMOTE=true; shift ;;
        --no-refresh)   RUN_REFRESH=false; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Helpers ────────────────────────────────────────────────────────────────

log() { echo "[ecommerce-pipeline] $(date '+%H:%M:%S') $*"; }

# ─── Main ───────────────────────────────────────────────────────────────────

log "Starting e-commerce pipeline..."
mkdir -p "$OUTPUT_DIR"

# ─── Refresh ────────────────────────────────────────────────────────────────

if [[ "$RUN_REFRESH" == "true" ]]; then
    log "=== Refresh: snapshotting raw.duckdb → silver.duckdb v0 ==="
    "$BINARY" run --config "$CONFIG" --refresh
fi

# ─── Silver Layer ───────────────────────────────────────────────────────────

if [[ "$RUN_SILVER" == "true" ]]; then
    log "=== Silver Layer: sql/silver/v1/ → v1.* ==="
    "$BINARY" run --config "$CONFIG" --sql-dir "$RECIPE_DIR/sql/silver/v1/"
    log "Silver layer complete."
fi

# ─── Feat Layer ─────────────────────────────────────────────────────────────

if [[ "$RUN_FEAT" == "true" ]]; then
    if [[ "$PREVIEW_ONLY" == "true" ]]; then
        log "=== Feat Layer: preview mode ==="
        for sql_file in "$RECIPE_DIR"/sql/feat/*.sql; do
            [[ -f "$sql_file" ]] || continue
            log "Preview: $(basename "$sql_file")"
            "$BINARY" preview \
                --config "$CONFIG" \
                --sql-file "$sql_file" \
                --limit 10
        done
    else
        log "=== Feat Layer: sql/feat/ → v1.* + Parquet export ==="
        "$BINARY" run --config "$CONFIG" --sql-dir "$RECIPE_DIR/sql/feat/"
    fi
    log "Feat layer complete."
fi

# ─── Promote ────────────────────────────────────────────────────────────────

if [[ "$RUN_PROMOTE" == "true" ]]; then
    log "=== Promote: v1 → current ==="
    "$BINARY" run --config "$CONFIG" --promote --version 1
fi

# ─── Done ───────────────────────────────────────────────────────────────────

if [[ "$PREVIEW_ONLY" == "true" ]]; then
    log "Preview complete."
else
    log "Pipeline complete. Parquet files in: $OUTPUT_DIR/"
    ls -lh "$OUTPUT_DIR"/*.parquet 2>/dev/null || true
fi
