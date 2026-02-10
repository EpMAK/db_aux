#!/usr/bin/env sh
set -eu

: "${PGHOST:=localhost}"
: "${PGPORT:=5432}"
: "${PGUSER:=app_user}"
: "${PGDATABASE:=app_db}"
: "${BACKUP_DIR:=./backups/postgres}"

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "Error: pg_dump is not installed or not in PATH." >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUT_FILE="$BACKUP_DIR/${PGDATABASE}_${TIMESTAMP}.sql.gz"

export PGPASSWORD="${PGPASSWORD:-}"
pg_dump \
  --host="$PGHOST" \
  --port="$PGPORT" \
  --username="$PGUSER" \
  --dbname="$PGDATABASE" \
  --format=plain \
  --no-owner \
  --no-privileges \
  | gzip > "$OUT_FILE"

echo "PostgreSQL backup created: $OUT_FILE"
