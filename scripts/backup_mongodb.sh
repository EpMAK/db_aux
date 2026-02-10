#!/usr/bin/env sh
set -eu

: "${MONGO_URI:=mongodb://admin:admin@localhost:27017/?authSource=admin}"
: "${MONGO_DB:=training_db}"
: "${BACKUP_DIR:=./backups/mongodb}"

if ! command -v mongodump >/dev/null 2>&1; then
  echo "Error: mongodump is not installed or not in PATH." >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUT_FILE="$BACKUP_DIR/${MONGO_DB}_${TIMESTAMP}.archive.gz"

mongodump \
  --uri="$MONGO_URI" \
  --db="$MONGO_DB" \
  --archive="$OUT_FILE" \
  --gzip

echo "MongoDB backup created: $OUT_FILE"
