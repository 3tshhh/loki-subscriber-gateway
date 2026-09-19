#!/bin/sh
set -e

echo "Checking database \"$DATABASE_NAME\" exists..."
DB_EXISTS=$(PGPASSWORD="$DATABASE_PASSWORD" psql -h "$DATABASE_HOST" -p "$DATABASE_PORT" -U "$DATABASE_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$DATABASE_NAME'")

if [ "$DB_EXISTS" != "1" ]; then
  echo "Database \"$DATABASE_NAME\" does not exist, creating it..."
  PGPASSWORD="$DATABASE_PASSWORD" createdb -h "$DATABASE_HOST" -p "$DATABASE_PORT" -U "$DATABASE_USER" "$DATABASE_NAME"
fi

echo "Running migrations..."
node ./node_modules/typeorm/cli.js -d dist/shared/database/data-source.js migration:run

echo "Starting server..."
exec node dist/main
