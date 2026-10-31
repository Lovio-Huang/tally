#!/bin/sh
# Creates the login role the API connects as (README, "Database roles"): a member of
# tally_app and nothing else, with its password from TALLY_APP_DB_PASSWORD. Postgres runs
# this once, when the data volume is first created. For a volume created before roles
# were split, run it by hand (it is safe to rerun, and resets the password):
#   docker compose exec postgres sh /docker-entrypoint-initdb.d/10-runtime-role.sh
set -eu
: "${TALLY_APP_DB_PASSWORD:?set TALLY_APP_DB_PASSWORD}"

psql -v ON_ERROR_STOP=1 -v password="$TALLY_APP_DB_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
-- Migration 012 creates tally_app too; creating it here lets the login role join it now.
SELECT 'CREATE ROLE tally_app NOLOGIN'
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tally_app') \gexec
SELECT 'CREATE ROLE tally_api LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS IN ROLE tally_app'
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tally_api') \gexec
SELECT format('ALTER ROLE tally_api PASSWORD %L', :'password') \gexec
SQL
