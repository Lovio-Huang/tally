-- The API connects as a role that can run the ledger and nothing more.
--
-- Until now the API connected as the role that owns the schema. An owner can disable
-- triggers, drop constraints and rewrite history, so every rule the database enforces
-- held only as long as the API had no bug and nobody got into it. Now the owner only
-- migrates and runs the admin CLI; the API and its maintenance jobs log in as a role
-- that is a member of tally_app, which
--   * owns nothing, can't create anything in the schema, isn't a superuser: it can't
--     ALTER TABLE ... DISABLE TRIGGER, drop a constraint, or switch on replica mode;
--   * has exactly the table privileges the code uses, column by column for updates;
--   * can't write the audit log or the outbox itself: rows reach them through
--     SECURITY DEFINER trigger functions, and old events through one purge function.
-- The login role and its password are created outside migrations, because credentials
-- don't belong in version control (README, "Database roles").
--
-- Still up to the API, not the database: which client's rows a request sees (this role
-- reads and writes every client's books), and which key an audit row names (it comes
-- from session settings the API sets). The owner, and any superuser, stay inside the
-- trust boundary: they can still do all of the above.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tally_app') THEN
        CREATE ROLE tally_app NOLOGIN;
    END IF;
END $$;

-- The default since PostgreSQL 15; explicit for databases created before it.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO tally_app;

GRANT SELECT, INSERT ON accounts, transactions, entries, holds TO tally_app;
-- Row locks (SELECT ... FOR UPDATE / FOR NO KEY UPDATE) need UPDATE on some column, and
-- the balance triggers (005) write balance and held as the caller. A direct UPDATE of
-- either is still refused by protect_derived_balances, and of a transaction by the
-- append-only trigger.
GRANT UPDATE (balance, held) ON accounts TO tally_app;
GRANT UPDATE (description) ON transactions TO tally_app;
GRANT UPDATE (status, captured_transaction_id, void_reason) ON holds TO tally_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON idempotency_keys TO tally_app;
-- Authentication reads keys; creating clients and keys is the admin CLI's job.
GRANT SELECT ON api_clients, api_keys TO tally_app;
GRANT SELECT ON outbox_events TO tally_app;
-- So the API can check at startup that the schema is up to date (it can't migrate).
GRANT SELECT ON schema_migrations TO tally_app;

-- Audit rows and events are written as the owner. pg_temp goes last so a temporary
-- table can't stand in for a real one.
ALTER FUNCTION audit_change() SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION emit_transaction_event() SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION emit_row_event() SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp;

-- The only way the API removes events: those past the 7-day retention, in batches.
CREATE FUNCTION purge_outbox_events(batch_size integer) RETURNS integer
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    WITH purged AS (
        DELETE FROM outbox_events
         WHERE id IN (SELECT id FROM outbox_events
                       WHERE created_at < now() - interval '7 days'
                       ORDER BY created_at
                       LIMIT batch_size
                       FOR UPDATE SKIP LOCKED)
        RETURNING 1
    )
    SELECT count(*)::integer FROM purged
$$;

REVOKE ALL ON FUNCTION purge_outbox_events(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION purge_outbox_events(integer) TO tally_app;
