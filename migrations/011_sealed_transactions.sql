-- A committed transaction is sealed: nothing can add entries to it afterwards.
--
-- The deferred check only asks whether a transaction's entries balance, so a balanced
-- pair appended to last month's transaction passed it: history changed and the books
-- still reconciled. Now each transaction records the database transaction that created
-- it (an xid8: 64 bits, never wraps around), and an entry is accepted only from that
-- same database transaction. pg_current_xact_id() returns the top-level id even inside
-- a savepoint, so settlement's per-group savepoints post as before.
--
-- Rows that already exist get NULL: which database transaction created them isn't
-- known, and stamping them with this migration's id would invent history. NULL never
-- matches a running transaction, so they are sealed like the rest.
ALTER TABLE transactions ADD COLUMN created_xid xid8;

-- Stamped by the database, whatever the INSERT says, so it can't be set to another
-- session's id. Changing it later is refused like any other edit (append-only trigger).
CREATE FUNCTION stamp_transaction_xid() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.created_xid := pg_current_xact_id();
    RETURN NEW;
END $$;

CREATE TRIGGER transactions_stamp_xid
    BEFORE INSERT ON transactions
    FOR EACH ROW EXECUTE FUNCTION stamp_transaction_xid();

-- The entry trigger already looks up its transaction to copy the client (006); the seal
-- check reuses that lookup. A missing transaction is left to the foreign key to report.
CREATE OR REPLACE FUNCTION stamp_entry_client() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    created xid8;
BEGIN
    SELECT client_id, created_xid INTO NEW.client_id, created FROM transactions WHERE id = NEW.transaction_id;
    IF FOUND AND created IS DISTINCT FROM pg_current_xact_id() THEN
        RAISE EXCEPTION 'transaction % is sealed: entries can only be added by the database transaction that created it',
            NEW.transaction_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    RETURN NEW;
END $$;
