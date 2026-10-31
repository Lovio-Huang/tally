-- What happens to a payment (a hold with settle_to_account_id) between being accepted
-- and being settled.
--
-- Until now a payment carried an expiry like an ordinary hold, and settlement skipped
-- expired ones. A payment accepted with a short expiry, or any payment while the
-- maintenance loop was stopped for longer than its expiry, could expire unsettled: the
-- API had answered 201, the payer got the money back, the merchant was never paid.
-- Now an accepted payment waits for settlement however long that takes: it has no
-- expiry, the expiry job leaves it alone, and it ends only by being settled (captured),
-- by settlement refusing it (voided, with void_reason) or by being cancelled (voided).

ALTER TABLE holds ALTER COLUMN expires_at DROP NOT NULL;

-- Why a hold was voided by the system rather than by its owner, e.g. a payment that
-- settlement couldn't post. NULL for holds voided through the API.
ALTER TABLE holds ADD COLUMN void_reason TEXT CHECK (length(void_reason) <= 500);

-- Payments still waiting: stop their clocks. (Updating an active hold's expiry is
-- allowed; the lifecycle trigger only protects account, amount, destination and
-- creation time.)
UPDATE holds SET expires_at = NULL WHERE status = 'active' AND settle_to_account_id IS NOT NULL;

-- From now on an ordinary hold has an expiry and a payment has none. NOT VALID: payments
-- that already ended (captured, voided or expired) keep the expiry they were created
-- with, because settled holds can't change and their history shouldn't be rewritten.
ALTER TABLE holds ADD CONSTRAINT holds_expiry_matches_kind
    CHECK ((settle_to_account_id IS NULL) = (expires_at IS NOT NULL)) NOT VALID;
