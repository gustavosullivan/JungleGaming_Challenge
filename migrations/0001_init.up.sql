CREATE TABLE wallets (
    id uuid PRIMARY KEY,
    player_id uuid NOT NULL,
    currency varchar(3) NOT NULL,
    balance_minor bigint NOT NULL,
    version bigint NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency UNIQUE (player_id, currency),
    CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    CONSTRAINT wallets_version_positive CHECK (version >= 1),
    CONSTRAINT wallets_currency_iso CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE wager_transactions (
    id uuid PRIMARY KEY,
    origin text NOT NULL,
    kind text NOT NULL,
    status text NOT NULL,
    wallet_id uuid NOT NULL REFERENCES wallets (id),
    player_id uuid NOT NULL,
    currency varchar(3) NOT NULL,
    amount_minor bigint NOT NULL,
    provider_id varchar(64),
    external_transaction_id varchar(128),
    idempotency_key varchar(255),
    payload_hash char(64),
    round_id varchar(128),
    game_id varchar(128),
    reference_external_transaction_id varchar(128),
    reference_transaction_id uuid REFERENCES wager_transactions (id),
    failure_code varchar(64),
    result_balance_minor bigint,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT wager_origin CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    CONSTRAINT wager_kind CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    CONSTRAINT wager_status CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    CONSTRAINT wager_amount_non_negative CHECK (amount_minor >= 0),
    CONSTRAINT wager_amount_by_kind CHECK (
        status <> 'PROCESSED'
        OR (kind = 'LOSS' AND amount_minor = 0)
        OR (kind = 'OPENING' AND amount_minor >= 0)
        OR (kind IN ('BET', 'WIN', 'REFUND', 'ROLLBACK') AND amount_minor > 0)
    ),
    CONSTRAINT wager_shape CHECK (
        (
            origin = 'INTERNAL'
            AND kind = 'OPENING'
            AND provider_id IS NULL
            AND external_transaction_id IS NULL
            AND idempotency_key IS NULL
            AND payload_hash IS NULL
            AND round_id IS NULL
            AND game_id IS NULL
            AND reference_external_transaction_id IS NULL
        )
        OR (
            origin = 'EXTERNAL'
            AND kind <> 'OPENING'
            AND provider_id IS NOT NULL
            AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL
            AND game_id IS NOT NULL
        )
    ),
    CONSTRAINT wager_reference_shape CHECK (
        CASE
            WHEN kind IN ('REFUND', 'ROLLBACK') THEN reference_external_transaction_id IS NOT NULL
            WHEN kind = 'WIN' THEN TRUE
            ELSE reference_external_transaction_id IS NULL
        END
    ),
    CONSTRAINT wager_failure_shape CHECK (
        (status IN ('REJECTED', 'FAILED') AND failure_code IS NOT NULL)
        OR (status NOT IN ('REJECTED', 'FAILED') AND failure_code IS NULL)
    ),
    CONSTRAINT wager_result_shape CHECK (
        (status IN ('PROCESSED', 'REJECTED') AND result_balance_minor IS NOT NULL)
        OR (status NOT IN ('PROCESSED', 'REJECTED') AND result_balance_minor IS NULL)
    ),
    CONSTRAINT wager_result_non_negative CHECK (result_balance_minor IS NULL OR result_balance_minor >= 0)
);

CREATE UNIQUE INDEX ux_wager_external
    ON wager_transactions (provider_id, external_transaction_id)
    WHERE origin = 'EXTERNAL';

CREATE UNIQUE INDEX ux_wager_idempotency
    ON wager_transactions (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX ux_wager_opening
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

CREATE UNIQUE INDEX ux_wager_one_reversal
    ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK')
      AND status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED');

CREATE INDEX ix_wager_wallet ON wager_transactions (wallet_id);

CREATE TABLE wallet_ledger_entries (
    id uuid PRIMARY KEY,
    wallet_id uuid NOT NULL REFERENCES wallets (id),
    transaction_id uuid NOT NULL REFERENCES wager_transactions (id),
    direction text NOT NULL,
    amount_minor bigint NOT NULL,
    currency varchar(3) NOT NULL,
    balance_before_minor bigint NOT NULL,
    balance_after_minor bigint NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT ledger_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT ledger_before_non_negative CHECK (balance_before_minor >= 0),
    CONSTRAINT ledger_after_non_negative CHECK (balance_after_minor >= 0),
    CONSTRAINT ledger_equation CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    ),
    CONSTRAINT ux_ledger_wallet_tx UNIQUE (wallet_id, transaction_id)
);

CREATE INDEX ix_ledger_wallet_order
    ON wallet_ledger_entries (wallet_id, created_at, id);

CREATE TABLE inbox (
    consumer_name varchar(64) NOT NULL,
    message_id varchar(128) NOT NULL,
    payload_hash char(64) NOT NULL,
    received_at timestamptz NOT NULL,
    completed_at timestamptz,
    CONSTRAINT inbox_pkey PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox (
    event_id uuid PRIMARY KEY,
    aggregate_id varchar(64) NOT NULL,
    event_type varchar(64) NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    published_at timestamptz,
    locked_by varchar(128),
    locked_until timestamptz,
    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0)
);

CREATE INDEX ix_outbox_pending
    ON outbox (next_attempt_at)
    WHERE published_at IS NULL;

CREATE TABLE reference_waits (
    transaction_id uuid PRIMARY KEY REFERENCES wager_transactions (id),
    attempts integer NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    locked_until timestamptz,
    CONSTRAINT reference_waits_attempts_non_negative CHECK (attempts >= 0)
);

CREATE FUNCTION reject_ledger_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger entries are immutable';
END;
$$;

CREATE TRIGGER ledger_no_update
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();

CREATE FUNCTION reject_terminal_wager_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'terminal wager transaction is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_terminal_immutable
    BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION reject_terminal_wager_update();
