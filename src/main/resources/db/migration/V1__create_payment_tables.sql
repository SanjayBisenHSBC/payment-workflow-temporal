-- ============================================================
-- V1__create_payment_tables.sql
-- Initial schema for the Payment Workflow Service
-- ============================================================

-- ── payment_records ──────────────────────────────────────────────────────────
-- Stores the top-level record for every payment submitted.
-- One row per payment. Updated at each workflow exit point.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payment_records (
    id                      BIGSERIAL       PRIMARY KEY,
    payment_id              VARCHAR(64)     NOT NULL UNIQUE,   -- PAY-xxxxxxxxxxxx
    workflow_id             VARCHAR(64)     NOT NULL,           -- Same as payment_id (Temporal workflowId)
    source_account_id       VARCHAR(64)     NOT NULL,
    destination_account_id  VARCHAR(64)     NOT NULL,
    amount                  NUMERIC(19,4)   NOT NULL,
    currency                VARCHAR(10)     NOT NULL,
    payment_type            VARCHAR(20)     NOT NULL,
    reference               VARCHAR(128),
    description             VARCHAR(512),
    status                  VARCHAR(32)     NOT NULL DEFAULT 'INITIATED',
    failure_reason          TEXT,

    -- Derived fields (populated during validation)
    payment_route           VARCHAR(32),
    correspondent_bank      VARCHAR(128),
    fx_rate                 NUMERIC(18,6),
    converted_amount        NUMERIC(19,4),
    settlement_currency     VARCHAR(10),
    charge_bearer           VARCHAR(10),
    value_date              DATE,

    -- Fraud fields
    fraud_risk_score        VARCHAR(10),
    fraud_check_details     TEXT,
    manual_review_required  BOOLEAN         DEFAULT FALSE,

    -- Accounting fields
    accounting_reference    VARCHAR(64),
    debit_ledger_entry      TEXT,
    credit_ledger_entry     TEXT,

    -- Completion fields
    transaction_reference   VARCHAR(64),
    notification_sent_sender   BOOLEAN      DEFAULT FALSE,
    notification_sent_receiver BOOLEAN      DEFAULT FALSE,

    -- Timestamps
    initiated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    validated_at            TIMESTAMPTZ,
    fraud_checked_at        TIMESTAMPTZ,
    accounting_posted_at    TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payment_records_status    ON payment_records(status);
CREATE INDEX idx_payment_records_source    ON payment_records(source_account_id);
CREATE INDEX idx_payment_records_dest      ON payment_records(destination_account_id);
CREATE INDEX idx_payment_records_initiated ON payment_records(initiated_at DESC);

-- ── workflow_event_log ────────────────────────────────────────────────────────
-- Immutable append-only audit log.
-- One row per workflow step (exit point). Never updated, only inserted.
-- Provides a complete, queryable history of every action taken on every payment.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflow_event_log (
    id              BIGSERIAL       PRIMARY KEY,
    payment_id      VARCHAR(64)     NOT NULL,
    workflow_id     VARCHAR(64)     NOT NULL,
    step_name       VARCHAR(64)     NOT NULL,   -- INITIATION, VALIDATION, FRAUD_CHECK, ACCOUNTING, COMPLETION
    event_type      VARCHAR(64)     NOT NULL,   -- STEP_STARTED, STEP_COMPLETED, STEP_FAILED
    status          VARCHAR(32)     NOT NULL,   -- SUCCESS, FAILURE, BLOCKED
    description     TEXT,
    output_data     JSONB,                       -- Structured output from the step
    error_message   TEXT,
    executed_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_event_log_payment
        FOREIGN KEY (payment_id) REFERENCES payment_records(payment_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_event_log_payment_id  ON workflow_event_log(payment_id);
CREATE INDEX idx_event_log_step        ON workflow_event_log(step_name);
CREATE INDEX idx_event_log_executed_at ON workflow_event_log(executed_at DESC);

-- ── fraud_check_requests ──────────────────────────────────────────────────────
-- Tracks Kafka messages sent to the fraud system and their responses.
-- Provides full visibility into the async fraud check round-trip.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_check_requests (
    id                  BIGSERIAL       PRIMARY KEY,
    payment_id          VARCHAR(64)     NOT NULL UNIQUE,
    correlation_id      VARCHAR(64)     NOT NULL UNIQUE,   -- Kafka message correlation key
    request_payload     JSONB           NOT NULL,
    response_payload    JSONB,
    request_sent_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    response_received_at TIMESTAMPTZ,
    status              VARCHAR(20)     NOT NULL DEFAULT 'PENDING',  -- PENDING, RESPONDED, TIMEOUT
    risk_score          VARCHAR(10),
    risk_details        TEXT
);

CREATE INDEX idx_fraud_requests_payment_id     ON fraud_check_requests(payment_id);
CREATE INDEX idx_fraud_requests_correlation_id ON fraud_check_requests(correlation_id);
CREATE INDEX idx_fraud_requests_status         ON fraud_check_requests(status);

-- ── Auto-update updated_at trigger ───────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_payment_records_updated_at
    BEFORE UPDATE ON payment_records
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
