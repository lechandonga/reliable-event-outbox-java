CREATE TABLE IF NOT EXISTS orders (
    id            VARCHAR(36)  PRIMARY KEY,
    order_no      VARCHAR(64)  NOT NULL,
    aggregate_key VARCHAR(128) NOT NULL,
    created_at    BIGINT       NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_event (
    id             VARCHAR(36)  PRIMARY KEY,
    aggregate_key  VARCHAR(128) NOT NULL,
    seq            BIGINT       NOT NULL,
    event_type     VARCHAR(128) NOT NULL,
    payload        CLOB         NOT NULL,
    status         VARCHAR(16)  NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    next_retry_at  BIGINT       NOT NULL DEFAULT 0,
    last_error     VARCHAR(1024),
    created_at     BIGINT       NOT NULL,
    updated_at     BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_retry ON outbox_event (status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate ON outbox_event (aggregate_key, seq);

CREATE TABLE IF NOT EXISTS processed_event (
    event_id     VARCHAR(36) PRIMARY KEY,
    processed_at BIGINT NOT NULL
);
