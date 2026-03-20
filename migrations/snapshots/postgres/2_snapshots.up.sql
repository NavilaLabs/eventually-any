CREATE TABLE snapshots (
    id               BIGSERIAL   NOT NULL PRIMARY KEY,
    aggregate_type   TEXT        NOT NULL,
    aggregate_id     TEXT        NOT NULL,
    event_stream_id  TEXT        NOT NULL REFERENCES event_streams (event_stream_id) ON DELETE CASCADE,
    "version"        INTEGER     NOT NULL CHECK ("version" > 0),
    "state"          BYTEA       NOT NULL,
    taken_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX snapshots_lookup_idx
    ON snapshots (aggregate_type, aggregate_id, "version" DESC);
