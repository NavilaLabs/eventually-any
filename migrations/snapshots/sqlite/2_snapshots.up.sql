CREATE TABLE snapshots (
    id               INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    aggregate_type   TEXT    NOT NULL,
    aggregate_id     TEXT    NOT NULL,
    event_stream_id  TEXT    NOT NULL REFERENCES event_streams (event_stream_id) ON DELETE CASCADE,
    "version"        INTEGER NOT NULL CHECK ("version" > 0),
    "state"          BLOB    NOT NULL,
    taken_at         TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX snapshots_lookup_idx
    ON snapshots (aggregate_type, aggregate_id, "version" DESC);
