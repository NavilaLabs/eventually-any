CREATE TABLE aggregates (
    aggregate_id TEXT    NOT NULL PRIMARY KEY REFERENCES event_streams (event_stream_id) ON DELETE CASCADE,
    "type"       TEXT    NOT NULL,
    "version"    INTEGER NOT NULL CHECK ("version" > 0),
    "state"      BYTEA   NOT NULL
);
