CREATE TABLE snapshots (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    aggregate_type   VARCHAR(255) NOT NULL,
    aggregate_id     VARCHAR(255) NOT NULL,
    event_stream_id  VARCHAR(255) NOT NULL,
    `version`        INT          NOT NULL,
    `state`          LONGBLOB     NOT NULL,
    taken_at         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    CONSTRAINT check_snapshot_version CHECK (`version` > 0),
    CONSTRAINT fk_snapshot_stream
        FOREIGN KEY (event_stream_id)
        REFERENCES event_streams (event_stream_id)
        ON DELETE CASCADE
);

CREATE INDEX snapshots_lookup_idx
    ON snapshots (aggregate_type, aggregate_id, `version` DESC);
