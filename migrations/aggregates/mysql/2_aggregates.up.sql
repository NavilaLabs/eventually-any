CREATE TABLE aggregates (
    aggregate_id VARCHAR(255) NOT NULL,
    `type`       VARCHAR(255) NOT NULL,
    `version`    INT NOT NULL,
    `state`      LONGBLOB NOT NULL,

    PRIMARY KEY (aggregate_id),
    CONSTRAINT fk_aggregate_stream
        FOREIGN KEY (aggregate_id)
        REFERENCES event_streams (event_stream_id)
        ON DELETE CASCADE,
    CONSTRAINT check_aggregate_version CHECK (`version` > 0)
);
