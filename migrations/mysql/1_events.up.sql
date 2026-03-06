CREATE TABLE event_streams (
    event_stream_id VARCHAR(255) NOT NULL,
    `version`       INT NOT NULL,

    PRIMARY KEY (event_stream_id),
    CONSTRAINT check_version_streams CHECK (`version` > 0)
);

CREATE TABLE events (
    event_stream_id  VARCHAR(255) NOT NULL,
    `type`           VARCHAR(255) NOT NULL,
    `version`        INT NOT NULL,
    `event`          LONGBLOB     NOT NULL,
    metadata         JSON,

    PRIMARY KEY (event_stream_id, `version`),
    CONSTRAINT check_version_events CHECK (`version` > 0),
    CONSTRAINT fk_event_stream
        FOREIGN KEY (event_stream_id)
        REFERENCES event_streams (event_stream_id)
        ON DELETE CASCADE
);

CREATE INDEX event_stream_id_idx ON events (event_stream_id);
