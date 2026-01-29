CREATE DATABASE autodesk; 
use autodesk;

CREATE TABLE IF NOT EXISTS enriched_user (
    user_id                 VARCHAR(64)  NOT NULL,
    name                    VARCHAR(255) NOT NULL,
    enriched_data           TEXT         NOT NULL,
    created_timestamp       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_timestamp TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
);
