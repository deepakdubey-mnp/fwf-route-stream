-- =============================================================
-- V1 : routes
-- Main table persisted by the Kafka Streams sink processor.
-- id  = SHA-256(airline | sourceAirport | destinationAirport | departureTime)
--       truncated to 32 hex chars — deterministic, idempotent upsert key.
-- version  : optimistic-lock counter managed by Hibernate @Version.
-- created_* / updated_* : managed by Spring Data JPA auditing.
-- =============================================================

CREATE TABLE IF NOT EXISTS routes (
    id                  VARCHAR(32)  NOT NULL,
    airline             VARCHAR(255),
    source_airport      VARCHAR(255),
    destination_airport VARCHAR(255),
    code_share          VARCHAR(255),
    stops               INTEGER      NOT NULL DEFAULT 0,
    equipment           VARCHAR(255),
    departure_time      TIMESTAMP,
    created_at          TIMESTAMP    NOT NULL,
    updated_at          TIMESTAMP    NOT NULL,
    created_by          VARCHAR(64),
    updated_by          VARCHAR(64),
    version             BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT pk_routes PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_routes_airline             ON routes (airline);
CREATE INDEX IF NOT EXISTS idx_routes_source_airport      ON routes (source_airport);
CREATE INDEX IF NOT EXISTS idx_routes_destination_airport ON routes (destination_airport);