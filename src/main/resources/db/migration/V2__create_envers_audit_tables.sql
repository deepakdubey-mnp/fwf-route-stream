-- =============================================================
-- V2 : Hibernate Envers audit tables
--
--   revinfo     : one row per revision (= one JPA transaction).
--                 Custom columns: rev, revtstmp, username.
--   routes_aud  : full column snapshot of routes at each revision.
--                 revtype:  0 = INSERT  1 = UPDATE  2 = DELETE
--
-- Sequence name MUST match AuditRevisionEntity:
--   @SequenceGenerator(sequenceName = "revinfo_rev_seq", allocationSize = 1)
-- INCREMENT BY 1 must align with allocationSize = 1 — mismatching causes
-- Hibernate to skip IDs or issue extra DB calls.
-- =============================================================

CREATE SEQUENCE IF NOT EXISTS revinfo_rev_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS revinfo (
    rev       INTEGER     NOT NULL DEFAULT nextval('revinfo_rev_seq'),
    revtstmp  BIGINT,
    username  VARCHAR(64),
    CONSTRAINT pk_revinfo PRIMARY KEY (rev)
);

ALTER SEQUENCE revinfo_rev_seq OWNED BY revinfo.rev;

-- -----------------------------------------------------------------
-- routes_aud: mirrors every column of routes (all nullable — DELETE
-- revisions carry NULL for all business columns).
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS routes_aud (
    id                  VARCHAR(32)  NOT NULL,
    rev                 INTEGER      NOT NULL,
    revtype             SMALLINT,
    airline             VARCHAR(255),
    source_airport      VARCHAR(255),
    destination_airport VARCHAR(255),
    code_share          VARCHAR(255),
    stops               INTEGER,
    equipment           VARCHAR(255),
    departure_time      TIMESTAMP,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    created_by          VARCHAR(64),
    updated_by          VARCHAR(64),
    version             BIGINT,

    CONSTRAINT pk_routes_aud     PRIMARY KEY (id, rev),
    CONSTRAINT fk_routes_aud_rev FOREIGN KEY (rev) REFERENCES revinfo (rev)
);

CREATE INDEX IF NOT EXISTS idx_routes_aud_rev ON routes_aud (rev);