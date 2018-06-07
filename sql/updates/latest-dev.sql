DROP FUNCTION IF EXISTS drop_chunks("any",name,name,boolean,"any",boolean,boolean);
DROP FUNCTION IF EXISTS add_drop_chunks_policy(REGCLASS,INTERVAL,BOOL,BOOL,BOOL);

ALTER TABLE _timescaledb_catalog.dimension
ADD COLUMN integer_now_func_schema     NAME     NULL;

ALTER TABLE _timescaledb_catalog.dimension
ADD COLUMN integer_now_func            NAME     NULL;

ALTER TABLE _timescaledb_catalog.dimension
ADD CONSTRAINT dimension_check2
CHECK (
        (integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR
        (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)
);
-- ----------------------
CREATE TYPE _timescaledb_catalog.ts_interval AS (
    is_time_interval        BOOLEAN,
    time_interval		    INTERVAL,
    integer_interval        BIGINT
    );

-- q -- todo:: this is probably necessary if we keep the validation constraint in the table definition.
CREATE OR REPLACE FUNCTION _timescaledb_internal.valid_ts_interval(invl _timescaledb_catalog.ts_interval)
RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'ts_valid_ts_interval' LANGUAGE C VOLATILE STRICT;

DROP VIEW IF EXISTS timescaledb_information.drop_chunks_policies;
DROP VIEW IF EXISTS timescaledb_information.policy_stats;

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_drop_chunks_tmp (
    job_id          		    INTEGER                 PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		    INTEGER     UNIQUE      NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    older_than	    _timescaledb_catalog.ts_interval    NOT NULL,
	cascade					    BOOLEAN                 NOT NULL,
    cascade_to_materializations BOOLEAN                 NOT NULL,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);

INSERT INTO _timescaledb_config.bgw_policy_drop_chunks_tmp
(SELECT job_id, hypertable_id, ROW('t',older_than,NULL)::_timescaledb_catalog.ts_interval  as older_than, cascade, cascade_to_materializations
FROM _timescaledb_config.bgw_policy_drop_chunks);

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_policy_drop_chunks;
DROP TABLE _timescaledb_config.bgw_policy_drop_chunks;

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_drop_chunks (
    job_id          		    INTEGER                 PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		    INTEGER     UNIQUE      NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    older_than	    _timescaledb_catalog.ts_interval    NOT NULL,
	cascade					    BOOLEAN                 NOT NULL,
    cascade_to_materializations BOOLEAN                 NOT NULL,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);

INSERT INTO _timescaledb_config.bgw_policy_drop_chunks
(SELECT * FROM _timescaledb_config.bgw_policy_drop_chunks_tmp);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_drop_chunks', '');
DROP TABLE _timescaledb_config.bgw_policy_drop_chunks_tmp;
GRANT SELECT ON _timescaledb_config.bgw_policy_drop_chunks TO PUBLIC;

DROP FUNCTION IF EXISTS alter_job_schedule(INTEGER, INTERVAL, INTERVAL, INTEGER, INTERVAL, BOOL);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log TO PUBLIC;
DROP FUNCTION IF EXISTS get_telemetry_report();

-- Add new function definitions, columns and tables for distributed hypertables
DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc);

ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN replication_factor SMALLINT NULL CHECK (replication_factor > 0);

-- Table for hypertable -> servers mappings
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_server (
    hypertable_id          INTEGER NOT NULL     REFERENCES _timescaledb_catalog.hypertable(id),
    server_hypertable_id   INTEGER NULL,
    server_name            NAME NOT NULL,
    UNIQUE(server_hypertable_id, server_name),
    UNIQUE(hypertable_id, server_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_server', '');

GRANT SELECT ON _timescaledb_catalog.hypertable_server TO PUBLIC;

-- Table for chunk -> servers mappings
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_server (
    chunk_id               INTEGER NOT NULL     REFERENCES _timescaledb_catalog.chunk(id),
    server_chunk_id        INTEGER NOT NULL,
    server_name            NAME NOT NULL,
    UNIQUE(server_chunk_id, server_name),
    UNIQUE(chunk_id, server_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_server', '');

GRANT SELECT ON _timescaledb_catalog.chunk_server TO PUBLIC;

--placeholder to allow creation of functions below
CREATE TYPE rxid;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rxid_in(cstring) RETURNS rxid
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_in' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rxid_out(rxid) RETURNS cstring
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_out' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE rxid (
   internallength = 16,
   input = _timescaledb_internal.rxid_in,
   output = _timescaledb_internal.rxid_out
);

CREATE TABLE _timescaledb_catalog.remote_txn (
    server_name              NAME, --this is really only to allow us to cleanup stuff on a per-server basis.
    remote_transaction_id    TEXT CHECK (remote_transaction_id::rxid is not null),
    PRIMARY KEY (server_name, remote_transaction_id)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.remote_txn', '');


GRANT SELECT ON _timescaledb_catalog.remote_txn TO PUBLIC;
