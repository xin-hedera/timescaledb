-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE hyper (time bigint NOT NULL, value integer);
SELECT table_name FROM create_distributed_hypertable('hyper', 'time',
        chunk_time_interval => 200, replication_factor => 3);

INSERT INTO hyper SELECT g, g FROM generate_series(0,1000) g;


-- Sanity checking of the drop_chunk_replica API

\set ON_ERROR_STOP 0
-- NULL input for chunk id errors out
SELECT _timescaledb_internal.drop_chunk_replica(NULL, NULL);

-- Specifying any regular hypertable instead of chunk errors out
SELECT _timescaledb_internal.drop_chunk_replica(NULL, 'public.metrics');


-- Specifying regular hypertable chunk on a proper data node errors out
SELECT _timescaledb_internal.drop_chunk_replica('data_node_1', '_timescaledb_internal._hyper_1_1_chunk');

-- Specifying non-existent chunk on a proper data node errors out
SELECT _timescaledb_internal.drop_chunk_replica('data_node_1', '_timescaledb_internal._dist_hyper_700_38_chunk');

-- Get the last chunk for this hypertable
SELECT ch1.schema_name|| '.' || ch1.table_name as "CHUNK_NAME", ch1.id "CHUNK_ID" FROM
_timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id
and ht.table_name = 'hyper' ORDER BY ch1.id desc LIMIT 1 \gset

-- Specifying wrong node name errors out
SELECT _timescaledb_internal.drop_chunk_replica('bad_node', :'CHUNK_NAME');

-- This chunk contains only one entry as of now
SELECT * FROM :CHUNK_NAME;

-- Specifying NULL node name along with proper chunk errors out
SELECT _timescaledb_internal.drop_chunk_replica(NULL, :'CHUNK_NAME');

\set ON_ERROR_STOP 1
-- Check the current primary foreign server for this chunk, that will change
-- post the drop_chunk_replica call
SELECT foreign_server_name FROM information_schema.foreign_tables WHERE
    foreign_table_name = split_part(:'CHUNK_NAME', '.', 2);

-- Drop one replica of a valid chunk. Should succeed
SELECT _timescaledb_internal.drop_chunk_replica('data_node_3', :'CHUNK_NAME');

-- The primary foreign server should be updated now
SELECT foreign_server_name FROM information_schema.foreign_tables WHERE
    foreign_table_name = split_part(:'CHUNK_NAME', '.', 2);

-- Number of replicas should have been reduced by 1
SELECT count(*) from _timescaledb_catalog.chunk_data_node where chunk_id = :'CHUNK_ID';

-- Ensure that INSERTs still work on this hyper table into this chunk
INSERT INTO hyper VALUES (1001, 1001);
-- Ensure that SELECTs are able to query data from the above chunk
SELECT count(*) FROM hyper WHERE time >= 1000;

-- Drop one replica of a valid chunk. Should succeed on another datanode
SELECT _timescaledb_internal.drop_chunk_replica('data_node_2', :'CHUNK_NAME');

-- Ensure that INSERTs still work on this hyper table into this chunk
INSERT INTO hyper VALUES (1002, 1002);
-- Ensure that SELECTs are able to query data from the above chunk
SELECT count(*) FROM hyper WHERE time >= 1000;

-- Number of replicas should have been reduced by 1
SELECT count(*) from _timescaledb_catalog.chunk_data_node where chunk_id = :'CHUNK_ID';

\set ON_ERROR_STOP 0
-- Drop one replica of a valid chunk. Should not succeed on last datanode
SELECT _timescaledb_internal.drop_chunk_replica('data_node_1', :'CHUNK_NAME');

DROP table hyper;
