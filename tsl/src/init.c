/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include "bgw_policy/compression_api.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/retention_api.h"
#include "bgw_policy/job.h"
#include "bgw_policy/job_api.h"
#include "bgw_policy/reorder_api.h"
#include "chunk_api.h"
#include "chunk.h"
#include "compression/array.h"
#include "compression/compression.h"
#include "compression/compress_utils.h"
#include "compression/create.h"
#include "compression/deltadelta.h"
#include "compression/dictionary.h"
#include "compression/gorilla.h"
#include "compression/segment_meta.h"
#include "continuous_aggs/create.h"
#include "continuous_aggs/insert.h"
#include "continuous_aggs/options.h"
#include "continuous_aggs/refresh.h"
#include "continuous_aggs/invalidation.h"
#include "cross_module_fn.h"
#include "nodes/data_node_dispatch.h"
#include "data_node.h"
#include "dist_util.h"
#include "export.h"
#include "fdw/fdw.h"
#include "hypertable.h"
#include "license_guc.h"
#include "nodes/decompress_chunk/planner.h"
#include "nodes/skip_scan/skip_scan.h"
#include "nodes/gapfill/gapfill.h"
#include "partialize_finalize.h"
#include "planner.h"
#include "process_utility.h"
#include "process_utility.h"
#include "remote/connection_cache.h"
#include "remote/connection.h"
#include "remote/dist_commands.h"
#include "remote/dist_copy.h"
#include "remote/dist_txn.h"
#include "remote/txn_id.h"
#include "remote/txn_resolve.h"
#include "reorder.h"
#include "telemetry.h"
#include "dist_backup.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#ifdef APACHE_ONLY
#error "cannot compile the TSL for ApacheOnly mode"
#endif

extern void PGDLLEXPORT _PG_init(void);
extern void PGDLLEXPORT _PG_fini(void);

static void
cache_syscache_invalidate(Datum arg, int cacheid, uint32 hashvalue)
{
	remote_connection_cache_invalidate_callback(arg, cacheid, hashvalue);
}

/*
 * Cross module function initialization.
 *
 * During module start we set ts_cm_functions to point at the tsl version of the
 * function registry.
 *
 * NOTE: To ensure that your cross-module function has a correct default, you
 * must also add it to ts_cm_functions_default in cross_module_fn.c in the
 * Apache codebase.
 */
CrossModuleFunctions tsl_cm_functions = {
	.add_tsl_telemetry_info = tsl_telemetry_add_info,

	.create_upper_paths_hook = tsl_create_upper_paths_hook,
	.set_rel_pathlist_dml = tsl_set_rel_pathlist_dml,
	.set_rel_pathlist_query = tsl_set_rel_pathlist_query,

	/* bgw policies */
	.policy_compression_add = policy_compression_add,
	.policy_compression_proc = policy_compression_proc,
	.policy_compression_remove = policy_compression_remove,
	.policy_refresh_cagg_add = policy_refresh_cagg_add,
	.policy_refresh_cagg_proc = policy_refresh_cagg_proc,
	.policy_refresh_cagg_remove = policy_refresh_cagg_remove,
	.policy_reorder_add = policy_reorder_add,
	.policy_reorder_proc = policy_reorder_proc,
	.policy_reorder_remove = policy_reorder_remove,
	.policy_retention_add = policy_retention_add,
	.policy_retention_proc = policy_retention_proc,
	.policy_retention_remove = policy_retention_remove,

	.job_add = job_add,
	.job_alter = job_alter,
	.job_delete = job_delete,
	.job_run = job_run,
	.job_execute = job_execute,

	/* gapfill */
	.gapfill_marker = gapfill_marker,
	.gapfill_int16_time_bucket = gapfill_int16_time_bucket,
	.gapfill_int32_time_bucket = gapfill_int32_time_bucket,
	.gapfill_int64_time_bucket = gapfill_int64_time_bucket,
	.gapfill_date_time_bucket = gapfill_date_time_bucket,
	.gapfill_timestamp_time_bucket = gapfill_timestamp_time_bucket,
	.gapfill_timestamptz_time_bucket = gapfill_timestamptz_time_bucket,

	.reorder_chunk = tsl_reorder_chunk,
	.move_chunk = tsl_move_chunk,
	.partialize_agg = tsl_partialize_agg,
	.finalize_agg_sfunc = tsl_finalize_agg_sfunc,
	.finalize_agg_ffunc = tsl_finalize_agg_ffunc,
	.process_cagg_viewstmt = tsl_process_continuous_agg_viewstmt,
	.continuous_agg_invalidation_trigger = continuous_agg_trigfn,
	.continuous_agg_update_options = continuous_agg_update_options,
	.continuous_agg_refresh = continuous_agg_refresh,
	.continuous_agg_refresh_chunk = continuous_agg_refresh_chunk,
	.continuous_agg_invalidate = invalidation_add_entry,
	.compressed_data_decompress_forward = tsl_compressed_data_decompress_forward,
	.compressed_data_decompress_reverse = tsl_compressed_data_decompress_reverse,
	.compressed_data_send = tsl_compressed_data_send,
	.compressed_data_recv = tsl_compressed_data_recv,
	.compressed_data_in = tsl_compressed_data_in,
	.compressed_data_out = tsl_compressed_data_out,
	.deltadelta_compressor_append = tsl_deltadelta_compressor_append,
	.deltadelta_compressor_finish = tsl_deltadelta_compressor_finish,
	.gorilla_compressor_append = tsl_gorilla_compressor_append,
	.gorilla_compressor_finish = tsl_gorilla_compressor_finish,
	.dictionary_compressor_append = tsl_dictionary_compressor_append,
	.dictionary_compressor_finish = tsl_dictionary_compressor_finish,
	.array_compressor_append = tsl_array_compressor_append,
	.array_compressor_finish = tsl_array_compressor_finish,
	.process_compress_table = tsl_process_compress_table,
	.process_altertable_cmd = tsl_process_altertable_cmd,
	.process_rename_cmd = tsl_process_rename_cmd,
	.compress_chunk = tsl_compress_chunk,
	.decompress_chunk = tsl_decompress_chunk,
	.data_node_add = data_node_add,
	.data_node_delete = data_node_delete,
	.data_node_attach = data_node_attach,
	.data_node_ping = data_node_ping,
	.data_node_detach = data_node_detach,
	.data_node_allow_new_chunks = data_node_allow_new_chunks,
	.data_node_block_new_chunks = data_node_block_new_chunks,
	.chunk_set_default_data_node = chunk_set_default_data_node,
	.show_chunk = chunk_show,
	.create_chunk = chunk_create,
	.create_chunk_on_data_nodes = chunk_api_create_on_data_nodes,
	.drop_chunk_replica = drop_chunk_replica,
	.hypertable_make_distributed = hypertable_make_distributed,
	.get_and_validate_data_node_list = hypertable_get_and_validate_data_nodes,
	.timescaledb_fdw_handler = timescaledb_fdw_handler,
	.timescaledb_fdw_validator = timescaledb_fdw_validator,
	.remote_txn_id_in = remote_txn_id_in_pg,
	.remote_txn_id_out = remote_txn_id_out_pg,
	.remote_txn_heal_data_node = remote_txn_heal_data_node,
	.remote_connection_cache_show = remote_connection_cache_show,
	.set_rel_pathlist = tsl_set_rel_pathlist,
	.data_node_dispatch_path_create = data_node_dispatch_path_create,
	.distributed_copy = remote_distributed_copy,
	.ddl_command_start = tsl_ddl_command_start,
	.ddl_command_end = tsl_ddl_command_end,
	.sql_drop = tsl_sql_drop,
	.set_distributed_id = dist_util_set_id,
	.set_distributed_peer_id = dist_util_set_peer_id,
	.is_frontend_session = dist_util_is_frontend_session,
	.remove_from_distributed_db = dist_util_remove_from_db,
	.dist_remote_hypertable_info = dist_util_remote_hypertable_info,
	.dist_remote_chunk_info = dist_util_remote_chunk_info,
	.dist_remote_compressed_chunk_info = dist_util_remote_compressed_chunk_info,
	.dist_remote_hypertable_index_info = dist_util_remote_hypertable_index_info,
	.validate_as_data_node = validate_data_node_settings,
	.distributed_exec = ts_dist_cmd_exec,
	.create_distributed_restore_point = create_distributed_restore_point,
	.func_call_on_data_nodes = ts_dist_cmd_func_call_on_data_nodes,
	.chunk_get_relstats = chunk_api_get_chunk_relstats,
	.chunk_get_colstats = chunk_api_get_chunk_colstats,
	.chunk_create_empty_table = chunk_create_empty_table,
	.hypertable_distributed_set_replication_factor = hypertable_set_replication_factor,
	.cache_syscache_invalidate = cache_syscache_invalidate,
	.update_compressed_chunk_relstats = update_compressed_chunk_relstats,
};

TS_FUNCTION_INFO_V1(ts_module_init);
/*
 * Module init function, sets ts_cm_functions to point at tsl_cm_functions
 */
PGDLLEXPORT Datum
ts_module_init(PG_FUNCTION_ARGS)
{
	ts_cm_functions = &tsl_cm_functions;

	_continuous_aggs_cache_inval_init();
	_decompress_chunk_init();
	_skip_scan_init();
	_remote_connection_cache_init();
	_remote_dist_txn_init();
	_tsl_process_utility_init();

	PG_RETURN_BOOL(true);
}

/* Informative functions */

PGDLLEXPORT void
_PG_init(void)
{
	/*
	 * In a normal backend, we disable loading the tsl until after the main
	 * timescale library is loaded, after which we enable it from the loader.
	 * In parallel workers the restore shared libraries function will load the
	 * libraries itself, and we bypass the loader, so we need to ensure that
	 * timescale is aware it can use the tsl if needed. It is always safe to
	 * do this here, because if we reach this point, we must have already
	 * loaded the tsl, so we no longer need to worry about its load order
	 * relative to the other libraries.
	 */
	ts_license_enable_module_loading();

	_remote_connection_init();
}

PGDLLEXPORT void
_PG_fini(void)
{
	_remote_connection_fini();
}
