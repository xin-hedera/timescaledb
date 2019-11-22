/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <foreign/fdwapi.h>

#include <planner.h>
#include "planner.h"
#include "nodes/gapfill/planner.h"
#include "nodes/compress_dml/compress_dml.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "chunk.h"
#include "hypertable.h"
#include "hypertable_compression.h"
#include "hypertable_cache.h"
#include <optimizer/paths.h>
#include "compat.h"
#if !PG96
#include "fdw/fdw.h"
#include "fdw/data_node_scan_plan.h"
#endif
#include "guc.h"
#include "debug_guc.h"
#include "async_append.h"
#include "debug.h"
#include "fdw/relinfo.h"

#if PG11_GE
static bool
is_dist_hypertable_involved(PlannerInfo *root)
{
	int rti;
	Cache *hcache = ts_hypertable_cache_pin();

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RangeTblEntry *rte = root->simple_rte_array[rti];
		Hypertable *ht;

		if (!ts_is_rte_hypertable(rte))
			continue;

		ht = ts_hypertable_cache_get_entry(hcache, rte->relid);

		Assert(ht != NULL);

		if (hypertable_is_distributed(ht))
		{
			ts_cache_release(hcache);
			return true;
		}
	}

	ts_cache_release(hcache);
	return false;
}
#endif

void
tsl_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
							RelOptInfo *output_rel, void *extra)
{
	if (UPPERREL_GROUP_AGG == stage)
		plan_add_gapfill(root, output_rel);
	else if (UPPERREL_WINDOW == stage && IsA(linitial(input_rel->pathlist), CustomPath))
		gapfill_adjust_window_targetlist(root, input_rel, output_rel);
#if PG11_GE
	else if (ts_guc_enable_async_append && UPPERREL_FINAL == stage &&
			 root->parse->resultRelation == 0 && is_dist_hypertable_involved(root))
		async_append_add_paths(root, output_rel);
#endif
}

void
tsl_set_rel_pathlist_query(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						   Hypertable *ht)
{
	if (ts_guc_enable_transparent_decompression && ht != NULL &&
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL && TS_HYPERTABLE_HAS_COMPRESSION(ht) &&
		rel->fdw_private != NULL && ((TimescaleDBPrivate *) rel->fdw_private)->compressed)
	{
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, true);

		if (chunk->fd.compressed_chunk_id > 0)
			ts_decompress_chunk_generate_paths(root, rel, ht, chunk);
	}
}
void
tsl_set_rel_pathlist_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						 Hypertable *ht)
{
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION(ht))
	{
		ListCell *lc;
		/* is this a chunk under compressed hypertable ? */
		AppendRelInfo *appinfo = ts_get_appendrelinfo(root, rti, false);
		Oid PG_USED_FOR_ASSERTS_ONLY parent_oid = appinfo->parent_reloid;
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, true);
		Assert(parent_oid == ht->main_table_relid && (parent_oid == chunk->hypertable_relid));
		if (chunk->fd.compressed_chunk_id > 0)
		{
			foreach (lc, rel->pathlist)
			{
				Path **pathptr = (Path **) &lfirst(lc);
				*pathptr = compress_chunk_dml_generate_paths(*pathptr, chunk);
			}
		}
	}
}

#if PG11_GE
/* The fdw needs to expand a distributed hypertable inside the `GetForeignPath` callback. But, since
 * the hypertable base table is not a foreign table, that callback would not normally be called.
 * Thus, we call it manually in this hook.
 */
void
tsl_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, rte->relid);

	if (rel->fdw_private != NULL && ht != NULL && hypertable_is_distributed(ht))
	{
		FdwRoutine *fdw = (FdwRoutine *) DatumGetPointer(
			DirectFunctionCall1(timescaledb_fdw_handler, PointerGetDatum(NULL)));

		fdw->GetForeignRelSize(root, rel, rte->relid);
		fdw->GetForeignPaths(root, rel, rte->relid);

#ifdef TS_DEBUG
		if (ts_debug_optimizer_flags.show_rel)
			tsl_debug_log_rel_with_paths(root, rel, (UpperRelationKind *) NULL);
#endif
	}

	ts_cache_release(hcache);
}
#endif
