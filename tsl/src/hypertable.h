/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_HYPERTABLE_H
#define TIMESCALEDB_TSL_HYPERTABLE_H

#include <hypertable.h>
#include "dimension.h"
#include "interval.h"

#if PG11_GE
#include "catalog.h"
#endif /*  PG11_GE */

/* We cannot make use of more data nodes than we have slices in closed (space)
 * dimensions, and the value for number of slices is an int16. */
#define MAX_NUM_HYPERTABLE_DATA_NODES INT16_MAX

extern Datum hypertable_valid_ts_interval(PG_FUNCTION_ARGS);

#if PG11_GE
extern void hypertable_make_distributed(Hypertable *ht, List *data_node_names);
List *hypertable_assign_data_nodes(int32 hypertable_id, List *nodes);
List *hypertable_get_and_validate_data_nodes(ArrayType *nodearr);
#endif /*  PG11_GE */

#endif /* TIMESCALEDB_TSL_HYPERTABLE_H */
