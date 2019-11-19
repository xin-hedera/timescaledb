/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <storage/procarray.h>
#include <foreign/foreign.h>
#include <foreign/fdwapi.h>
#include <miscadmin.h>
#include <access/reloptions.h>
#include <access/htup_details.h>
#include <catalog/pg_foreign_server.h>
#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <utils/guc.h>
#include <fmgr.h>
#include <funcapi.h>

#include "export.h"
#include "remote/connection.h"
#include "test_utils.h"

static void
test_options()
{
	Assert(remote_connection_valid_user_option("user"));
	Assert(!remote_connection_valid_user_option("port"));
	Assert(!remote_connection_valid_user_option("xxx"));
	Assert(!remote_connection_valid_user_option("fallback_application_name"));

	Assert(remote_connection_valid_node_option("port"));
	Assert(!remote_connection_valid_node_option("user"));
	Assert(!remote_connection_valid_node_option("xxx"));
	Assert(!remote_connection_valid_node_option("fallback_application_name"));
}

static void
test_numbers_associated_with_connections()
{
	TSConnection *conn = get_connection();
	Assert(remote_connection_get_cursor_number() == 1);
	Assert(remote_connection_get_cursor_number() == 2);
	Assert(remote_connection_get_cursor_number() == 3);
	remote_connection_reset_cursor_number();
	Assert(remote_connection_get_cursor_number() == 1);
	Assert(remote_connection_get_cursor_number() == 2);

	Assert(remote_connection_get_prep_stmt_number() == 1);
	Assert(remote_connection_get_prep_stmt_number() == 2);
	Assert(remote_connection_get_prep_stmt_number() == 3);
	remote_connection_close(conn);
}

static void
test_simple_queries()
{
	TSConnection *conn = get_connection();
	PGresult *res;
	remote_connection_exec(conn, "SELECT 1");
	remote_connection_exec(conn, "SET search_path = pg_catalog");

	res = remote_connection_exec(conn, "SELECT 1");
	Assert(PQresultStatus(res) == PGRES_TUPLES_OK);
	PQclear(res);
	res = remote_connection_exec(conn, "SELECT abc");
	Assert(PQresultStatus(res) != PGRES_TUPLES_OK);
	PQclear(res);
	res = remote_connection_exec(conn, "SET search_path = pg_catalog");
	Assert(PQresultStatus(res) == PGRES_COMMAND_OK);
	PQclear(res);
	res = remote_connection_exec(conn, "SET 123 = 123");
	Assert(PQresultStatus(res) != PGRES_COMMAND_OK);
	PQclear(res);

	remote_connection_cmd_ok(conn, "SET search_path = pg_catalog");
	/* not a command */
	EXPECT_ERROR_STMT(remote_connection_cmd_ok(conn, "SELECT 1"));
	remote_connection_close(conn);
}

#define ASSERT_NUM_OPEN_CONNECTIONS(stats, num)                                                    \
	Assert((((stats)->connections_created - (stats)->connections_closed) == num))
#define ASSERT_NUM_OPEN_RESULTS(stats, num)                                                        \
	Assert((((stats)->results_created - (stats)->results_cleared) == num))

static void
test_connection_and_result_leaks()
{
	TSConnection *conn, *subconn;
	PGresult *res;
	RemoteConnectionStats *stats;

	stats = remote_connection_stats_get();
	remote_connection_stats_reset();

	conn = get_connection();
	res = remote_connection_exec(conn, "SELECT 1");
	remote_connection_close(conn);

	ASSERT_NUM_OPEN_CONNECTIONS(stats, 0);
	ASSERT_NUM_OPEN_RESULTS(stats, 0);

	conn = get_connection();

	ASSERT_NUM_OPEN_CONNECTIONS(stats, 1);

	BeginInternalSubTransaction("conn leak test");

	subconn = get_connection();
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 2);

	remote_connection_exec(conn, "SELECT 1");
	ASSERT_NUM_OPEN_RESULTS(stats, 1);

	BeginInternalSubTransaction("conn leak test 2");

	res = remote_connection_exec(subconn, "SELECT 1");
	ASSERT_NUM_OPEN_RESULTS(stats, 2);

	/* Explicitly close one result */
	remote_result_close(res);

	ASSERT_NUM_OPEN_RESULTS(stats, 1);

	remote_connection_exec(subconn, "SELECT 1");
	remote_connection_exec(conn, "SELECT 1");

	ASSERT_NUM_OPEN_RESULTS(stats, 3);

	RollbackAndReleaseCurrentSubTransaction();

	/* Rollback should have cleared the two results created in the
	 * sub-transaction, but not the one created before the sub-transaction */
	ASSERT_NUM_OPEN_RESULTS(stats, 1);

	remote_connection_exec(subconn, "SELECT 1");
	ASSERT_NUM_OPEN_RESULTS(stats, 2);

	ReleaseCurrentSubTransaction();

	/* Should only leave the original connection created before the first
	 * sub-transaction, but no results */
	ASSERT_NUM_OPEN_CONNECTIONS(stats, 1);
	ASSERT_NUM_OPEN_RESULTS(stats, 0);

	remote_connection_stats_reset();
}

TS_FUNCTION_INFO_V1(ts_test_bad_remote_query);

/* Send a bad query that throws an exception without cleaning up connection or
 * results. Together with get_connection_stats(), this should show that
 * connections and results are automatically cleaned up. */
Datum
ts_test_bad_remote_query(PG_FUNCTION_ARGS)
{
	TSConnection *conn;
	PGresult *result;

	conn = get_connection();
	result = remote_connection_exec(conn, "BADY QUERY SHOULD THROW ERROR");
	Assert(PQresultStatus(result) == PGRES_FATAL_ERROR);
	elog(ERROR, "bad query error thrown from test");

	PG_RETURN_VOID();
}

enum Anum_connection_stats
{
	Anum_connection_stats_connections_created = 1,
	Anum_connection_stats_connections_closed,
	Anum_connection_stats_results_created,
	Anum_connection_stats_results_cleared,
	Anum_connection_stats_max,
};

TS_FUNCTION_INFO_V1(ts_test_get_connection_stats);

Datum
ts_test_get_connection_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	RemoteConnectionStats *stats = remote_connection_stats_get();
	Datum values[Anum_connection_stats_max];
	bool nulls[Anum_connection_stats_max] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	values[Anum_connection_stats_connections_created - 1] =
		Int64GetDatum((int64) stats->connections_created);
	values[Anum_connection_stats_connections_closed - 1] =
		Int64GetDatum((int64) stats->connections_closed);
	values[Anum_connection_stats_results_created - 1] =
		Int64GetDatum((int64) stats->results_created);
	values[Anum_connection_stats_results_cleared - 1] =
		Int64GetDatum((int64) stats->results_cleared);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

TS_FUNCTION_INFO_V1(ts_test_remote_connection);

Datum
ts_test_remote_connection(PG_FUNCTION_ARGS)
{
	test_options();
	test_numbers_associated_with_connections();
	test_simple_queries();
	test_connection_and_result_leaks();

	PG_RETURN_VOID();
}
