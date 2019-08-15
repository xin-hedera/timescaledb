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
	remote_connection_query_ok_result(conn, "SELECT 1");
	remote_connection_query_ok_result(conn, "SET search_path = pg_catalog");

	res = remote_connection_query_any_result(conn, "SELECT 1");
	Assert(PQresultStatus(res) == PGRES_TUPLES_OK);
	remote_connection_result_close(res);
	res = remote_connection_query_any_result(conn, "SELECT abc");
	Assert(PQresultStatus(res) != PGRES_TUPLES_OK);
	remote_connection_result_close(res);
	res = remote_connection_query_any_result(conn, "SET search_path = pg_catalog");
	Assert(PQresultStatus(res) == PGRES_COMMAND_OK);
	remote_connection_result_close(res);
	res = remote_connection_query_any_result(conn, "SET 123 = 123");
	Assert(PQresultStatus(res) != PGRES_COMMAND_OK);
	remote_connection_result_close(res);

	remote_connection_exec_ok_command(conn, "SET search_path = pg_catalog");
	/* not a command */
	EXPECT_ERROR_STMT(remote_connection_exec_ok_command(conn, "SELECT 1"));
	remote_connection_close(conn);
}

static void
test_prepared_stmts()
{
	TSConnection *conn = get_connection();
	const char **params = (const char **) palloc(sizeof(char *) * 5);
	PreparedStmt *prep;
	PGresult *res;

	prep = remote_connection_prepare(conn, "SELECT 3", 0);
	res = remote_connection_query_prepared_ok_result(prep, NULL);
	Assert(PQresultStatus(res) == PGRES_TUPLES_OK);
	Assert(strcmp(PQgetvalue(res, 0, 0), "3") == 0);
	remote_connection_result_close(res);
	prepared_stmt_close(prep);

	prep = remote_connection_prepare(conn, "SELECT $1, $3, $2", 3);
	params[0] = "2";
	params[1] = "4";
	params[2] = "8";
	res = remote_connection_query_prepared_ok_result(prep, params);
	Assert(PQresultStatus(res) == PGRES_TUPLES_OK);
	Assert(strcmp(PQgetvalue(res, 0, 0), "2") == 0);
	Assert(strcmp(PQgetvalue(res, 0, 1), "8") == 0);
	Assert(strcmp(PQgetvalue(res, 0, 2), "4") == 0);
	remote_connection_result_close(res);
	prepared_stmt_close(prep);

	/* malformed sql (missing commas) */
	EXPECT_ERROR_STMT(prep = remote_connection_prepare(conn, "SELECT $1 $3 $2", 3));

	remote_connection_close(conn);
}

static void
test_params()
{
	TSConnection *conn = get_connection();
	const char **params = (const char **) palloc(sizeof(char *) * 5);
	PGresult *res;

	params[0] = "2";

	res = remote_connection_query_with_params_ok_result(conn, "SELECT $1", 1, params);
	Assert(PQresultStatus(res) == PGRES_TUPLES_OK);
	Assert(strcmp(PQgetvalue(res, 0, 0), "2") == 0);
	remote_connection_result_close(res);

	EXPECT_ERROR
	res = remote_connection_query_with_params_ok_result(conn, "SELECT 1 2 3", 1, params);
	EXPECT_ERROR_END

	remote_connection_close(conn);
}

static void
test_result_leaks()
{
	TSConnection *conn, *subconn;
	PGresult *res;
	RemoteConnectionStats *stats, savedstats;

	stats = remote_connection_stats_get();

	remote_connection_stats_reset();

	conn = get_connection();
	res = remote_connection_query_ok_result(conn, "SELECT 1");
	remote_connection_close(conn);
	Assert(stats->connections_closed == 1);
	Assert(stats->connections_closed == stats->connections_created);
	Assert(stats->results_cleared == stats->results_created);

	/* Reset stats */
	remote_connection_stats_reset();
	conn = get_connection();

	BeginInternalSubTransaction("conn leak test");

	subconn = get_connection();
	remote_connection_query_ok_result(conn, "SELECT 1");

	BeginInternalSubTransaction("conn leak test 2");

	res = remote_connection_query_ok_result(subconn, "SELECT 1");

	/* Explicitly close one result */
	remote_connection_result_close(res);

	/* get_connection() creates and clears results so use saved stats as a
	 * reference */
	savedstats = *stats;
	remote_connection_query_ok_result(subconn, "SELECT 1");
	remote_connection_query_ok_result(conn, "SELECT 1");

	RollbackAndReleaseCurrentSubTransaction();

	/* Should have cleared two results on rollback (one on each connection) */
	Assert(stats->results_cleared == (savedstats.results_cleared + 2));
	remote_connection_query_ok_result(subconn, "SELECT 1");

	savedstats = *stats;
	ReleaseCurrentSubTransaction();

	/* Should have cleared three results and one connection */
	Assert(stats->results_cleared == (savedstats.results_cleared + 3));
	Assert(stats->connections_closed == (savedstats.connections_closed + 1));

	remote_connection_stats_reset();
}

TS_FUNCTION_INFO_V1(tsl_test_bad_remote_query);

/* Send a bad query that throws an exception without cleaning up connection or
 * results. Together with get_connection_stats(), this should show that
 * connections and results are automatically cleaned up. */
Datum
tsl_test_bad_remote_query(PG_FUNCTION_ARGS)
{
	TSConnection *conn;

	EXPECT_ERROR;
	conn = get_connection();
	remote_connection_query_ok_result(conn, "BADY QUERY SHOULD THROW ERROR");
	EXPECT_ERROR_END;

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

TS_FUNCTION_INFO_V1(tsl_test_get_connection_stats);

Datum
tsl_test_get_connection_stats(PG_FUNCTION_ARGS)
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

	values[Anum_connection_stats_connections_created - 1] = stats->connections_created;
	values[Anum_connection_stats_connections_closed - 1] = stats->connections_closed;
	values[Anum_connection_stats_results_created - 1] = stats->results_created;
	values[Anum_connection_stats_results_cleared - 1] = stats->results_cleared;

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

TS_FUNCTION_INFO_V1(tsl_test_remote_connection);

Datum
tsl_test_remote_connection(PG_FUNCTION_ARGS)
{
	test_options();
	test_numbers_associated_with_connections();
	test_simple_queries();
	test_prepared_stmts();
	test_params();
	test_result_leaks();

	PG_RETURN_VOID();
}
