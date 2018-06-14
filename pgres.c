/*
 * pgres.c: Postgres routines
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

// For keydesc in schema.h
#include <isam.h>

#include <libpq-fe.h>

#include "sys.h"
#include "schema.h"
#include "pgres.h"
#include "xstring.h"


// Static data

// Shared data
char * last_sql = NULL;
int last_sql_count = 0;

static char color_red[] = {	0x1b, '[', '3', '1', 'm', 0 };
static char color_magenta[] = { 0x1b, '[', '3', '5', 'm', 0 };
static char color_yellow[] = { 0x1b, '[', '3', '3', 'm', 0 };
static char color_normal[] = { 0x1b, '[', '3', '7', 'm', 0 };

// Static function prototypes
static void pg_print_tuples(FILE *fd, RES *res);


// CODE STARTS HERE

/*
 * pg_msg
 * Message wrapper to print postgres related messages
 */
void pg_msg (CONN * conn, int mode, char *fmt, ...)
{
	va_list ap;
	char buf[2048];
	
__STACK(pg_msg)
	
	va_start(ap, fmt);
	
	vsprintf(buf, fmt, ap);
	
	pgout_t(0, "SQL %s [%s]", PQerrorMessage (conn->pgconn), buf);
	
	va_end(ap);
	
	__return;
	
} /* pg_msg */


void pg_free (void *data)
{
__STACK(pg_free)

	if (! data) {
		__return;
	}
	
	PQfreemem(data);
	//free(data);
		
	data = (void *)NULL;
	
	__return;

} /* pg_free */


/*
 * pg_startup
 * Startup a connection to a Postgres database
 */
CONN * pg_startup (CONN * conn, char * connstr, char * set_schema)
{
	PGconn * pgconn = NULL;
	PGresult * res = NULL;
	char *sql = NULL;
	
__STACK(pg_startup)
	
	pgout(mDEBUG3, "pg_startup: connecting [%s]", connstr);
	
	conn->pgconn = PQconnectdb(connstr);
	
	if (PQstatus(conn->pgconn) != CONNECTION_OK) {
		pg_msg(conn, 0, "pg_startup");
		pgout(mDEBUG, "connstr=[%s]", connstr);
		__return (CONN *)NULL;
	}
	
	asprintf(&sql, "SET search_path TO %s", set_schema);
	res = PQexec(conn->pgconn, sql);
	xfree(sql);
	
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		pg_msg(conn, 0, "%s failed", sql);
        PQclear(res);
        PQfinish(conn->pgconn);
        __return (CONN *)NULL;
    }
	
	__return conn;
	
} /* pg_startup */


/*
 * pg_shutdown [X]
 * Shutdown a connection to a Postgres database
 */
bool pg_shutdown (CONN * conn)
{
__STACK(pg_shutdown)
	
	pgout(mDEBUG3, "disconnecting");
    PQfinish(conn->pgconn);
    
	__return true;
	
} /* pg_shutdown */


/*
 * pg_print_tuples
 * Print a grid of the tuples (debugging purposes only)
 */
static void pg_print_tuples(FILE *fd, RES *res)
{
	int rows = res->tuples;
	int cols = res->nfields;
	int x, y;
	int collen[1024];
	char *rowsep = NULL;
	
__STACK(pg_print_tuples)
	
	if (! rows) {
		__return;
	}
	
	// Just for testing... TODO: remove
	cols = (cols < 8) ? cols : 8;
	
	memset(&collen, 0, 1024);
	
	// Get collen based on PQfname
	for (x=0; x < cols; x++) {
		collen[x] = strlen(PQfname(res->pgres, x));
	}
	
	// Each row
	for (y=0; y < rows; y++) {
		// Iterate the cols
		for (x=0; x < cols; x++) {
			int len = PQgetlength(res->pgres, y, x);
			collen[x] = (len > collen[x]) ? len : collen[x];				
		}
	}
	
	// Create the row separator
	str_append(&rowsep, "%s", color_magenta);
	for (x=0; x < cols; x++) {
		str_append(&rowsep, "%.*s+"
			,collen[x]
			,"----------------------------------------"
			);
	}
	str_append(&rowsep, "%s\n", color_normal);
		
	fprintf(fd, rowsep);
	
	// Print header rows
	for (x=0; x < cols; x++) {
		fprintf(fd, "%-*s%s|%s"
			,collen[x]
			,PQfname(res->pgres, x)
			,color_magenta
			,color_normal
			);		
	}
	
	fprintf(fd, "\n%s", rowsep);
	
	// For each row
	for (y=0; y < rows; y++) {
		// Print the data
		for (x=0; x < cols; x++) {
			fprintf(fd, "%-*s%s|%s"
				,collen[x]
				,PQgetvalue(res->pgres, y, x)
				,color_magenta
				,color_normal
				);	
		}
		fprintf(fd, "\n%s", rowsep); 
	}
		
	str_free(&rowsep);
	
	__return;
	
} /* pg_print_tuples */


/*
 * pg_exec
 * Execute a query on a postgres database
 */
RES * pg_exec (CONN * conn, char * sql)
{
	RES *res = NULL;
	ExecStatusType pgstatus;

__STACK(pg_exec)

	if (PGIsamOptions & PrintOnly) {
		fprintf(stdout, "%s\n", sql);
		__return (RES *)NULL;		
	}
	
	res = (RES *)xalloc(sizeof(RES));
	
	// Store the last_sql global
	str_free(&last_sql);	
	str_append(&last_sql, "%s;", sql);
	
	res->pgres = PQexec(conn->pgconn, sql);
	
	pgstatus = PQresultStatus(res->pgres);

	// Get the number of tuples in the result
	res->tuples = PQntuples(res->pgres);
	
	// Get the number of fields in the result
	res->nfields = PQnfields(res->pgres);
	
	// Print SQL debugging info
	if (pgout_get_sql_print()) {
		fprintf(stderr, "> %s%s%s\n", color_yellow, sql, color_normal);
		pg_print_tuples(stderr, res);
	}
	
	if (pgstatus != PGRES_COMMAND_OK && pgstatus != PGRES_TUPLES_OK) {
		pg_msg(conn, 0, "%s", sql);
		RES_delete(&res);
		__return (RES *)NULL;
	}
	
	pgout(mDEBUG2, "sql=[%s] tuples=[%d]",
		sql, res->tuples);
	
	__return res;
	
} /* pg_exec */
