/* 
 * pgbridge.c: bridge routines between C-ISAM and PostgreSQL APIs
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <libpq-fe.h>

#include "pgisam.h"
#include "iserrcodes.h"
#include "sys.h"
#include "pgbridge.h"
#include "schema.h"
#include "xstring.h"
#include "pgres.h"

#define MAXBUFSZ 1024
#define ISAM_TRUE 0
#define ISAM_FALSE -1

pgisam_opt PGIsamOptions = PGIsamNormal;

// External data
// For compatibility with C-ISAM or other bridge functionality
bool suppress_error = false;
bool initialized = false;
extern char *last_sql;					// in pgres.c
int iserrno = 0;

SCHEMA *hSchema = NULL;
CONTEXT *hContext = NULL;
char *envEDATA = NULL;
char *envBRIDGE = NULL;

// Static data

/*
 */
static CONN *conn_default = NULL;		// The conn resource used by reads


// Static function prototypes
static int ISERR (int errcode, bool logmsg);
static char *build_select_stmt (INDEX * i, CONTEXT * cx, char * record, int mode);
static char *get_mode (int mode);


// CODE STARTS HERE
/*
 * get_EDATA|BRIDGE
 * "Get" methods for retreiving the
 * EDATA and BRIDGE environment variables.
 * (cannot be changed at runtime)
 */
char * get_EDATA(void)
{
__STACK(get_EDATA);

	__return envEDATA;
	
} /* get_EDATA */

char * get_BRIDGE(void)
{
__STACK(get_BRIDGE)

	__return envBRIDGE;
	
} /* get_BRIDGE */

/*
 * ISERR [X]
 * Sets C-ISAM error codes and proper returns
 */
static int ISERR (int errcode, bool logmsg)
{
	int x = 0;
	char *description = NULL;

__STACK(ISERR)
	
	if (errcode == 1 || errcode == 0) {
		iserrno = errcode;
		__return errcode;
	}
	
	while (iserrlist[x++].errcode) {
		if (errcode == iserrlist[x].errcode) {
			// Set iserrno
			iserrno = errcode >= 100 ? (errcode - 100) : errcode;
			
			// Set ptr to description
			description = iserrlist[x].description;
			
			break;
		}
	}
	
	if (! description) {
		iserrno = 999;
		description = "Unknown";
	}
	
	if (logmsg) {
		pgout(0, "%s", description);
	} else {
		pgout(mDEBUG3, "%s (errcode=%d)", description, errcode);
	}
	
	__return (-1);
	
} /* ISERR */


/*
 * init_program [X]
 * Initialize a Postgres connection
 */
bool init_program (void)
{
	FILE *fd;
	char *preload_def = NULL;
	char BUF[MAXBUFSZ];
	
__STACK(init_program)
	
	// Don't init unless required
	if (initialized) {
		__return false;
	}
	
	pgout(mDEBUG1, "initializing");
	
	// Check for required environment variables
	if (getenv("EDATA") == (char *)NULL) {
		pgout(mDISPLAY, "EDATA is a required environment variable");
		__return false;
	}	
	envEDATA = getenv("EDATA");
	
	if (getenv("BRIDGE") == (char *)NULL) {
		pgout(mDISPLAY, "BRIDGE is a required environment variable");
		__return false;
	}	
	envBRIDGE = getenv("BRIDGE");

	// Open the default conn
	pgout(mDEBUG3, "opening default PG conn");	
	if ((conn_default = CONN_new ()) == (CONN *)NULL) {
		pgout(0, "failed to create conn_default");
		__return false;
	}
	
	str_append(&preload_def
		,"%s/preload.def"
		,get_BRIDGE()
		);
	
	// Look for and process schemas in preload.def
	pgout(mDEBUG3, "opening preload.def");
	fd = fopen(preload_def, "r");
	
	if (! fd) {
		pgout(mSYS, "fopen failed for [%s]", preload_def);
		__return false;
	}
	
	while (fgets(BUF, MAXBUFSZ, fd) != (char *)NULL) {
		if (BUF[0] == '#' || BUF[0] == '\n'|| BUF[0] == '\r') continue;
		
		if (BUF[strlen(BUF)-1] == '\n') {
			BUF[strlen(BUF)-1] = '\0';
			if (BUF[strlen(BUF)-1] == '\r') {
				BUF[strlen(BUF)-1] = '\0';
			}
		}
		
		pgout(mDEBUG2, "preloading %s definition", BUF);
			
		// Add the schema definition
		SCHEMA_push(&hSchema, BUF);
	}
		
	fclose(fd);
	
	__return initialized = true;
	
} /* init_program */


/* set_pgisam_options
 * Set overridable runtime options for PG-ISAM
 * 
 * Options (separate with comma):
 * printonly	Do not execute SQL; print to stdout
 */
void set_pgisam_options (char *optstr)
{
	if (!strcmp(optstr, "printonly")) {
		PGIsamOptions = PGIsamOptions ^ PrintOnly;
	}
	
	
} /* set_pgisam_options */


/*
 * shutdown_program [X]
 * Shutdown a Postgres connection
 */
bool shutdown_program (void)
{
__STACK(shutdown_program)

	if (! initialized) {
		pgout(0, "initialized");
		__return false;
	}
	
	pgout(mDTSTAMP|mDEBUG1, "shutting down");
	
	// Delete all open contexts	
	CONTEXT_delete(&hContext);
	
	// Delete the connection object
	CONN_delete(conn_default);
	
	// Delete the global schema
	SCHEMA_delete(&hSchema);
	
	// Delete stack local to schema module
	SCHEMA_shutdown();
	
	initialized = false;
	
	__return true;
	
} /* shutdown_program */


/*
 * get_last_sql:
 * Get the last SQL statement executed by PGISAM
 * (in pgbridge.c)
 */
char * get_last_sql (void)
{
__STACK(get_last_sql)

	__return last_sql;
	
} /* get_last_sql */


/*
 * x_isaddindex [X]
 * Add an index to a C-ISAM file
 * isfd		file desccriptor returned by isopen or isbuild
 * key		pointer to a key description struct
 * 
 * NOTE: indexes used to be added in isbuild
 */
int x_isaddindex (int isfd, struct keydesc * key)
{
__STACK(x_isaddindex)

	pgout(mDEBUG3, "x_isaddindex (does nothing)");
	
	__return ISAM_TRUE;
	
} /* x_isaddindex */


/*
 * x_isbegin [X]
 * Defines the beginning of a transaction
 */
int x_isbegin (void)
{
	int ret;
	
__STACK(x_isbegin)
	
	pgout(mDEBUG3, "transaction started");
	
	ret = CONN_begin(conn_default);
	
	conn_default->in_transaction = true;
	
	if (ret < 0) {
		__return ISERR(122, true); // 122 = no transaction
	} else {
		__return ISAM_TRUE;
	}
	
} /* x_isbegin */


/*
 * x_isbuild [X]
 * Create a C-ISAM file
 * filename	name of the file w/out an extension
 * reclen	length of the record in bytes		** IGNORED **
 * keydesc	ptr to the key description struct of primary key
 * mode		access mode							** IGNORED **
 * 
 * NOTE: isbuild will primary key
 */
int x_isbuild (char *filename, int reclen, struct keydesc *key, int mode)
{
	char *sql = NULL;
	char *basename;
	SCHEMA *s;
	COLUMN *c;
	MODIFY *m;
	INDEX *i;
	RES *res = NULL;
	char *rptmp = NULL;
	
__STACK(x_isbuild)
	
	pgout(mDEBUG3, "filename=[%s] reclen=[%d] mode=%d",
		filename, reclen, mode);

	basename = strrchr(filename, '/');
	
	// Attempt to add the schema definition
	SCHEMA_push(&hSchema, basename ? (++basename) : filename);
	
	// Get a new pointer in case it existed already
	s = SCHEMA_get(hSchema, basename ? basename : filename);
	
	// Associate the context with a schema
	// and push it on to the stack
	CONTEXT_push(&hContext, s);
	
	// Associate the context with a CONN
	hContext->conn = conn_default;
	
	// Don't actually build the table if "nocreate" is set in the schema
	if (s->nocreate) {
		__return hContext->isfd;
	}
	
	if (! strcmp(s->name, "rptmp")) {
		str_append(&rptmp, "%s", &s->pgname[4]);
	}	
	
	// Build the sql statement
	str_append(&sql,
		"CREATE %sTABLE %s ( "
		"oid SERIAL UNIQUE PRIMARY KEY, "
		"phantom BOOLEAN NOT NULL DEFAULT false, "
		,rptmp ? "TEMP " : ""
		,s->pgname
		);
	
	// Iterate through the table's coldef to build the SQL statement
	c = s->column;
	while (c) {
		
		str_append(&sql,
			"%s "
			,c->name
			);
		
		switch (c->datatype) {
			
			case ISAM_TYPE_CHAR:
			str_append(&sql, "VARCHAR(%d)", c->length);
			break;
		
			case ISAM_TYPE_DECIMAL:
			str_append(&sql, "NUMERIC");
			break;
			
			case ISAM_TYPE_CODE:
			str_append(&sql, "CHAR(%d)", 
				c->codelength ? c->codelength : c->length);
			break;
		
			case ISAM_TYPE_BINARY:
			str_append(&sql, "BYTEA");
			break;
			
			case ISAM_TYPE_INTEGER:
			str_append(&sql, "INTEGER");
			break;
			
			case ISAM_TYPE_BOOLEAN:
			str_append(&sql, "BOOLEAN");
			break;
		
			default:
			str_append(&sql, "VARCHAR(%d)", c->length);
		}

		if (c->params) {
			str_append(&sql, " %s,", c->params);
		} else {
			str_append(&sql, ",");
		}

		c = c->next;
	}
	
	// Pick off the last comma
	str_trim_char(&sql, ',');
			
	str_append(&sql,
		") WITHOUT OIDS"
		);
	
	res = pg_exec(hContext->conn, sql);
	
	str_free(&sql);
	
	if (! res) {
		__return ISERR(101, true); // 101 = file not open
	}

	RES_delete(&res);
	
	// Iterate through and exec the table's modifiers
	m = s->modify;
	while (m) {

		res = pg_exec(hContext->conn, m->definition);
		
		if (! res) {
			__return ISERR(101, true); // 101 = file not open
		}
		
		RES_delete(&res);
		
		m = m->next;
	}
	
	// Build the indexes
	// Retrieve the index by keydesc (primary key)
	i = s->index;
	while (i) {
		char *sql_index = NULL;
		
		str_append(&sql_index,
			"CREATE %sINDEX %s ON %s ( "
			,i->is_unique ? "UNIQUE " : ""
			,rptmp ? rptmp : i->name
			,s->pgname
			);

		// Iterate through the column names in index
		c = i->column;
		while (c) {
			str_append(&sql_index,
				"%s,"
				,c->name
				);
					
			c = c->next;
		}
		
		// Strip off trailing comma
		if (sql_index[strlen(sql_index)-1] == ',') {
			sql_index[strlen(sql_index)-1] = '\0';
		}
		
		str_append(&sql_index, " )");
		
		res = NULL;
		res = pg_exec(hContext->conn, sql_index);
		
		if (! res) {
			str_free(&sql_index);
			str_free(&rptmp);
			__return ISERR(101, true); // 101 = file not open
		}
		
		RES_delete(&res);

		str_free(&sql_index);
		
		i = i->next;
	}
	
	str_free(&rptmp);
	
	// Return the context's file descriptor
	__return hContext->isfd;
	
} /* x_isbuild */


/*
 * x_iscleanup [X]
 * Closes all C-ISAM files opened by the program
 */
int x_iscleanup (void)
{
__STACK(x_iscleanup)

	pgout(mDEBUG3, "deleting all contexts");
	
	// Close all contexts
	CONTEXT_delete(&hContext);
	
	__return ISAM_TRUE;
	
} /* x_iscleanup */


/*
 * x_isclose [X]
 * Closes a C-ISAM file
 * isfd		File descriptor
 * 
 * NOTE: must delete the context associated with the isfd
 */
int x_isclose (int isfd)
{
	CONTEXT *cx;

__STACK(x_isclose)
	
	// Find the context
	cx = CONTEXT_get(hContext, isfd);
	
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);

	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}
	
	CONTEXT_delete_node(&hContext, cx);
	
	__return ISAM_TRUE;
	
} /* x_isclose */


/*
 * x_iscommit [X]
 * Ends a transaction and releases all locks
 */
int x_iscommit (void)
{
	int ret;
	CONTEXT *cx = hContext;

__STACK(x_iscommit)

	pgout(mDEBUG3, "committing transaction");

	ret = CONN_commit(conn_default);
	
	conn_default->in_transaction = false;
	
	/*
	 * Here we iterate through all of the contexts.
	 * We are looking for open contexts which are declared "WITHOUT HOLD"
	 * This is determined by cx->trans_cursor
	 * We must close these...
	 */
	while (cx) {
		
		// First, is the context already in a cursor?
		if (cx->cursor_name && cx->trans_cursor) {
		
			// Indicates that the cursor is "closed"
			str_free(&cx->cursor_name);
			cx->trans_cursor = false;
		}
	
		cx = cx->next;
	}
		
	if (ret < 0) {
		__return ISERR(122, true); // 122 = no transaction
	} else {
		__return ISAM_TRUE;
	}
		
} /* x_iscommit */

/*
 * x_isdelcurr
 * Deletes the current record from the C-ISAM file
 * isfd		file descriptor
 */
int x_isdelcurr (int isfd)
{
	CONTEXT *cx = NULL;
	RES *res;
	RES *res_curr;
	char *sql = NULL;
	char *oid = NULL;
	bool ret = ISAM_TRUE;
	
__STACK(x_isdelcurr)
		
	// Obtain the context by file descriptor
	cx = CONTEXT_get(hContext, isfd);
	
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);
	
	// Success?
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}
	
	// Must be incursor
	if (! cx->cursor_name) {
		__return ISERR(112, true); // 112 = no current record
	}

	// Exec the SQL statement prepared by isstart
	// Why? Because it will contain the current state of
	// the record
	str_append(&sql, "FETCH FORWARD 0 FROM %s", cx->cursor_name);
		
	res_curr = pg_exec(cx->conn, sql);
	str_free(&sql);
		
	// Better have a result or err
	if (! res_curr) {
		__return ISERR(112, true); // 112 = no current record
	}
	
	// Obtain the OID of the current record
	RES_get_oid(res_curr, &oid);

	// That's all we need it for
	RES_delete(&res_curr);
	
	// Check to make sure we were able to get the oid
	if (! oid) {
		__return ISERR(111, true); // 111 = no record found
	}
	
	// Create the delete statement	
	str_append(&sql,
		"DELETE FROM %s WHERE oid='%s'"
		, cx->schema->pgname
		, oid
		);

	res = pg_exec(cx->conn, sql);
	str_free(&sql);
		
	if (! res) {
		ret = ISERR(111, false); // 111 = no record found
	} else {
		RES_delete(&res);
	}
	
	__return ret;
	
} /* x_isdelcurr */


/*
 * x_isdelete
 * Deletes a record using the primary key
 * isfd		file descriptor
 * record	contains a key value in the position defined for the primary key
 */
int x_isdelete (int isfd, char * record)
{
	COLUMN *c;
	CONTEXT *cx = NULL;
	RES *res;
	char *sql = NULL;
	char *sql_where = NULL;
	bool first_clause = true;
	
__STACK(x_isdelete)
	
	// Obtain the context by file descriptor
	cx = CONTEXT_get(hContext, isfd);
	
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);
	
	// Success?
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}
	
	COLUMN_from_record(cx->schema->column, record);
	
	c = cx->schema->column;
	
	while (c) {
			
		if (c->value) {
			if (c->datatype == ISAM_TYPE_BOOLEAN && (! strcmp(c->value,
				"null"))) {
				// TODO: determin whether we need to handle this
			} else {
				str_append(&sql_where,
					"%s%s='%s'"
					,first_clause ? " " : " AND "
					,c->name
					,c->value
					);
			}
					
			first_clause = first_clause ? false : true;
		}
			
		c = c->next;
	}
	
	// Create the delete statement	
	str_append(&sql,
		"DELETE FROM %s WHERE%s"
		, cx->schema->pgname
		, sql_where
		);

	res = pg_exec(cx->conn, sql);
	str_free(&sql);
	str_free(&sql_where);
	
	// Clean the COLUMN
	COLUMN_clean(cx->schema->column);
			
	if (! res) {
		__return ISERR(111, false); // 111 = no record found
	} else {
		RES_delete(&res);
	}
	
	__return ISAM_TRUE;
	
} /* x_isdelete */


/*
 * x_isdelindex
 * Removes an entire index
 * isfd		file descriptor
 * keydesc	ptr to a key description struct
 * 
 * NOTE: not required so it's ignored
 */
int x_isdelindex (int isfd, struct keydesc * keydesc)
{
__STACK(x_isdelindex)

	pgout(mDEBUG3, "isfd=%d", isfd);

	__return ISAM_TRUE;
	
} /* x_isdelindex */


/*
 * x_isdelrec
 * Deletes a record using the record number
 * isfd		file descriptor
 * recnum	record number of the data file record
 */
int x_isdelrec (int isfd, long recnum)
{
__STACK(x_isdelrec)

	pgout(mDEBUG3, "isfd=%d", isfd);

	__return ISAM_TRUE;
	
} /* x_isdelrec */


/*
 * x_iserase [X]
 * Removes the operating system files comprising the C-ISAM file
 * filename	the C-ISAM file to delete
 */
int x_iserase (char * filename)
{
	char *sql = NULL;
	char *basename;
	RES *res = NULL;
	SCHEMA *s = NULL;
	int ret;
	
__STACK(x_iserase)

	pgout(mDEBUG3, "filename=[%s]", filename);

	basename = strrchr(filename, '/');
	
	// Attempt to add the schema definition
	s = SCHEMA_get(hSchema, basename ? (++basename) : filename);
	
	if (! s) {
		__return ISERR(900, true); // 900 = no schema definition
	}
	
	// Don't drop the table if "nocreate" is set in the schema
	if (s->nocreate) {
		__return ISAM_TRUE;
	}
	
	// Build the sql statement
	/* NOTE: added "CASCADE"
	 * Although there are no constraints in PG-ISAM
	 * triggers and constraints may be added after-the-fact
	 * by other processes (i.e. phantom history columns/triggers).
	 */
	str_append(&sql,
		"DROP TABLE %s CASCADE"
		,s->pgname
		);
	
	// Since we are not associated w/a context here,
	// act on the default conn
	res = pg_exec(conn_default, sql);
	str_free(&sql);
	
	if (! res) {
		__return ISERR(-1, false);
	} else {
		RES_delete(&res);
	}
	
	__return ISAM_TRUE;
	
} /* x_iserase */


/*
 * x_isindexinfo
 * Determines information about the structure and indexes of a C-ISAM file
 * isfd		file descriptor
 * buffer	ptr to a struct (keydesc | dictinfo)
 * number	an index number or zero
 */
int x_isindexinfo (int isfd, struct keydesc * buffer, int number)
{
__STACK(x_isindexinfo)

	pgout(mDEBUG3, "isfd=%d", isfd);

	__return ISAM_TRUE;
	
} /* x_isindexinfo */


/*
 * x_islogclose
 * Closes the transaction log file
 * 
 * NOTE: bridge always returns true,
 * function not required by Postgres
 */
int x_islogclose (void)
{
__STACK(x_islogclose)

	pgout(mDEBUG3, "closing log files");

	__return ISAM_TRUE;
	
} /* x_islogclose */


/*
 * x_islogopen
 * Opens the transaction log file
 * logname	pointer to the filename string
 * 
 * NOTE: bridge always returns true,
 * function not required by Postgres
 * 
 */
int x_islogopen(char * logname)
{
__STACK(x_islogopen)

	pgout(mDEBUG3, "opening log files");

	__return ISAM_TRUE;
	
} /* x_islogopen */


/*
 * x_isopen [X]
 * Opens a C-ISAM file for processing
 * filename	the name of the file
 * mode		mode
 * 
 * NOTE: must return the correct schema isfd;
 */
int x_isopen (char * filename, int mode)
{
	char *basename;
	SCHEMA *s = NULL;
	
__STACK(x_isopen)
	
	pgout(mDEBUG3, "filename=[%s] mode=[%d]",
		filename, mode);
	
	basename = strrchr(filename, '/');
	
	// Find the schema matching filename
	s = SCHEMA_get(hSchema, basename ? (++basename) : filename);
	
	if (! s) {
		__return ISERR(102, true); // illegal argument
	}
	
	// Create a new context and associate it with the schema
	CONTEXT_push(&hContext, s);
	
	// Associate the context with the appropriate connection
	hContext->conn = conn_default;
	
	__return hContext->isfd;
	
} /* x_isopen */


/*
 * x_isread [X]
 * Read records sequentially or randomly
 * isfd		file descriptor
 * record	pointer to string containing the search val, and receives the record
 * mode		mode
 */
int x_isread (int isfd, char * record, int mode)
{
	CONTEXT *cx;
	RES *res;
	char *sql = NULL;
	char *direction = NULL;
	
__STACK(x_isread)
	
	// Find the context
	cx = CONTEXT_get(hContext, isfd);

	pgout(mDEBUG3, "schema=[%s] mode=[%s]",
		cx->schema->name, get_mode(mode));
			
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}

	// If there is no cursor
	if (! cx->cursor_name) {
		
		// Allow for an isread to occur on the default index, without an isstart
		if (mode & ISEQUAL || mode & ISGTEQ) {
			INDEX *i;

			// Pivot to correct table if targeting isam file "tables"
			if (cx->schema->is_pivotable) {
				SCHEMA *s;
				
				s = SCHEMA_pivot(hSchema, record);
				if (s) {
					cx->schema = s;
				}
			}
			
			// Get the first index
			i = INDEX_get(cx->schema->index, 1);
			if (! i) {
				__return ISERR(124, true); // 124 = no begin work yet
			}
			
			// Build the select stmt... strip mode of other masks
			sql = build_select_stmt(
				i,
				cx,
				record,
				(mode & ISEQUAL) ? ISEQUAL : ISGTEQ
				);

			// Only get one record
			str_append(&sql, " LIMIT 1");
			
			goto execsql;

		} else {
			// In all other cases, a cursor is required
			__return ISERR(124, true); // 124 = no begin work yet
		}
	}

	// isread has "lock modes" that do not apply to pgisam
	// Those are stripped here
	mode = (mode & ISLOCK) ? (mode - ISLOCK) : mode;
	mode = (mode & ISSKIPLOCK) ? (mode - ISSKIPLOCK) : mode;
	mode = (mode & ISWAIT) ? (mode - ISWAIT) : mode;
	mode = (mode & ISLCKW) ? (mode - ISLCKW) : mode;
	
	if (cx->reverse_direction) {
		switch (mode) {
			case ISPREV:
			mode=ISNEXT;
			break;
			
			case ISNEXT:
			mode=ISPREV;
			break;
		}
	}
	
	switch (mode) {
		case ISFIRST:
		direction = "FIRST";
		break;
	
		case ISLAST:
		direction = "LAST";
		break;
		
		
		/* This appears to be where the problem is occurring.
		 * Possibly reverse the direction of the cursor?
		 */
		
		case ISPREV:
		if ((! cx->in_read) && (cx->mode == ISLAST)) {
			direction = "LAST";
		} else {
			direction = "BACKWARD 1";
		}
		break;
		
		case ISNEXT:
		// Deals w/special-case behavior in C-ISAM
		if ((cx->mode == ISGREAT) && (! cx->special_case)) {
			char *sql_temp = NULL;
			char *part1, *part2;
			RES *tmpres;
			
			// First, close the current cursor
			str_append(&sql_temp,
				"CLOSE %s"
				,cx->cursor_name);

				
			if ((tmpres = pg_exec(cx->conn, sql_temp)) == (RES *)NULL) {
				pgout(0, "unable to close cursor [%s]",
					cx->cursor_name);
			}
			
			RES_delete(&tmpres);
			str_free(&sql_temp);
			
			// Build the new cursor declaration from the previous
			// in addition, add the additional WHERE clause
			part1 = cx->sql_last;
			
			part2 = strstr(cx->sql_last, " ORDER BY ");
			
			*part2 = '\0'; *part2++;

			str_append(&sql_temp, "%s%s %s", part1, cx->sql_temp, part2);
			
			// Re-open the cursor
			if ((tmpres = pg_exec(cx->conn, sql_temp)) == (RES *)NULL) {
				pgout(0, "unable to close cursor [%s]",
					cx->cursor_name);
			}
			
			RES_delete(&tmpres);
			
			cx->special_case = true;

			str_free(&sql_temp);
		}
		
		direction = "FORWARD 1";
		break;
		
		default:
		// ISCURR || ISGREAT || ISGTEQ || ISEQUAL
		// fetch the current record
		direction = "FORWARD 1";
	}

	str_append(&sql,
		"FETCH %s FROM %s"
		,direction
		,cx->cursor_name
		);

execsql:
	res = pg_exec(cx->conn, sql);
	str_free(&sql);

	if (! res) {
		__return ISERR(111, false); // 111 = no record found
	}
		
	if (res->tuples != 1) {
		RES_delete(&res);
		__return ISERR(111, false); // 111 = no record found
	}
	
	// Obtain the OID of the current record
	RES_get_oid(res, &cx->oid_last);

	// Context has had a successful read
	cx->in_read = true;

	// Fill columns from resource
	COLUMN_from_res(&cx->schema->column, res);
	
	// Fill the record with spaces (only on a successful read/fetch)
	memset(record, 0x20, cx->schema->reclen);
	
	// Fill record from columns
	COLUMN_to_record(cx->schema->column, &record);

	// Clean it
	COLUMN_clean(cx->schema->column);

	RES_delete(&res);

	__return ISAM_TRUE;

} /* x_isread */


/*
 * x_isrelease [X]
 * Unlock records that are locked by calls to isread
 * isfd		file desccriptor returned by isopen or isbuild
 * 
 * NOTE: not meaningful in PG-ISAM
 */
int x_isrelease (int isfd)
{
__STACK(x_isrelease)

	pgout(mDEBUG3, "isfd=%d", isfd);

	__return ISAM_TRUE;
	
} /* x_isrelease */


/*
 * x_isrewcurr
 * Modifies or updates fields in the current record
 * isfd		file descriptor
 * record	pointer to string containing the search val, and receives the record
 * 
 * NOTE: this routine closely mirrors an "UPDATE" sql stmt
 */
int x_isrewcurr (int isfd, char * record)
{
	CONTEXT *cx = NULL;
	RES *res;
	char *sql = NULL;
	bool ret = ISAM_TRUE;
	
__STACK(x_isrewcurr)
	
	// Obtain the context by file descriptor
	cx = CONTEXT_get(hContext, isfd);
	
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);
	
	// Success?
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}
	
	// Check to make sure we were able to get the oid
	if (! cx->oid_last) {
		__return ISERR(111, true); // 111 = no record found
	}
	
	// Fill column values from record
	COLUMN_from_record(cx->schema->column, record);

	// Create the update statement	
	sql = SCHEMA_create_update(cx);

	res = pg_exec(cx->conn, sql);
	str_free(&sql);
		
	if (! res) {
		ret = ISERR(111, false); // 111 = no record found
	} else {
		RES_delete(&res);
	}
		
	// Clean the COLUMN
	COLUMN_clean(cx->schema->column);
	
	__return ret;
	
} /* x_isrewcurr */


/*
 * x_isrewrec
 * Updates record identified by its record number
 * isfd		file descriptor
 * recnum	the record number
 * record	pointer to string containing the search val, and receives the record
 */
int x_isrewrec (int isfd, long recnum, char * record)
{
__STACK(x_isrewcur)

	pgout(mDEBUG3, "isfd=%d", isfd);

	__return ISAM_TRUE;
	
} /* x_isrewrec */


/*
 * x_isrewrite
 * Rewrite the nonprimary key fields of a record
 * isfd		file descriptor
 * record	pointer to string containing the search val, and receives the record
 * 
 * ++ The behavior of this function differs from the C-ISAM specs in that
 * it will allow overwriting the primary key.
 */
int x_isrewrite (int isfd, char * record)
{
__STACK(x_isrewrite)

	pgout(mDEBUG3, "isfd=%d", isfd);

	__return x_isrewcurr(isfd, record);
		
} /* x_isrewrite */


/*
 * x_isrollback [X]
 * Cancel the effect of C-ISAM calls since x_isbegin
 */
int x_isrollback (void)
{
	int ret;
	CONTEXT *cx = hContext;

__STACK(x_isrollback)
	
	pgout(mDEBUG3, "rolling back transaction");
	
	ret = CONN_rollback(conn_default);
	
	conn_default->in_transaction = false;
	
	/*
	 * Here we iterate through all of the contexts.
	 * We are looking for open contexts which are declared "WITHOUT HOLD"
	 * This is determined by cx->trans_cursor
	 * We must close these...
	 */
	while (cx) {
		
		// First, is the context already in a cursor?
		if (cx->cursor_name && cx->trans_cursor) {
			
			// Indicates that the cursor is "closed"
			str_free(&cx->cursor_name);
			cx->trans_cursor = false;
		}
	
		cx = cx->next;
	}
		
	if (ret < 0) {
		__return ISERR(122, true); // 122 = no transaction
	} else {
		__return ISAM_TRUE;
	}
		
} /* x_isrollback */


/*
 * build_select_stmt [X]
 * Build a select statement on the current context, on the selected index
 * i		pointer to the selected index
 * cx		pointer to the current context
 * record	specifies the key search value
 * mode		mode
 */
static char * build_select_stmt(INDEX * i, CONTEXT * cx, char * record, int mode)
{
	char *sql = NULL;
	char *collation;
	COLUMN *c = NULL;
	bool z_values = false;
	bool where_clause_included = false;

__STACK(build_select_stmt)
			
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);

	/* -------------------------------------
	 * SQL statement
	 * -------------------------------------
	 */
	str_append(&sql,
		"SELECT * FROM %s"
		,cx->schema->pgname
		);


	/* -------------------------------------
	 * WHERE clause
	 * -------------------------------------
	 */
	if (cx->schema->column && (
		mode == ISEQUAL ||
		mode == ISGREAT ||
		mode == ISGTEQ
		)) {
		
		/*
		 * Since only index columns need to be in the "where" clause,
		 * get column from the index selected above.
		 */		
		COLUMN *valc;
		bool first_clause = true;
		char *sql_op;
		STMT *st = NULL, *stmt = NULL;

		valc = i->column;

		COLUMN_from_record(cx->schema->column, record);
		
		switch (mode) {
			case ISGREAT:
			sql_op = " > ";
			break;
			
			case ISGTEQ:
			sql_op = " >= ";
			break;
			
			default:
			sql_op = "=";
		}
		
		while (valc) {
			COLUMN *c_comp = NULL;

			// Index columns are just names, so get the real column
			c_comp = COLUMN_get(cx->schema->column, valc->name);
		
			// Make sure there was a match
			if (! c_comp) {
				pgout(0, "could not retrieve column matching index %s",
					i->name);
				str_free(&sql);
				__return (char *)NULL;
			}

			if (str_is_filled((char *)c_comp->value, 'z')) {
				z_values = true;
				goto skipit;
			}
			
			if ((mode == ISGTEQ) && ! c_comp->value) {
				goto skipit;				
			}

			STMT_append(&stmt, c_comp->name, sql_op, (char *)c_comp->value, c_comp->datatype);
		
		skipit:
			valc = valc->next;
		}
		
		// Compile the WHERE clause
		if (stmt) {
			st = stmt->head;
			str_append(&sql, " WHERE");
			where_clause_included = true;
		}
		
		while (st) {
			str_append(&sql,
				"%s%s%s'%s'"
				,first_clause ? " " : " AND "		// spacer or "AND"
				,st->column							// Column name
				,z_values ? "=" : st->operator		// operator
				,(char *)st->value ? (char *)st->value : ""
				);

			/* NOTE:
			 * You cannot use regexp's on binary fields.
			 * The following code solves the issue by:
			 * SELECT famnbr FROM ecn_hist WHERE
			 * 	encode(famnbr::bytea, 'escape'::text) !~ '^      0231';
			 */
			if (mode == ISGREAT) {
				str_append(&cx->sql_temp,
					" AND %s%s%s !~ '^%s'"
					,(st->datatype == ISAM_TYPE_BINARY) ? "encode(" : ""
					,st->column
					,(st->datatype == ISAM_TYPE_BINARY) ? "::bytea, 'escape'::text)" : ""
					,(char *)st->value ? (char *)st->value : ""
					);
			}
				
			first_clause = false;
						
			st = st->next;
		}
		
		// Pick off the last comma
		str_trim_char(&sql, ',');
		
		// Destroy the STMT object
		if (stmt) {
			STMT_delete(&stmt->head);
		}
			
		// Clean the COLUMN
		COLUMN_clean(cx->schema->column);
	}


	/* -------------------------------------
	 * Determine collation
	 * -------------------------------------
	 */
	if (z_values) {
		collation = " DESC";
		cx->reverse_direction = true;
	} else {
		collation = " ASC";
	}


	/* -------------------------------------
	 * ORDER BY clause
	 * -------------------------------------
	 */
	str_append(&sql,
		" %s phantom != true ORDER BY"
		,where_clause_included ? "AND" : "WHERE"
		);
	
	c = i->column;
	
	while (c) {
		str_append(&sql,
			" %s%s,"
			, c->name
			, collation
			);
				
		c = c->next;
	}
	
	// Pick off the last comma
	str_trim_char(&sql, ',');

	__return sql;
	
} /* build_select_stmt */


/*
 * get_mode [X]
 * Returns the string representation of the mode
 * mode		Holds the "ISAM" mode
 */
static char * get_mode (int mode)
{
	char *modestr = NULL;
	
__STACK(get_mode)

	// Strip off lock modes	
	if (mode & ISLOCK) {
		mode -= ISLOCK;
	} else
	if (mode & ISSKIPLOCK) {
		mode -= ISSKIPLOCK;
	} else
	if (mode & ISWAIT) {
		mode -= ISWAIT;
	} else
	if (mode & ISLCKW) {
		mode -= ISLCKW;
	} else
	if (mode & ISKEEPLOCK) {
		mode -= ISKEEPLOCK;
	}
	
	switch (mode) {
		case ISFIRST:
			modestr = "ISFIRST";
			break;
		case ISLAST:
			modestr = "ISLAST";
			break;
		case ISNEXT:
			modestr = "ISNEXT";
			break;
		case ISPREV:
			modestr = "ISPREV";
			break;
		case ISCURR:
			modestr = "ISCURR";
			break;
		case ISEQUAL:
			modestr = "ISEQUAL";
			break;
		case ISGREAT:
			modestr = "ISGREAT";
			break;
		case ISGTEQ:
			modestr = "ISGTEQ";
			break;
		default:
			modestr = "UNKNOWN";		
	}
	
	__return modestr;
	
} /* get_mode */


/*
 * x_isstart [X]
 * Select the index and starting point in the index for subsequent calls to v_isread
 * isfd		file descriptor
 * keydesc	pointer to a key description structure
 * length	part of the key considered significant when locating the starting record
 * record	specifies the key search value
 * mode		mode
 * 
 * NOTE: difficult because of its dual-nature, isstart is both for confirming that a
 * matching record exists, and for setting the record pointer (cursor) to that location.
 * 
 * Cursors:
 * Cursors are declared at the schema level. A check is first made to see if a cursor
 * is open already, if so it is closed and a new cursor is declared.
 * 
 * Modes:
 * ISFIRST	Finds the first record by positioning the starting point just before 
 * 			the first record.
 * ISLAST	Finds the last record by positioning the starting point just before
 * 			the last record.
 * ISEQUAL	Finds the record equal to the search value.
 * ISGREAT	Finds the record greater than the search value.
 * ISGTEQ	Finds the first record greater than of equal to the search value
 * ISKEEPLOCK	Causes isstart to keep locks held on any record in automatic
 * 				locking mode [IS NOT USED]
 *  
 */
int x_isstart (int isfd, struct keydesc * key, int length, char * record, int mode)
{
	CONTEXT *cx = NULL;
	INDEX *i = NULL;
	RES *res = NULL;
	char *sql_full = NULL;
	char *sql_select = NULL;
	bool WITH_HOLD = false;
	
__STACK(x_isstart)
	
	// Find the context
	cx = CONTEXT_get(hContext, isfd);

	pgout(mDEBUG3, "schema=[%s] mode=[%s]", cx->schema->name, get_mode(mode));
	
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}
	
	// Pivot to correct table if targeting isam file "tables"
	if (cx->schema->is_pivotable) {
		SCHEMA *s;
			
		s = SCHEMA_pivot(hSchema, record);
		if (s) {
			cx->schema = s;
		}
	}
	
	// Retreive the index matching keydesc
	i = INDEX_get_keydesc(cx->schema->index, key);
	
	if (! i) {
		__return ISERR(103, true); // 103 = illegal key desc
	}
	
	pgout(mDEBUG3, "selected index [%s]", i->name);
	
	// Save a copy of the index
	// See isrewrite()
	cx->index = i;

	/* -------------------------------------
	 * CURSOR preparation:
	 * isstart creates a new cursor in each invocation
	 * -------------------------------------
	 */
	// First, is the context already in a cursor?
	if (cx->cursor_name) {
		RES *tmpres;
		char *tmpsql = NULL;
		
		// If so, close it
		str_append(&tmpsql,
			"CLOSE %s"
			,cx->cursor_name);

		if ((tmpres = pg_exec(cx->conn, tmpsql)) == (RES *)NULL) {
			pgout(0, "unable to close cursor [%s]",
				cx->cursor_name);
		}

		RES_delete(&tmpres);
		str_free(&tmpsql);
	}


	/* -------------------------------------
	 * Data cleaning
	 * -------------------------------------
	 */	
	str_free(&cx->sql_last);
	str_free(&cx->sql_temp);
	str_free(&cx->cursor_name);	


	/* -------------------------------------
	 * Cursor declaration
	 * -------------------------------------
	 */	
	str_append(&cx->cursor_name,
		"%s_%ld_%d"
		,cx->schema->name
		,cx->id
		,getpid()
		);
		
	// Default direction
	cx->reverse_direction = false;
	
	// The connection we're pointed to in this context
	// determines whether a hold is placed on this cursor
	WITH_HOLD = (cx->conn->in_transaction) ? false : true;

	// Build the cursor declaration/select statment
	str_append(&sql_full,
		"DECLARE %s SCROLL CURSOR %s FOR "
		,cx->cursor_name
		,WITH_HOLD ? "WITH HOLD" : "WITHOUT HOLD"
		);
		

	/* -------------------------------------
	 * Build the full sql stmt
	 * -------------------------------------
	 */
	sql_select = build_select_stmt(i, cx, record, mode);
	
	if (! sql_select) {
		str_free(&sql_full);
		str_free(&cx->cursor_name);
		__return ISERR(111, true); // 111 = no matching record
	}
	
	if (WITH_HOLD) {
		CONN_begin(cx->conn);
		cx->trans_cursor = false;		// Remove the transactionable flag
	} else {
		cx->trans_cursor = true;		// Assign the transactionable flag
	}

	// Append the SQL stmt on the cursor declaration
	str_append(&sql_full, "%s", sql_select);
	str_free(&sql_select);


	/* -------------------------------------
	 * Exec the full sql stmt
	 * -------------------------------------
	 */
	res = pg_exec(cx->conn, sql_full);
		
	if (! res) {
		if (WITH_HOLD) {
			CONN_rollback(cx->conn);
		}
		str_free(&sql_full);
		__return ISERR(111, false); // 111 = no matching record
	}
	
	// _______ Everything after this line happens only IF cursor succeeds ________
	
	// Save a copy of sql_full in the context
	cx->sql_last = str_dup(sql_full);
	
	str_free(&sql_full);
	
	// Clear the special_case flag (see ISGREAT in isread)
	cx->special_case = false;
	
	// Tell the context about this call to isstart
	cx->in_read = false;
	cx->mode = mode;
	
	if (WITH_HOLD) {
		CONN_commit(cx->conn);
	}
	
	// Free the tuples
	RES_delete(&res);

	__return ISERR(ISAM_TRUE, false);
	
} /* x_isstart */


/*
 * x_isfinish [X]
 * Does not close a file descriptor, but closes cursors and may commit transactions
 * isfd		file descriptor
 */
int x_isfinish (int isfd)
{
	CONTEXT *cx;
	RES *res;
	char *sql = NULL;
	
__STACK(x_isfinish)
	
	// Find the context
	cx = CONTEXT_get(hContext, isfd);
	
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);
	
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}
	
	// If we're not in a cursor, we don't need to do anything
	if (! cx->cursor_name) {
		__return false;
	}
	
	str_append(&sql,
		"CLOSE %s"
		, cx->cursor_name);
				
	if ((res = pg_exec(cx->conn, sql)) == (RES *)NULL) {
		pgout(0, "unable to close cursor [%s]",
			cx->cursor_name);
		__return err;
	}
	
	RES_delete(&res);
		
	// Not in cursor anymore
	str_free(&cx->cursor_name);
	
	str_free(&sql);
	
	__return ISAM_TRUE;
	
} /* x_isfinish */


/*
 * x_iswrcurr [X]
 * Writes a record and makes it the current record
 * isfd		file descriptor
 * record	specifies the key search value
 */
int x_iswrcurr (int isfd, char * record)
{
	CONTEXT *cx = NULL;
	RES *res;
	char *sql = NULL;
	bool ret = ISAM_TRUE;
	
__STACK(x_iswrcurr)
	
	cx = CONTEXT_get(hContext, isfd);
	
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);
	
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}
	
	// Fill column values from record
	COLUMN_from_record(cx->schema->column, record);
	
	sql = SCHEMA_create_insert(cx);

	res = pg_exec(cx->conn, sql);
	if (! res) {
		ret = err;
	} else {
		RES_delete(&res);
	}
		
	// Clean the COLUMN
	COLUMN_clean(cx->schema->column);
	
	str_free(&sql);
	
	__return ret;
	
} /* x_iswrcurr */


/*
 * x_iswrite
 * Writes a unique record
 * isfd		file descriptor
 * record	specifies the key search value
 */
int x_iswrite (int isfd, char * record)
{
	CONTEXT *cx = NULL;
	RES *res;
	char *sql = NULL;
	bool ret = ISAM_TRUE;
	
__STACK(x_iswrite)

	cx = CONTEXT_get(hContext, isfd);
	
	pgout(mDEBUG3, "schema=[%s]", cx->schema->name);
	
	if (! cx) {
		__return ISERR(101, true); // 101 = file not open
	}

	// Fill column values from record
	COLUMN_from_record(cx->schema->column, record);
	
	sql = SCHEMA_create_insert(cx);
	res = pg_exec(cx->conn, sql);

	if (! res) {
		ret = err;
	} else {
		RES_delete(&res);
	}

	// Clean the COLUMN
	COLUMN_clean(cx->schema->column);

	str_free(&sql);
	
	__return ret;
	
} /* x_iswrite */
