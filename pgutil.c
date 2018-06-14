/*
 * pgutil.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <libpq-fe.h>

#include <isam.h>
#include <decimal.h>

#include "sys.h"
#include "pgbridge.h"
#include "schema.h"
#include "xstring.h"

#define CLONELISTFILE "clonelist.def"
#define MAXBUFSZ 1024
#define SZ_TABLEREC 257

// Externs
extern char *ffilename;
extern int DEBUG_LEVEL;
extern bool append_convert;

extern SCHEMA *hSchema;
extern CONTEXT *hContext;

static int printrec = 0;
static int lastlen = 0;
static bool DISABLE_COUNTER = false;
static bool DROP_TABLE = true;
static bool VERBOSE = false;

// Static function prototypes
static void cleanup (void);
static void exit_handler (int extstat);
static void signal_handler (int signo);
static void usage (void);
static bool clone_main (char * singlefile);
static bool schema_main (void);
static bool process_schema (char * schemaname);
static bool process_tables (void);
static bool process_single_table (char *table_type);
void print_recnum (long recnum);
static bool process_table_type (int isfd, int pgfd, struct keydesc * key, char * tbl_type);


// CODE STARTS HERE


/* cleanup
 */
static void cleanup (void)
{
	return;

} /* cleanup */


/* exit_handler
 */
static void exit_handler (int exstat)
{
	char *str_status;
	
	str_status = exstat ? "failure" : "success";
	
	pgout(mDTSTAMP, "exit %s [exstat=%d]", str_status, exstat);

	cleanup();
	exit(exstat);
	
} /* exit_handler */


/* signal_handler
 */
static void signal_handler(int signo)
{
	pgout(0,"signal %d received", signo);
	exit_handler(EXIT_FAILURE);
	
} /* signal_handler */


/* usage
 */
static void usage(void)
{
	fprintf(stderr, "\nPG-ISAM Utilty: pgutil [-cdnv?] action\n"
		"  Action    Usage                 Description\n"
		"  clone     clone <singlefile>    Clone a C-ISAM database to PG-ISAM\n"
		"  schema    schema                Print bridge schema to stdout\n"
		"\n"
		"  Options:\n"
		"  -c        Disable counter\n"
		"  -d        Print extra debugging info\n"
		"  -n        No 'DROP TABLE' statements processed\n"
		"  -v        Print verbose messages\n"
		"  -?        Print this message\n"
		);

} /* usage */


/* print_recnum
 */
void print_recnum(long recnum)
{
	char *tmp = NULL;
	int x;
	
	if (DISABLE_COUNTER) return;
	
	if (printrec < 8) {
		printrec++;
		return;
	} else
		printrec = 0;
	
	asprintf(&tmp, "%ld", recnum);
	
	for (x = 0; x < lastlen; x++) {
		printf("\b");
	}

	printf("%s", tmp);
	
	lastlen = strlen(tmp);
	
	free(tmp);
	
} /* print_recnum */


/* process_schema
 * Process an individual schema (in clonelist.txt)
 * schemaname		Name of the schema
 */
static bool process_schema (char * schemaname)
{
	char *isampath = NULL;
	char *record = NULL;
	SCHEMA *s = NULL;
	int io, isfd, pgfd;
	int trancount = 0;
	struct keydesc key;
	long recnum = 0;
	
	str_append(&isampath, "%s/%s", getenv("EDATA"), schemaname);
	
	// Drop the pgisam table
	if (DROP_TABLE) {
		io = x_iserase(schemaname);
	}
	
	// Get the PGISAM schema
	s = SCHEMA_get(hSchema, schemaname);
	if (! s) {
		pgout(0, "process_schema: could not locate schema for %s", schemaname);
		return false;
	}
	
	// Open the cisam table
	isfd = isopen(isampath, ISINOUT + ISMANULOCK);
	if (isfd < 0) {
		pgout(0, "could not open c-isam file [%s], iserrno=%d", isampath, iserrno);
		return false;
	}
	
	io = isindexinfo(isfd, &key, 1);
	if (io < 0) {
		pgout(0, "unable to retrieve index info for [%s]", isampath);
		return false;
	}
	
	// Create the pgisam table
	pgfd = x_isbuild(schemaname, s->reclen, &key, 0);
	if (pgfd < 0) {
		pgout(0, "could not create pgisam table [%s]", isampath);
		return false;
	}
	
	// Prepare the record
	record = (char *)xalloc(s->reclen);
	memset(record, 0x20, s->reclen);		
	
	io = isstart(isfd, &key, 1, record, ISFIRST);
	if (io < 0) {
		pgout(0, "isstart failed to position record pointer");
		return false;
	}

	lastlen = 0;
	printf("\tProcessing %s record # ", schemaname);
	
	while ((io = isread(isfd, record, ISNEXT)) >= 0) {
		
		if (trancount++ == 0) {
			x_isbegin();
		}
			
		print_recnum(recnum++);
		
		io = x_iswrite(pgfd, record);
		if (io < 0) {
			pgout(0, "x_iswrite failed: unable to write record to pgisam table");
		}
		
		if (trancount == 1000) {
			trancount = 0;
			x_iscommit();
		}
	}
	
	// Commit the last block
	if (trancount)
		x_iscommit();
	
	// Always print the last record #
	printrec = 10;
	print_recnum(recnum - 1);
	
	printf(" ... done.\n");
	
	// Close both resources
	x_isclose(pgfd);
	isclose(isfd);
	
	str_free(&record);
	str_free(&isampath);
	
	return true;
	
} /* process_schema */


/* process_table_type
 */
static bool process_table_type (int isfd, int pgfd, struct keydesc * key, char * tbl_type)
{
	int io;
	char *record = NULL;
	char schemaname[12];
	long recnum = 0;
	SCHEMA *s;
	
	if (! strlen(tbl_type)) {
		return true;
	}
	
	sprintf(schemaname, "tables_%c%c",
		tolower(tbl_type[0]), tolower(tbl_type[1]));

	// Get the PGISAM schema
	s = SCHEMA_get(hSchema, schemaname);
	if (! s) {
		strcpy(schemaname, "tables");
	
		s = SCHEMA_get(hSchema, schemaname);
		if (! s) {
			pgout(0, "process_table_type: failed to get schema for [%s]",
				schemaname);
			return false;
		}
		
	} else {
		// Drop the pgisam table
		if (DROP_TABLE) {
			io = x_iserase(schemaname);
		}
		
		// Create the pgisam table
		pgfd = x_isbuild(schemaname, s->reclen, NULL, 0);
		if (pgfd < 0) {
			pgout(0, "could not create pgisam table [%s]", schemaname);
			return false;
		}		
	}
	
	// Upper-case the table type
	str_to_upper(tbl_type);
	
	// Prepare the record
	record = (char *)xalloc(SZ_TABLEREC);
	memset(record, 0x20, SZ_TABLEREC);
	memcpy(record, tbl_type, 2);
	memcpy(&record[14], tbl_type, 2);

	io = isstart(isfd, key, 2, record, ISGTEQ);
	
	if (io < 0) {
		pgout(0, "isstart failed in process_table_type");
		return false;
	}
	
	lastlen = 0;
	printf("\tProcessing tables type %s record # ", tbl_type);

	while ((io = isread(isfd, record, ISNEXT)) >= 0) {
		
		if (memcmp(record, tbl_type, 2)) {
			break;
		}
		
		print_recnum(++recnum);
		
		io = x_iswrite(pgfd, record);
		if (io < 0) {
			pgout(0, "x_iswrite failed: unable to write record to pgisam table");
		}
				
	}
	
	printf(" ... done.\n");
	
	str_free(&record);
	
	return true;

} /* process_type_type */


/* process_tables
 * Process the tables file
 */
static bool process_tables (void)
{
	char *isampath = NULL;
	char record1[SZ_TABLEREC];
	char tbl_type[3];
	SCHEMA *s = NULL;
	int io, isfd1, isfd2, pgfd;
	struct keydesc key;
	
	// Drop the pgisam table
	if (DROP_TABLE) {
		io = x_iserase("tables");
	}
		
	// Re-create the pgisam table
	pgfd = x_isbuild("tables", SZ_TABLEREC, NULL, 0);
	if (pgfd < 0) {
		pgout(0, "could not create pgisam table");
		return false;
	}
	
	pgout(0, "process_tables: reading tables");

	str_append(&isampath, "%s/tables", getenv("EDATA"));
	
	// Open the first cisam table
	isfd1 = isopen(isampath, ISINOUT + ISMANULOCK);
	if (isfd1 < 0) {
		pgout(0, "could not open c-isam file [%s], iserrno=%d", isampath, iserrno);
		return false;
	}
	
	// Open the second cisam table
	isfd2 = isopen(isampath, ISINOUT + ISMANULOCK);
	if (isfd2 < 0) {
		pgout(0, "could not open c-isam file [%s], iserrno=%d", isampath, iserrno);
		return false;
	}
	
	io = isindexinfo(isfd1, &key, 1);
	if (io < 0) {
		pgout(0, "unable to retrieve index info for [%s]", isampath);
		return false;
	}
	
	// Prepare the record
	memset(&record1, 0x20, SZ_TABLEREC);
	
	io = isstart(isfd1, &key, 1, record1, ISFIRST);
	
	if (io < 0) {
		pgout(0, "isstart failed to position record pointer");
		return false;
	}

	memset(&tbl_type, 0x00, 3);
	
	while ((io = isread(isfd1, record1, ISNEXT)) >= 0) {
		
		if (strncmp(tbl_type, record1, 2)) {
			process_table_type(isfd2, pgfd, &key, tbl_type);
		}
	
		strncpy(tbl_type, record1, 2);
	
	}

	return true;
	
} /* process_tables */


/* process_single_table
 * Process a single table file
 */
static bool process_single_table (char *table_type)
{
	char *isampath = NULL;
	char tbl_type[3];
	SCHEMA *s = NULL;
	int io, isfd1, pgfd;
	struct keydesc key;
	
	pgout(0, "process_tables: reading tables");

	str_append(&isampath, "%s/tables", getenv("EDATA"));
	
	// Open the first cisam table
	isfd1 = isopen(isampath, ISINOUT + ISMANULOCK);
	if (isfd1 < 0) {
		pgout(0, "could not open c-isam file [%s], iserrno=%d", isampath, iserrno);
		return false;
	}
	
	io = isindexinfo(isfd1, &key, 1);
	if (io < 0) {
		pgout(0, "unable to retrieve index info for [%s]", isampath);
		return false;
	}
	
	process_table_type(isfd1, pgfd, &key, table_type);
		
	return true;
	
} /* process_single_table */


/* clone_main
 * Main clone routine
 */
static bool clone_main (char * singlefile)
{
	FILE *fd;
	char BUF[MAXBUFSZ];
	char *clonelist_path = NULL;
	
	if (singlefile) {
		if (! strncmp(singlefile, "tables_", 7) && strlen(singlefile) == 9) {
			process_single_table(&singlefile[7]);
		} else
		if (! strcmp(singlefile, "tables")) {
			process_tables();
		} else {
			process_schema(singlefile);
		}
		return true;
	}
	
	str_append(&clonelist_path,
		"%s/%s"
		,get_BRIDGE()
		,CLONELISTFILE
		);

	// Open the clone file
	fd = fopen(clonelist_path, "r");
	
	if (! fd) {
		pgout(mSYS, "cannot open %s", clonelist_path);
		return false;
	}
	
	while (fgets(BUF, MAXBUFSZ, fd) != (char *)NULL) {
		if (BUF[0] == '#' || BUF[0] == '\n'|| BUF[0] == '\r') continue;
		
		if (BUF[strlen(BUF)-1] == '\n') {
			BUF[strlen(BUF)-1] = '\0';
			
			if (BUF[strlen(BUF)-1] == '\r') {
				BUF[strlen(BUF)-1] = '\0';
			}
		}
		
		if (! strncmp(BUF, "tables_", 7) && strlen(BUF) == 9) {
			process_single_table(&BUF[7]);
		} else
			process_schema(BUF);
	}
	
	fclose(fd);
	
	process_tables();
	
	return true;
	
} /* clone_main */


/* schema_main
 * Main schema dump routine
 */
static bool schema_main (void)
{
	FILE *fd;
	char BUF[MAXBUFSZ];
	char *clonelist_path = NULL;
	
	str_append(&clonelist_path,
		"%s/%s"
		,get_BRIDGE()
		,CLONELISTFILE
		);

	// Open the clone file
	fd = fopen(clonelist_path, "r");
	
	if (! fd) {
		pgout(mSYS, "cannot open %s", clonelist_path);
		return false;
	}
	
	while (fgets(BUF, MAXBUFSZ, fd) != (char *)NULL) {
		if (BUF[0] == '#' || BUF[0] == '\n'|| BUF[0] == '\r') continue;
		
		if (BUF[strlen(BUF)-1] == '\n') {
			BUF[strlen(BUF)-1] = '\0';
			
			if (BUF[strlen(BUF)-1] == '\r') {
				BUF[strlen(BUF)-1] = '\0';
			}
		}
		
		process_schema(BUF);
	}
	
	fclose(fd);
	
	process_tables();
	
	return true;
	
} /* schema_main */


int main (int argc, char ** argv)
{
	char *ffilename=NULL;
	int c, exstat;
	extern int optind;
	extern char *optarg;
	char *actionarg = NULL;
	char *action = NULL;
			
	append_convert = true;

	// Set signal handling routines
	signal(SIGTERM, signal_handler);
	signal(SIGHUP, signal_handler);
	signal(SIGINT, signal_handler);
	signal(SIGSEGV, signal_handler);

	// Parse options on command line
	while ((c = getopt(argc, argv, "cdnv?")) != -1) {
		switch (c) {
			case 'c':
			DISABLE_COUNTER = true;
			break;
			
			case 'd':
			DEBUG_LEVEL = false;
			break;
			
			case 'n':
			DROP_TABLE = false;
			break;
			
			case 'v':
			VERBOSE = true;
			break;

			case '?':
			usage(); exit(EXIT_SUCCESS);

			default:
			usage(); exit(EXIT_FAILURE);
		}
	}
	
	if (argc <= optind) {
		usage();
		exit(EXIT_FAILURE);
	}
	
	action = str_dup(argv[optind]);
	
	if (argc == optind + 2) {
		actionarg = str_dup(argv[argc-1]);
	}
	
	pgout_set("logs/pgutil.log");
	
	if (VERBOSE) {
		pgout_set_display();
	} else {
		unsetenv("PGISAM");
	}
	
	pgout_zero();
	pgout(mDTSTAMP, "pgutil %s: program started", action);
	
	// Initialize the program
	if (!init_program()) {
		pgout(mDISPLAY, "pgutil: could not initialize the program");
		exit_handler(false);
	}

	if (!strcmp(action, "clone")) {
		exstat = clone_main(actionarg);
	} else
	if (!strcmp(action, "schema")) {
		set_pgisam_options("printonly");
		exstat = schema_main();
	} else {
		pgout(mDISPLAY, "pgutil: %s is not a valid action", action);
		exstat = false;
	}
	
	// End the program
	shutdown_program();

	exit_handler(exstat ? false : true);

} /* main */
