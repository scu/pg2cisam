/* 
 * sys.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <string.h>
#include <malloc.h>
#include <time.h>

#include <isam.h>
#include <decimal.h>

#include "pgisam.h"
#include "sys.h"
#include "iserrcodes.h"

// Shared data
int DEBUG_LEVEL = 0;

// Static data
static char * logfilename = NULL;
static unsigned long count_cycle = 0L;
static bool NO_PID = false;
static bool DISPLAY = false;
static bool PGCB_IS_SET = false;
static bool SQL_PRINT = false;

pgCallback PgCallback;

// External data
extern int iserrno;


// CODE STARTS HERE

/*
 * xalloc
 */
void * xalloc (size_t size)
{
	register void *value = malloc(size);
	memset(value, 0x00, size);

__STACK(xalloc)
	
	if (value == 0)
    	pgout(mNORMAL, "failed to create virtual memory");
	
	__return value;

} /* xalloc */


/*
 * xfree
 */
void xfree (void * value)
{
__STACK(xfree)

	if (! value) {
		__return;
	}
	
	free(value);
	
	value = NULL;
	
	__return;

} /* xfree */

/*
 * pgout_set_callback
 */
void pgout_set_callback(pgCallback callback)
{
__STACK(pgout_set_callback)

	PgCallback = callback;
	PGCB_IS_SET = true;
	
	__return;
	
} /* pgout_set_callback */

/*
 * pgout_get_sql_print
 */
bool pgout_get_sql_print (void) { return SQL_PRINT; }

/*
 * pgout_set
 */
void pgout_set (char * filename)
{
	char cwd[1024];
	char *env_PGISAM = NULL;
	
__STACK(pgout_set)
	
	getcwd(cwd, 1024);
	
	logfilename = (char *)xalloc(strlen(cwd) + strlen(filename) + 8);
	
	sprintf(logfilename, "%s/%s", cwd, filename);
	
	// Set debug level by env var
	env_PGISAM = getenv("PGISAM");
	
	if (env_PGISAM) {
		
		if (strlen(env_PGISAM) == 6) {
			if (! strcmp(env_PGISAM, "debug1")) {
				DEBUG_LEVEL = 1;
			} else
			if (! strcmp(env_PGISAM, "debug2")) {
				DEBUG_LEVEL = 2;
			} else
			if (! strcmp(env_PGISAM, "debug3")) {
				DEBUG_LEVEL = 3;
			}
		} else
		
		if (strlen(env_PGISAM) == 10) {
			if (! strcmp(&env_PGISAM[6], " sql")) {
				SQL_PRINT = true;
			}
		} else
		
		if (! strcmp(env_PGISAM, "sql")) {
			SQL_PRINT = true;
		}
	}
	
	__return;
	
} /* pgout_set */


/*
 * pgout_set_no_pid
 */
void pgout_set_no_pid (void)
{
__STACK(pgout_set_no_pid)

	NO_PID = true;
	
	__return;

} /* pgout_set_no_pid */


/*
 * pgout_set_display
 */
void pgout_set_display (void)
{
__STACK(pgout_set_display)

	DISPLAY = true;
	
	__return;
	
} /* pgout_set_display */


/*
 * pgout_zero
 */
void pgout_zero (void)
{
	FILE *logfd;

__STACK(pgout_zero)
	
	logfd = fopen(logfilename, "w+");
	
	if (logfd == NULL) {
		perror("fopen");
		fprintf(stderr, "FATAL: could not open log file %s\n", logfilename);
		exit(EXIT_FAILURE);
	}
	
	fclose(logfd);
	
	__return;
	
} /* pgout_zero */


/*
 * pgout_t
 */
void pgout_t (int mode, char *fmt, ...)
{
	va_list ap;
	FILE *logfd;
	time_t t;
	char *timestr;
	
__STACK(pgout_t)
	
	if (mode & mDEBUG && (DEBUG_LEVEL < 1)) {
		__return;
	}
	if (mode & mDEBUG1 && (DEBUG_LEVEL < 1)) {
		__return;
	}
	if (mode & mDEBUG2 && (DEBUG_LEVEL < 2)) {
		__return;
	}
	if (mode & mDEBUG3 && (DEBUG_LEVEL < 3)) {
		__return;
	}
	
	if (! logfilename) {
		fprintf(stderr, "FATAL: logfilename not set (use pgout_set)\n");
		exit(EXIT_FAILURE);
	}

	va_start(ap, fmt);
	
	// Build datetime stamp:
	time(&t);
	timestr = ctime(&t);
	timestr[strlen(timestr)-1]='\0';

	logfd = fopen(logfilename, "a+");
	
	if (logfd == NULL) {
		perror("fopen");
		fprintf(stderr, "FATAL: could not open log file %s\n", logfilename);
		exit(EXIT_FAILURE);
	}

	if ((mode & mDISPLAY) || DISPLAY) {
		fprintf(stderr, "%d|%s: ", getpid(), timestr);
		vfprintf(stderr, fmt, ap);
	}
	
	fprintf(logfd,
		"%d%s%s: ",
		NO_PID ? 0 : getpid(),
		mode & mDTSTAMP ? "|" : "",
		mode & mDTSTAMP ? timestr : ""
		);

	vfprintf(logfd, fmt, ap);
	
	// Fire the callback
	if (PGCB_IS_SET) {
		char vbuf[4096];
		vsprintf(vbuf, fmt, ap);
		PgCallback(mode, vbuf);
	}
	
	if (mode & mSYS) {
		fprintf(logfd, " <%s>", strerror(errno));
		
		if ((mode & mDISPLAY) || DISPLAY) {
			fprintf(stderr, " <%s>", strerror(errno));
		}
	}

	if (mode & mISAM) {
		char *description = NULL;
		int x = 0;
	
		while (iserrlist[x++].errcode) {
			if (iserrlist[x].errcode == iserrno) {
			
				// Set ptr to description
				description = iserrlist[x].description;
			
				break;
			}
		}
		
		fprintf(logfd, " iserr=<%d: %s>", iserrno,
			description ? description : "UNKNOWN");
		
		if ((mode & mDISPLAY) || DISPLAY) {
			fprintf(stderr, " iserr=<%d: %s>", iserrno,
				description ? description : "UNKNOWN");
		}
	}
				
	fprintf(logfd, "\n");
	
	if ((mode & mDISPLAY) || DISPLAY) {
		fprintf(stderr, "\n");
	}
	
	fflush(logfd);

	fclose(logfd);

	va_end(ap);
	
	__return;

} /* pgout_t */


/*
 * fprintb
 * Print bytes in hex
 */
void fprintb (FILE *fd, char *label, void *buf, size_t sz)
{
	int x;
	char *b = buf;
	
	fprintf(fd, "%s: ", label);
	
	for (x=0; x < sz; x++)
		fprintf(fd, "%.2X(%.2X) ", x, b[x]);
	
	fprintf(fd, "\n");
	
} /* fprintb */
