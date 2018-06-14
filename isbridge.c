/*
 * isbridge.c: bridge routines between C-ISAM and PostgreSQL APIs
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "sys.h"
#include "pgbridge.h"

// External data
bool suppress_error = false;

// CODE STARTS HERE

/*
 * init_program:
 * Initialize a C-ISAM program
 */
bool init_program (void)
{
__STACK(init_program)

	__return true;
	
} /* init_program */

/*
 * get_EDATA|BRIDGE
 * Stubs
 */
char * get_EDATA(void)
{
__STACK(get_EDATA)

	__return (char *)NULL;

} /* get_EDATA */

char * get_BRIDGE(void)
{
__STACK(get_BRIDGE)

	__return (char *)NULL;

} /* get_BRIDGE */

/*
 * shutdown_program
 * Shutdown a Postgres connection (stub in C-ISAM)
 */
bool shutdown_program (void)
{
__STACK(shutdown_program)

	__return true;
} /* shutdown_program */
