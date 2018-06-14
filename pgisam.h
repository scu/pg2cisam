/*
 * pgisam.h: external routines (not covered by isam/decimal.h)
 */

#ifndef _PGISAM_H
#define _PGISAM_H

#include <decimal.h>
#include <bridge.h>

#define pgout(a, b, ...) pgout_t(a, "%s|%s|%d|" b, __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

#define	ISAM_TYPE_CHAR		0x10
#define ISAM_TYPE_DECIMAL	0x20
#define ISAM_TYPE_CODE		0x40
#define ISAM_TYPE_BINARY	0x80
#define ISAM_TYPE_INTEGER	0x100
#define ISAM_TYPE_BOOLEAN	0x200
#define ISAM_TYPE_CODEBLANK	0x400

#define		mNORMAL		0x00
#define		mSYS		0x01	// Add system error info to log
#define		mISAM		0x02	// Log isam error strings
#define		mSQL		0x04	// TraceSQL placeholder
#define		mDEBUG1		0x08	// Log basic messages
#define		mDEBUG2		0x10	// Log basic messages
#define		mDEBUG3		0x20	// Log basic messages
#define		mTRACE		0x20	// Trace basic messages
#define		mDISPLAY	0x40	// Display messages to stdout
#define		mDTSTAMP	0x80	// Add date/time stamp to log
#define		mDEBUG		0x100

// Configuration enum
typedef enum pgisam_opt {
	 PGIsamNormal = 0
	,PrintOnly = 1
} pgisam_opt;

extern pgisam_opt PGIsamOptions;

// pgout typedef
typedef void (*pgCallback)(int mode, char *message);

/* dec_to_str
 * Convert a decimal type to a string
 */
unsigned char * dec_to_str (unsigned char *dec_str, int sz);

/* dec_diag
 * Decimal diagnostic
 */
void dec_diag (dec_t *dec);

/* init_program:
 * Initialize a Postgres connection
 * (in pgbridge.c)
 */
int init_program (void);

/* set_pgisam_options
 * Set overridable runtime options for PG-ISAM
 * 
 * Options (separate with comma):
 * printonly	Do not execute SQL; print to stdout
 */
void set_pgisam_options (char *optstr);

/* shutdown_program:
 * Initialize a Postgres connection
 * (in pgbridge.c)
 */
int shutdown_program (void);

/* get_last_sql:
 * Get the last SQL statement executed by PGISAM
 * (in pgbridge.c)
 */
char * get_last_sql (void);

/* get_EDATA|BRIDGE
 * "Get" methods for retreiving the
 * EDATA and BRIDGE environment variables.
 * (cannot be changed at runtime)
 */
char * get_EDATA(void);
char * get_BRIDGE(void);

/* pgout_set:
 * Initialize the messages file
 * (in sys.c)
 */
void pgout_set (char * filename);

/* pgout_set_callback
 * Set a callback function to be fired at pgout
 */
void pgout_set_callback(pgCallback callback);

/* pgout_get_sql_print
 * (in sys.c)
 */
int pgout_get_sql_print (void);

/* pgout_set_no_pid
 * Suppress printing PIDs in messages
 * (in sys.c)
 */
void pgout_set_no_pid (void);

/* pgout_set_display
 * Print msgs to stdout
 * (in sys.c)
 */
void pgout_set_display (void);

/* pgout_zero:
 * Zero-byte the message file (must be used after pgout_set)
 * (in sys.c)
 */
void pgout_zero (void);

/* pgout_t:
 * Write a message to the log
 * (in sys.c)
 */
void pgout_t (int mode, char *fmt, ... );

#endif // _PGISAM_H
