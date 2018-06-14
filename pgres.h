/*
 * pgres.h: Postgres routines
 */

CONN * pg_startup(CONN * conn, char * connstr, char * set_schema);
bool pg_shutdown(CONN * conn);
void pg_msg(CONN * conn, int mode, char *fmt, ...);
RES * pg_exec(CONN * conn, char * sql);
void pg_free (void *data);
