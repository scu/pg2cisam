/*
 * isbridge.h: bridge routines between C-ISAM and PostgreSQL APIs
 */

#ifndef _ISBRIDGE_H
#define _ISBRIDGE_H

#include <isam.h>
#include <decimal.h>

/*
 * initialize:
 * Initialize a C-ISAM program
 */
bool init_program (void);

/*
 * get_EDATA|BRIDGE
 * Stubs
 */
char * get_EDATA(void);
char * get_BRIDGE(void);

/*
 * shutdown_program
 * Shutdown a Postgres connection (stub in C-ISAM)
 */
bool shutdown_program (void);

/*
 * x_isaddindex:
 * Add an index to a C-ISAM file
 * isfd		file desccriptor returned by isopen or isbuild
 * key		pointer to a key description struct
 */
int x_isaddindex (int isfd, struct keydesc * key);

/*
 * x_isbegin:
 * Defines the beginning of a transaction
 */
int x_isbegin (void);

/*
 * x_isbuild:
 * Create a C-ISAM file
 * filename	name of the file w/out an extension
 * reclen	length of the record in bytes
 * keydesc	ptr to the key description struct
 * mode		access mode
 */
int x_isbuild (char * filename, int reclen, struct keydesc * keydesc, int mode);

/*
 * x_iscleanup:
 * Closes all C-ISAM files opened by the program
 */
int x_iscleanup (void);

/*
 * x_isclose:
 * Closes a C-ISAM file
 * isfd		File descriptor
 */
int x_isclose (int isfd);

/*
 * x_iscommit:
 * Ends a transaction and releases all locks
 */
int x_iscommit (void);

/*
 * x_isdelcurr:
 * Deletes the current record from the C-ISAM file
 * isfd		file descriptor
 */
int x_isdelcurr (int isfd);

/*
 * x_isdelete:
 * Deletes a record using the primary key
 * isfd		file descriptor
 * record	contains a key value in the position defined for the primary key
 */
int x_isdelete (int isfd, char * record);

/*
 * x_isdelindex:
 * Removes an entire index
 * isfd		file descriptor
 * keydesc	ptr to a key description struct
 */
int x_isdelindex (int isfd, struct keydesc * keydesc);

/*
 * x_isdelrec:
 * Deletes a record using the record number
 * isfd		file descriptor
 * recnum	record number of the data file record
 */
int x_isdelrec (int isfd, long recnum);

/*
 * x_iserase:
 * Removes the operating system files comprising the C-ISAM file
 * filename	the C-ISAM file to delete
 */
int x_iserase (char * filename);

/*
 * x_isindexinfo:
 * Determines information about the structure and indexes of a C-ISAM file
 * isfd		file descriptor
 * buffer	ptr to a struct (keydesc | dictinfo)
 * number	an index number or zero
 */
int x_isindexinfo (int isfd, struct keydesc * buffer, int number);

/*
 * x_islogclose:
 * Closes the transaction log file
 */
int x_islogclose (void);

/*
 * x_islogopen:
 * Opens the transaction log file
 * logname	pointer to the filename string
 */
int x_islogopen (char * logname);

/*
 * x_isopen:
 * Opens a C-ISAM file for processing
 * filename	the name of the file
 * mode		mode
 */
int x_isopen (char * filename, int mode);

/*
 * x_isread:
 * Read records sequentially or randomly
 * isfd		file descriptor
 * record	pointer to string containing the search val, and receives the record
 * mode		mode
 */
int x_isread (int isfd, char * record, int mode);

/*
 * x_isrewcurr:
 * Modifies or updates fields in the current record
 * isfd		file descriptor
 * record	pointer to string containing the search val, and receives the record
 */
int x_isrewcurr (int isfd, char * record);

/*
 * x_isrewrec:
 * Updates record identified by its record number
 * isfd		file descriptor
 * recnum	the record number
 * record	pointer to string containing the search val, and receives the record
 */
int x_isrewrec (int isfd, long recnum, char * record);

/*
 * x_isrewrite:
 * Rewrite the nonprimary key fields of a record
 * isfd		file descriptor
 * record	pointer to string containing the search val, and receives the record
 */
int x_isrewrite (int isfd, char * record);

/*
 * x_isrollback:
 * Cancel the effect of C-ISAM calls since v_isbegin
 */
int x_isrollback (void);

/*
 * x_isstart:
 * Select the index and starting point in the index for subsequent calls to v_isread
 * isfd		file descriptor
 * keydesc	pointer to a key description structure
 * length	part of the key considered significant when locating the starting record
 * record	specifies the key search value
 * mode		mode
 */
int x_isstart (int isfd, struct keydesc * keydesc, int length, char * record, int mode);

/*
 * x_iswrcurr:
 * Writes a record and makes it the current record
 * isfd		file descriptor
 * record	specifies the key search value
 */
int x_iswrcurr (int isfd, char * record);

/*
 * x_iswrite:
 * Writes a unique record
 * isfd		file descriptor
 * record	specifies the key search value
 */
int x_iswrite (int isfd, char * record);

#endif // _ISBRIDGE_H
