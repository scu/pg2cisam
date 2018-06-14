/*
 * wrappers.h
 * ISAM wrapper functions
 */

#ifndef _WRAPPERS_H
#define _WRAPPERS_H

#include "sys.h"

void display (char * record);

int create_table (char * filename, struct keydesc * key, int recsize, int mode);

int create_index (int isfd, struct keydesc * key);

int open_table (char * filename, int mode);

int close_table (int isfd);

int drop_table (char * filename);

int bulk_insert (int isfd, char * filename, int recsize, int cols[]);

int read_sequential (int isfd, struct keydesc * key, int recsize);

int read_record (int isfd, struct keydesc * key, char * record);

int write_record (int isfd, char * record, bool overwrite);

int delete_record (int isfd, struct keydesc * key, char * record);

void begin_transaction (void);

void end_transaction (bool rollback);

void open_logfile (void);

void close_logfile (void);

#endif // _WRAPPERS_H
