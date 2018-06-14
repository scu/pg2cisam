/*
 * wrappers.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include <isam.h>
#include <decimal.h>

#include "sys.h"
#include "pgisam.h"

#include "wrappers.h"

// Static data
static unsigned long reccount = 0L;

// External data
extern bool suppress_error;

void display (char * record)
{
		pgout(0, "<[%.39s]>", record);

} /* display */


/*
 * create_table
 */
int create_table (char * filename, struct keydesc * key, int recsize, int mode)
{
	int isfd;

	// First, we create an isam file
	isfd = isbuild(filename, recsize, key, mode);
	if (isfd < 0) {
		pgout(0, "could not create [%s]", filename);
	}
	
	return isfd;
	
} /* create_table */


/*
 * create_index
 */
int create_index (int isfd, struct keydesc * key)
{
	int io = 0;
	
	io = isaddindex(isfd, key);
	if (io < 0) {
		pgout(0, "could not create index on isfd=%d", isfd);
	}
	
	return io;	
	
} /* create_index */


/*
 * open_table
 */
int open_table (char * filename, int mode)
{
	int isfd;

	// First, we create an isam file
	isfd = isopen(filename, mode);
	if (isfd < 0) {
		pgout(0, "could not open [%s]", filename);
	}
	
	return isfd;
	
} /* open_table */


/*
 * close_table
 */
int close_table (int isfd)
{
	int ret;
	
	ret = isclose(isfd);
	
	if (ret < 0) {
		pgout(0, "could not close fd=%d", isfd);
	}
	
	return ret;
	
} /* close_table */


/*
 * drop_table
 */
int drop_table (char * filename)
{
	int ret;
	
	ret = iserase(filename);
	
	if (ret < 0) {
		pgout(0, "could not erase [%s]", filename);
	}
	
	return ret;
	
} /* drop_table */


/*
 * bulk_insert
 */
int bulk_insert (int isfd, char * filename, int recsize, int cols[])
{
	FILE *fd;
	char BUF[1024];
	char *record = NULL;
	char *filepath = NULL;
	char *beginning, *end;
	int *sv;
	int offset = 0;
	int io = 0;

	str_append(&filepath,
		"../psvfiles/%s"
		,filename
		);
	
	fd = fopen(filepath, "r");
	if (fd == (FILE *)NULL) {
		pgout(mSYS, "could not open [%s]", filepath);
		goto retbad;
	}
	
	xfree(filepath);
	
	while (fgets(BUF, 1024, fd) != (char *)NULL) {
		BUF[strlen(BUF)-1] = '\0';
		
		pgout(0, "bulk insert [%s]", BUF);
		
		// Prepare the record
		record = (char *)xalloc(recsize);
		memset(record, 0x20, recsize);
		
		offset = 0;
		beginning = end = BUF;
				
		// Save ptr to cols
		sv = cols;
		do {
			if (*cols == 0) {
				break;
			}
			
			beginning = end;
			end = strchr(beginning, '|');
			
			if (end) {
				*end = '\0'; *end++;
			} else {
				end = strchr(beginning, 0);
			}
			
			memcpy(&record[offset], beginning, strlen(beginning));
			
			offset += *cols;

		} while (*cols++);
		
		// Restore cols ptr
		cols = sv;
		
		// Write the data
		pgout(0, "writing record number %ld", reccount++);
		io = iswrcurr(isfd, record);
		if (io < 0) {
			goto retbad;
		}
		
		// Free the record
		xfree(record);
	}
	
	fclose(fd);
	
	return true;

retbad:
	fclose(fd);
	
	pgout(0, "bulk_insert: failed");
	xfree(record);
	return false;
	
} /* bulk_insert */


/*
 * read_sequential
 */
int read_sequential (int isfd, struct keydesc * key, int recsize)
{
	int io;
	char *record = NULL;
	
	// Prepare the record
	record = (char *)xalloc(recsize);
	memset(record, 0x20, recsize);
	
	io = isstart(isfd, key, 1, record, ISFIRST);
	if (io < 0) {
		goto retbad;
	}
	
	while ((io = isread(isfd, record, ISNEXT)) >= 0) {
		record[recsize - 1] = '\0';
		pgout(0, "read_seq: [%.39s >]", record);
	}
	
	// Free the record
	str_free(&record);
	
	return true;

retbad:
	str_free(&record);
	pgout(0, "read_sequential: failed");
	return false;
	
} /* read_sequential */


/*
 * read_record
 */
int read_record (int isfd, struct keydesc * key, char * record)
{
	int io;
	
	io = isstart(isfd, key, 1, record, ISEQUAL);
	if (io < 0) {
		pgout(0, "isstart failed");
		goto retbad;
	}
	
	io = isread(isfd, record, ISEQUAL);
	if (io < 0) {
		pgout(0, "isread failed");
		goto retbad;
	}
	
	pgout(0, "read_rec: [%.39s >]", record);
	
	return true;
	
retbad:
	pgout(0, "read_record: failed");
	return false;
	
} /* read_record */


/*
 * write_record
 */
int write_record (int isfd, char * record, bool overwrite)
{
	int io;
	
	if (overwrite) {
		suppress_error = true;
		io = isrewcurr(isfd, record);
		suppress_error = false;
		
		if (io < 0) {
			io = iswrite(isfd, record);
			if (io < 0) {
				goto retbad;
			}
		}
	} else {
		suppress_error = true;
		io = isrewrite(isfd, record);
		suppress_error = false;
		
		if (io < 0) {
			io = iswrite(isfd, record);
			if (io < 0) {
				goto retbad;
			}
		}
	}
	
	return true;
	
retbad:
	pgout(0, "write_record: failed");
	return false;
	
} /* write_record */


/*
 * delete_record
 */
int delete_record (int isfd, struct keydesc * key, char * record)
{
	int io = 0;
	
	io = isdelete(isfd, record);
	if (io < 0) {
		goto retbad;
	}
	
	return true;
	
retbad:
	pgout(0, "delete_record: failed");
	return false;
	
} /* delete_record */


/*
 * begin_transaction
 */
void begin_transaction (void)
{
	isbegin();
	
} /* begin_transaction */


/*
 * end_transaction
 */
void end_transaction (bool rollback)
{
	if (rollback) {
		isrollback();
	} else {
		iscommit();
	}
	
} /* end_transaction */


/*
 * open_logfile
 */
void open_logfile (void)
{
	if (access("recovery.log", W_OK) < 0) {
		if (creat("recovery.log", 0666) < 0) {
			pgout(mSYS, "creat failed in open_logfile");
			return;
		}
	}
	
	islogopen("recovery.log");
	
} /* open_logfile */


/*
 * close_logfile
 */
void close_logfile (void)
{
	islogclose();
	
} /* close_logfile */
