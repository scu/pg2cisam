/*
 * schema.c: C-ISAM schema definitions for Postgres
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

// For keydesc
#include <isam.h>
#include <decimal.h>

#include <libpq-fe.h>

#include "sys.h"
#include "schema.h"
#include "pgres.h"
#include "xstring.h"

#define MAXBUFSZ 1024
#define MAXFDS 512

// External data
extern bool PRINT_DEBUG;

// Shared data
bool append_convert = false;

// Static data types
static const char * conn_def_file = "conn.def";
static int fdpool[MAXFDS];
static bool is_fdpool_initialized = false;
static unsigned long context_id = 1L;

// Connection string
static char *connstr = NULL;
static CONN *CURRENT_conn = NULL;

// Schema
static char *set_schema = NULL;

// Static function prototypes
static char * CONN_build_string (void);
static int CONTEXT_fdpool_get (void);
static void CONTEXT_fdpool_delete (int fd);


// CODE STARTS HERE


// _____/ CONN functions \__________

/*
 * CONN_build_string [X]
 * Build a connection string
 */
static char * CONN_build_string (void)
{
	FILE *fd;
	bool VALID_CONN = false;
	char BUF[MAXBUFSZ];
	char *start = NULL;
	char *end = NULL;
	char *EDATA_DEF, *hostname, *port, *database, *username, *password;
	char *_connstr = NULL;
	char *conn_def_path = NULL;
	
__STACK(CONN_build_string)
	
	pgout(mDEBUG2, "EDATA=[%s]", get_EDATA());
	
	str_append(&conn_def_path, "%s/%s"
		,get_BRIDGE()
		,conn_def_file
		);

	pgout(mDEBUG2, "Opening conn def file [%s]", conn_def_path);
	
	fd = fopen(conn_def_path, "r");
	
	if (! fd) {
		pgout(mSYS, "could not open %s", conn_def_path);
		goto retbad;
	}

	while (fgets(BUF, MAXBUFSZ, fd) != (char *)NULL) {

		// Allow comments and blank lines
		if (BUF[0] == '#' || BUF[0] == '\n'|| BUF[0] == '\r') continue;

		// Strip off CR/newline characters
		if (BUF[strlen(BUF)-1] == '\n') {
			BUF[strlen(BUF)-1] = '\0';
			if (BUF[strlen(BUF)-1] == '\r') {
				BUF[strlen(BUF)-1] = '\0';
			}
		}
		
		start = end = BUF;
		EDATA_DEF = start = str_part(&end, '=');
		
		if (! EDATA_DEF) {
			pgout(0, "[%s] malformed", conn_def_file);
			goto retbad;
		}
	
		if ( ! strcmp(EDATA_DEF, get_EDATA())) {
			hostname = start = str_part(&end, ',');
			if (! hostname) {
				goto malformed;			
			}
			
			port = start = str_part(&end, ',');
			if (! port) {
				goto malformed;			
			}
			
			database = start = str_part(&end, ',');
			if (! database) {
				goto malformed;			
			}
			
			start = str_part(&end, ',');
			if (! start) {
				goto malformed;			
			}
			
			set_schema = str_dup(start);
			
			username = start = str_part(&end, ',');
			if (! username) {
				goto malformed;
			}
			
			password = start = str_part(&end, ',');
			if (! password) {
				goto malformed;			
			}
			
			VALID_CONN = true;

			break;			
		}		
	}
	
	if (! VALID_CONN) {
		pgout(0, "no schema matching [%s] in [%s]",
			get_EDATA(), conn_def_file);
		goto retbad;
	}
	
	// Build the connection string
	asprintf(&_connstr,
		"host=%s "
		"port=%s "
		"dbname=%s "
		"user=%s "
		"password=%s",
		hostname,
		port,
		database,
		username,
		password
		);
	
	if (! hostname) {
		pgout(0, "no schema matching [%s] in [%s]",
			get_EDATA(), conn_def_file);
		goto retbad;
	}
	
	fclose(fd);
	
	pgout(mDEBUG2, "connstr=[%s]", _connstr);

	__return _connstr;
	
malformed:
	pgout(0, "%s is malformed"
		" (Usage: EDATA=hostname, port, database, schema, username, password)",
		conn_def_file);

retbad:
	if (fd) fclose(fd);
	
	__return (char *)NULL;
	
} /* CONN_build_string */


/*
 * CONN_new [X]
 * Create a new connection to a Postgres database
 */
CONN * CONN_new (void)
{
	CONN *conn;
	
__STACK(CONN_new)
	
	pgout(mDEBUG3, "connecting");

	// If we haven't built a connection string yet,
	// do that here.
	if (! connstr) {
		connstr = CONN_build_string();
		
		if (! connstr) {
			pgout(0, "unable to retreive connection string");
			__return (CONN *)NULL;
		}
	}
	
	conn = (CONN *)xalloc(sizeof(CONN));
	
	// Establish the connection
	CURRENT_conn = conn = pg_startup(conn, connstr, set_schema);
	
	// Set the bool flag
	if (conn) {
		conn->is_connected = true;
	}
	
	__return conn;
	
} /* CONN_new */


/*
 * CONN_delete [X]
 * Delete a connection (NOTE: since CONN is reusable, certain members stay intact)
 * conn			Connection object
 */
bool CONN_delete (CONN * conn)
{
	bool ret;
	
__STACK(CONN_delete)
	
	if (! conn) {
		__return false;
	}
	
	pgout(mDEBUG3, "disconnecting");
	
	conn->is_connected = ret = pg_shutdown(conn);
	
	xfree(conn);
	conn = NULL;
	
	__return ret;
	
} /* CONN_delete */


/*
 * CONN_begin | commit | rollback [X]
 * Transaction control functions
 * conn			Connection object
 */
bool CONN_begin (CONN * conn)
{
	RES *res;

__STACK(CONN_begin)
	
	pgout(mDEBUG3, "starting transaction");
	
	if ((res = pg_exec(conn, "BEGIN")) == (RES *)NULL) {
		__return err;
	}
	
	RES_delete(&res);
	
	__return true;
	
} /* CONN_begin */


bool CONN_commit (CONN * conn)
{
	RES *res;
	
__STACK(CONN_commit)
	
	pgout(mDEBUG3, "committing transaction");
	
	if ((res = pg_exec(conn, "COMMIT")) == (RES *)NULL) {
		CONN_rollback(conn);
		__return err;
	}

	RES_delete(&res);
	
	__return true;
	
} /* CONN_commit */


bool CONN_rollback (CONN * conn)
{
	RES *res;
	
__STACK(CONN_rollback)
	
	pgout(mDEBUG3, "rolling back transaction");
	
	if ((res = pg_exec(conn, "ROLLBACK")) == (RES *)NULL) {
		__return err;
	}
	
	RES_delete(&res);
	
	__return true;
	
} /* CONN_rollback */


// _____/ RES functions \__________
/*
 * RES_delete [X]
 * Delete a resource
 * res	Pointer to the resource
 */
void RES_delete (RES ** res)
{
	RES *r = *res;
	
__STACK(RES_delete)
	
	if (r == (RES *)NULL) {
		__return;
	}
	
	pgout(mDEBUG3, "deleting resource");
	
	if (r->pgres != (PGresult *)NULL) {
		PQclear(r->pgres);
	}	
	
	// For security, clean the memory
	memset(r, 0x00, sizeof(RES));

	free(r);
	
	*res = NULL;
	
	__return;
	
} /* RES_delete */


/*
 * RES_print [X]
 * Print the columns/rows in a res
 * res			Pointer to the resource
 */
void RES_print (RES * res)
{
	PQprintOpt poptions;
	
__STACK(RES_print)
	
	pgout(mDEBUG3, "printing resource");
	
	memset(&poptions, 0x00, sizeof(poptions));
	poptions.header = 1;
	poptions.align = 1;
	poptions.html3 = 0;
	poptions.fieldSep = "|";
	poptions.fieldName = NULL;
	
	PQprint(stdout, res->pgres, &poptions);
	
	__return;
	
} /* RES_print */


/*
 * RES_get_oid
 * Obtain an oid from a RES
 * res			Pointer to the resource
 * oidstr		Pointer to the oid
 */
void RES_get_oid (RES * res, char ** oidstr)
{
	char *colname, *oid = *oidstr;
	PGresult *pgres = res->pgres;
	int colidx = 0;
	
__STACK(RES_get_oid)
	
	pgout(mDEBUG3, "obtaining oid");

	// Free the old string
	str_free(&oid);	
	
	// Iterate through each RES column
	while ((colname = PQfname(pgres, colidx)) != (char *)NULL) {
		
		// Found a match?  Save it...
		if (! strcmp(colname, "oid")) {
				oid = str_dup(PQgetvalue(pgres, 0, colidx));
				pgout(mDEBUG3, "found oid=[%s]", oid);
				break;	
		}
			
		colidx++;
		
		// Can't go past the number of fields
		if (colidx == res->nfields) break;
	}
	
	*oidstr = oid;
	
	__return;
	
} /* RES_get_oid */


// _____/ COLUMN functions \__________
/*
 * COLUMN_push [X]
 * Create a new column on the tail of the list
 * column		Pointer to the head of the list
 * name			Column name
 * startpos		Starting position in the record
 * length		Length of the column
 * codelength	If a code field, significant portion of the column
 * coltype		Type of column (ISAM_TYPE_CHAR|DECIMAL|CODE)
 */
void COLUMN_push (COLUMN ** column, char * name, char * length,
	char * codelength, int coltype, char * params)
{
	COLUMN *new_element = xalloc(sizeof(COLUMN));
	COLUMN *current = *column;
	
__STACK(COLUMN_push)
	
	pgout(mDEBUG3, "name=[%s] length=[%s] codelength=[%s] "
		"coltype=%d params=[%s]",
		name, length, codelength, coltype, params);
	
	new_element->name = str_dup(name);
	new_element->length = atoi(length);

	if (codelength) {
		new_element->codelength = atoi(codelength);
	}

	new_element->params = str_dup(params);
	new_element->datatype = coltype;
	
	new_element->next = *column;
	
	if (current != NULL) {
		current->prev = new_element;
	}

	*column = new_element;
	
	__return;
	
} /* COLUMN_push */


/*
 * COLUMN_reverse
 * Reverse a COLUMN list
 * column		Pointer to the column head
 */
void COLUMN_reverse (COLUMN ** column)
{
	COLUMN *result = NULL;
	COLUMN *current = *column;
	COLUMN *next;
	
__STACK(COLUMN_reverse)
	
	while (current != NULL) {
		next = current->next;
		current->next = result;
		result = current;
		current = next;
	}
	
	*column = result;
	
	__return;

} /* COLUMN_reverse */


/*
 * COLUMN_push_copy [X]
 * Add a copy of a column to another list
 * to			Pointer which will allocated and copied into
 * from			Pointer containing structure to copy
 */
void COLUMN_push_copy (COLUMN ** to, COLUMN * from)
{
	COLUMN *new_element = xalloc(sizeof(COLUMN));
	
__STACK(COLUMN_push_copy)

	new_element->name = str_dup(from->name);
	new_element->params = str_dup(from->params);
	new_element->codelength = from->codelength;
	new_element->datatype = from->datatype;
	new_element->length = from->length;
	new_element->startpos = from->startpos;
	
	new_element->next = *to;
	*to = new_element;
	
	__return;

} /* COLUMN_push_copy */


/*
 * COLUMN_get [X]
 * Retrieve a column by it's name
 * column		Pointer to the list to print
 * name			Name of the column
 */
COLUMN * COLUMN_get (COLUMN * column, char * name)
{
	COLUMN *c = column;
	
__STACK(COLUMN_get)
	
	while (c) {
		if (! strcmp(c->name, name)) {
			__return c;
		}
		c = c->next;
	}
	
	__return (COLUMN *)NULL;
	
} /* COLUMN_get */


/*
 * COLUMN_print [X]
 * Print a COLUMN type to stdout
 * column		Pointer to the list to print
 * label		Label to identify the print list
 */
char * COLUMN_print (COLUMN * column, char * label, int indent_level)
{
	char *strlabel = "UNKNOWN";
	char *indent = NULL;
	char *b = NULL;
	int x;
	COLUMN *c = column;
	
__STACK(COLUMN_print)
	
	for (x=0; x < indent_level; x++) {
		str_append(&indent, "\t");
	}
	
	while (c) {

		if (c->datatype & ISAM_TYPE_CHAR) {
			strlabel = "CHAR";
		}
		
		if (c->datatype & ISAM_TYPE_DECIMAL) {
			strlabel = "DECIMAL";
		}
		
		if (c->datatype & ISAM_TYPE_CODE) {
			strlabel = "CODE";
		}
		
		if (c->datatype & ISAM_TYPE_CODEBLANK) {
			strlabel = "CODEBLANK";
		}
		
		str_append(&b, "%sCOLUMN %s : %s {\n%s\tstartpos=[%d] length=[%d]\n%s\ttype=[%s] value=[%s]"
			,indent
			,c->name
			,label
			,indent
			,c->startpos
			,c->length
			,indent
			,strlabel
			,c->value ? (char *)c->value : ""
			);
		
		if (c->codelength)
			str_append(&b, " codelength=[%d]", c->codelength);

		if (c->params)
			str_append(&b, " params=[%s]", c->params);
		
		str_append(&b, "\n%s}\n", indent);
					
		c = c->next;
	}
	
	str_free(&indent);
	
	__return b;
	
} /* COLUMN_print */


/*
 * COLUMN_clean [X]
 * Clean a column's values
 * column		Pointer to the object to clean
 */
void COLUMN_clean (COLUMN * column)
{
	COLUMN *c = column;
	
__STACK(COLUMN_clean)
	
	if (! c) {
		pgout(0, "cannot clean a non-existent column");
		__return;
	}
	
	while (c) {
	
		if (c->datatype == ISAM_TYPE_BINARY) {
			str_free((char **)&c->value);
		} else {
			str_free((char **)&c->value);
		}	
		
		c = c->next;
	}
	
	__return;
	
} /* COLUMN_clean */


/*
 * COLUMN_from_record [X]
 * Fills a column's values from record
 * column		Pointer to object receiving values from record
 * record		Generic record pointer containting values
 */
void COLUMN_from_record (COLUMN * column, char * record)
{
	COLUMN *c = column;
		
__STACK(COLUMN_from_record)
	
	pgout(mDEBUG3, "column < record");
	
	while (c) {
		size_t padlength;
		char *unescaped;
		
		// Skip "phantom" columns
		if (c->is_phantom) {
			c = c->next;
			continue;
		}
		
		if (! str_is_blank(&record[c->startpos], c->length)) {
			
			// --- DECIMAL
			if (c->datatype == ISAM_TYPE_DECIMAL) {
				unsigned char *dec_str = NULL;
				
				dec_str = malloc(33);
				memset(dec_str, 0, 33);

				dectostr(&dec_str, &record[c->startpos], c->length);

				// A blank field is NULL
				if (! str_is_blank(dec_str, c->length)) {
					c->value = (unsigned char *)str_dup(dec_str);
				}
				
				free(dec_str);
			}
			else
			// --- INTEGER
			if (c->datatype == ISAM_TYPE_INTEGER) {
				long l_number;
				
				memcpy(&l_number, &record[c->startpos], sizeof(long));

				asprintf((void *)&c->value, "%d", l_number);
			}
			else
			// --- BINARY
			if (c->datatype == ISAM_TYPE_BINARY) {
				size_t vallen = 0;
				c->value = (unsigned char *)PQescapeByteaConn(CURRENT_conn->pgconn, (const unsigned char *)&record[c->startpos],
					c->length, &vallen);
			}
			else
			// --- BOOLEAN
			if (c->datatype == ISAM_TYPE_BOOLEAN) {
				if (record[c->startpos] == 'Y')
					c->value = str_dup("true");
				else
				if (record[c->startpos] == 'N')
					c->value = str_dup("false");
			} else
			// --- CODE
			if ((c->datatype == ISAM_TYPE_CODE) && c->codelength) {
				int startpos = c->startpos;

				// If numeric, start at the end of code minus the actual length
				if (str_is_block_numeric(&record[c->startpos], c->length))
					startpos += (c->length - c->codelength);
				
				// Allocate enough memory to hold the value + the null terminator
				unescaped = (char *)xalloc((c->codelength) + 1);
				c->value = (unsigned char *)xalloc((c->codelength * 2) + 1);

				memcpy(unescaped, &record[startpos], c->codelength);
			
				PQescapeString((char *)c->value, (const char *)unescaped,
					strlen(unescaped));
			
				str_free(&unescaped);
			}
			// --- CHAR/VARCHAR
			else {
				padlength = str_padlength(&record[c->startpos], c->length);
				
				// Allocate enough memory to hold the value + the null terminator
				unescaped = (char *)xalloc((c->length - padlength) + 1);
				c->value = (unsigned char *)xalloc(((c->length - padlength) * 2) + 1);

				memcpy(unescaped, &record[c->startpos], c->length - padlength);
			
				PQescapeString((char *)c->value, (const char *)unescaped,
					strlen(unescaped));
			
				str_free(&unescaped);
			}
		}
		// Blank values
		else {
			// Blank booleans are treated differently
			if (c->datatype == ISAM_TYPE_BOOLEAN)
				c->value = str_dup("null");
				
			// Blank "CODEBLANK" values
			if (c->datatype == ISAM_TYPE_CODEBLANK) {
				// Allocate enough memory to hold the spaces plus one
				c->value = (char *)xalloc(c->length + 1);
				// Set spaces into the code length value
				memset(c->value, 0x20, c->length);
			}				
		}
		
		c = c->next;
	}
	
	__return;
	
} /* COLUMN_from_record */


/*
 * COLUMN_to_record [X]
 * Fills a record from COLUMN
 * column		Pointer to object containing values
 * record		Generic record pointer receiving values
 */
void COLUMN_to_record (COLUMN * column, char ** record)
{
	COLUMN *c = column;
	char *rec = *record;
	
__STACK(COLUMN_to_record)
	
	pgout(mDEBUG3, "column > record");
	
	while (c) {
		unsigned int vallen;
		
		// Skip "phantom" columns
		if (c->is_phantom) {
			c = c->next;
			continue;
		}		

		// Only fill char arrays if value has a size
		if (! c->value) {
			c = c->next;
			continue;
		}

		vallen = strlen((char *)c->value);
		
		// --- DECIMAL
		if (c->datatype == ISAM_TYPE_DECIMAL) {
			dec_t number;
			
			deccvasc((char *)c->value, vallen, &number);
			
			stdecimal(&number, &rec[c->startpos], c->length);
		}
		else
		// --- INTEGER
		if (c->datatype == ISAM_TYPE_INTEGER) {
			long number;

			number = atol((char *)c->value);
				
			memcpy(&rec[c->startpos], &number, sizeof(long));
		}
		else
		// --- BINARY
		if (c->datatype == ISAM_TYPE_BINARY) {
			unsigned char *bytea_lit = NULL;
			size_t lit_size = 0;
			
			// A new string is allocated which contains the actual binary data
			bytea_lit = PQunescapeBytea(c->value, &lit_size);
			
			if (! bytea_lit) {
				// Get the other columns
				continue;
			}
			
			// The size of the byte array cannot be greater than the field size
			if (lit_size > vallen) {
				pgout(0, "bytea mismatch in bridge schema column=[%s]",
					c->name);
				
				// Don't use str_free... binary data could include a NULL
				pg_free(bytea_lit);
				__return;
			}
			
			// Copy the bytea literal to the record
			memcpy(&rec[c->startpos], bytea_lit, lit_size);
			
			// Free the bytea literal
			pg_free(bytea_lit);

		} else
		// --- BOOLEAN
		if (c->datatype == ISAM_TYPE_BOOLEAN) {
			if (c->value[0] == 't')
				rec[c->startpos] = 'Y';
			else
			if (c->value[0] == 'f')
				rec[c->startpos] = 'N';
			else
				rec[c->startpos] = ' ';
		} else
		// --- CODE
		if ((c->datatype == ISAM_TYPE_CODE || c->datatype == ISAM_TYPE_CODEBLANK)
			&& c->codelength) {
			int startpos = c->startpos;

			if (vallen > c->codelength) {
				pgout(0, "length mismatch in bridge schema column=[%s]",
					c->name);
				__return;
			}
			
			// If numeric, start at the end of code minus the actual length
			if (str_is_block_numeric(c->value, strlen(c->value))) {
				startpos += (c->length - c->codelength);
			}
			
			memcpy(&rec[startpos], c->value, vallen);
		}
		// --- CHAR/VARCHAR
		else {
			if (vallen > c->length) {
				pgout(0, "length mismatch in bridge schema column=[%s]",
					c->name);
				__return;
			}
			
			memcpy(&rec[c->startpos], c->value, vallen);
		}

		c = c->next;
	}	
	
	*record = rec;
	
	__return;
	
} /* COLUMN_to_record */


/*
 * COLUMN_from_res [X]
 * Fills a column's values from a res
 * column		Pointer to object receiving value from res
 * res			Resource object containing values
 */
void COLUMN_from_res (COLUMN ** column, RES * res)
{
	COLUMN *c = *column;
	COLUMN *head;
	PGresult *pgres = res->pgres;
	char *colname;
	int colidx = 0;
	
__STACK(COLUMN_from_res)
	
	pgout(mDEBUG3, "column < res");
	
	// Save a pointer to the top of the list
	head = c;
	
	// Iterate through each RES column
	while ((colname = PQfname(pgres, colidx)) != (char *)NULL) {
		COLUMN *c2 = c;
		
		// Iterate through each column
		while (c2) {
			
			// Skip "phantom" columns
			if (c2->is_phantom) {
				c2 = c2->next;
				continue;
			}		
			
			// Found a match?  Save it...
			if (! strcmp(c2->name, colname)) {
				c2->value = (unsigned char *)strdup(PQgetvalue(pgres, 0, colidx));
				break;	
			}
			
			c2 = c2->next;
		}

		colidx++;
		
		// Can't go past the number of fields
		if (colidx == res->nfields) break;
	}
	
	*column = head;
	
	__return;
	
} /* COLUMN_from_res */


/*
 * COLUMN_delete [X]
 * Delete a COLUMN object
 * column		Pointer to the list to delete
 */
void COLUMN_delete (COLUMN ** column)
{
	COLUMN *c = *column;
	
__STACK(COLUMN_delete)
	
	while (c) {
		COLUMN *next = c->next;
		
		str_free(&c->name);
		str_free(&c->params);
		
		if (c->datatype == ISAM_TYPE_BINARY) {
			pg_free(c->value);
		} else {
			str_free((char **)&c->value);
		}
	
		xfree(c);
		
		c = next;
	}
	
	*column = c;
	
	__return;
	
} /* COLUMN_delete */


// _____/ SCHEMA functions \__________
/*
 * SCHEMA_assign_def [X]
 * Assign a schema definition
 * definition	Definition name (must have a *.def)
 */
bool SCHEMA_assign_def (SCHEMA ** schema, char * definition)
{
	SCHEMA *s = *schema;
	char *filepath = NULL;
	char *rptmp = NULL;
	char BUF[MAXBUFSZ];
	FILE *fd;
	int ignore_length = 0;
	unsigned int last_valid_startpos = 0;
	unsigned int last_valid_length = 0;
	int io, x;
	
__STACK(SCHEMA_assign_def)
	
	// Deal with CONV tables
	if (definition[strlen(definition)-1] == '*') {
		definition[strlen(definition)-1] = '\0';
		s->is_convertable = true;
	}
	
	// Check to see if it's a temporary file
	if (! strncmp(definition, "rptmp", 5)) {
		rptmp = str_dup(definition);	
	}
	
	str_append(&filepath, "%s/%s.def"
		,get_BRIDGE()
		,rptmp ? "rptmp" : definition
		);

	pgout(mDEBUG3, "definition=[%s] filepath=[%s]"
		,rptmp ? "rptmp" : definition
		,filepath
		);
	
	fd = fopen(filepath, "r");
	
	if (! fd) {
		pgout(0, "fopen failed for [%s]", filepath);		
		goto retbad;
	}
	
	s->name = rptmp ? str_dup("rptmp") : str_dup(definition);
	s->nocreate = false;
	
	// A pointer to the top of fielddef; will need to reset later
	while (fgets(BUF, MAXBUFSZ, fd)) {
		bool is_phantom = false;
		unsigned int LINENUM=0L;
		COLUMN *c = s->column;
		int coltype;
		char *cpBUF;
		char *name, *startpos, *length, *datatype,
			*codelength, *params = NULL, *idx;
		
		// Increment LINENUM
		LINENUM++;
		
		// Allow for comments and blank lines
		if ((BUF[0] == '#') || (BUF[0] == '\r') || (BUF[0] == '\n'))
			continue;	
		
		// Strip off CR/LFs
		if (BUF[strlen(BUF)-1] == '\n')
			BUF[strlen(BUF)-1] = '\0';
		
		if (BUF[strlen(BUF)-1] == '\r')
			BUF[strlen(BUF)-1] = '\0';
		
		// Make a copy of the original		
		cpBUF = (char *)strdup(BUF);

		name = BUF;
		
		// "phantom" columns
		if ((! strncmp(BUF, "phantom ", 8)) ||
			(! strncmp(BUF, "PHANTOM ", 8)) ) {
			is_phantom = true;
			name = &BUF[8];
		}
		
		// Look for SCHEMA-level directives
		if (! strncmp(BUF, "reclen=", 7)) {
			s->reclen = atoi(&BUF[7]);
			xfree(cpBUF);
			continue;
		}
		
		if (! strncmp(BUF, "pgname=", 7)) {
			s->pgname = str_dup(&BUF[7]);
			xfree(cpBUF);
			continue;
		}
		
		if (! strncmp(BUF, "prefix=", 7)) {
			s->prefix = str_dup(&BUF[7]);
			xfree(cpBUF);
			continue;
		}
		
		if (! strncmp(BUF, "modify=", 7)) {
			MODIFY_push(&s->modify, &BUF[7]);
			xfree(cpBUF);
			continue;
		}
		
		if (! strcmp(BUF, "nocreate")) {
			s->nocreate = true;
			xfree(cpBUF);
			continue;
		}
		
		if (! strncmp(BUF, "index ", 6)) {
			INDEX_append_node(&s->index, s->column, &BUF[6]);
			if (! s->index) {
				goto malformed;
			}
			xfree(cpBUF);
			continue;
		}
		
		// Look for params
		idx = (char *)index(BUF, '[');
		if (idx) {
			*idx = '\0'; *idx++;
			params = idx;
			
			// Check for closing brackets
			if (params[strlen(params)-1] == ']') {
				params[strlen(params)-1] = '\0';
			} else {
				goto malformed;
			}
		}
		
		idx = (char *)index(BUF, ':');
		if (! idx) goto malformed;
		*idx = '\0'; *idx++;
		
		startpos = idx;
		
		idx = (char *)index(startpos, ':');
		if (! idx) goto malformed;
		*idx = '\0'; *idx++;
		
		length = idx;
		
		idx = (char *)index(length, ':');
		if (! idx) goto malformed;
		*idx = '\0'; *idx++;
		
		datatype = idx;
		
		idx = (char *)index(datatype, ':');
		
		// OK not to have a "codelength" described
		if (idx) {
			*idx = '\0'; *idx++;
			codelength = idx;
		} else
			codelength = NULL;
		
		if (! atoi(length)) {
			pgout(0, "%s: line #%ld: length is zero in definition [%s]",
				filepath, LINENUM, cpBUF);
		}
		
		// Validate types
		if (! strlen(datatype)) {
			coltype = ISAM_TYPE_CHAR;
		}
		else
		if (! strcmp(datatype, "char")) {
			coltype = ISAM_TYPE_CHAR;
		}
		else
		if (! strcmp(datatype, "code")) {
			coltype = ISAM_TYPE_CODE;
		}
		else
		if (! strcmp(datatype, "codeblank")) {
			coltype = ISAM_TYPE_CODEBLANK;
		}
		else
		if (! strcmp(datatype, "decimal")) {
			coltype = ISAM_TYPE_DECIMAL;
		}
		else
		if (! strcmp(datatype, "integer")) {
			coltype = ISAM_TYPE_INTEGER;
		}
		else
		if (! strcmp(datatype, "binary")) {
			coltype = ISAM_TYPE_BINARY;
		}
		else
		if (! strcmp(datatype, "bool") || (! strcmp(datatype, "boolean"))) {
			coltype = ISAM_TYPE_BOOLEAN;
		}
		else {
			pgout(0, "%s: line #%ld: no such type [%s]",
				filepath, LINENUM, datatype);
			goto malformed;
		}

		// Free the temporary buffer
		xfree(cpBUF);
		
		// Special column name "IGNORE" means, skip length # of bytes on next record
		if (! strcmp(name, "IGNORE")) {
			ignore_length = atoi(length);
			continue;
		} else {
			COLUMN_push(
				&s->column,
				name,
				length,
				codelength,
				coltype,
				params);
		}
		
		// A phantom column should not interrupt
		// the startpos count
		if (c && is_phantom) {
			s->column->is_phantom = true;
			continue;
		}					
		
		if (c && (! strlen(startpos))) {
			s->column->startpos = last_valid_startpos + last_valid_length;
			s->column->startpos += ignore_length ? ignore_length : 0;
		} else {
			s->column->startpos = atoi(startpos);
		}
		
		last_valid_startpos = s->column->startpos;
		last_valid_length = s->column->length;
		
		ignore_length = 0;
		
		continue;

malformed:
		pgout(0, "%s: line #%ld: malformed definition [%s]",
			filepath, LINENUM, cpBUF);
		xfree(cpBUF);
	}
	
	// Close the input fd
	fclose(fd);
	
	// If we didn't set it explicitly
	// Create the postgres field name
	if (! s->pgname) {
		asprintf(&s->pgname, "%s%s",
			s->prefix ? s->prefix : "",
			rptmp ? rptmp : definition);
	}
	
	// Append for clone & tblcnv
	if (s->is_convertable && append_convert) {
		str_append(&s->pgname, "_conv");
	}
		
	str_free(&rptmp);
	
	// Is the schema pivotable?
	if (! strncmp(s->name, "tables", 6)) {
		s->is_pivotable = true;
	}
	
	// Reverse the COLUMN list so we can iterate it in order
	COLUMN_reverse(&s->column);
	
	// Do the same with the index
	INDEX_reverse(&s->index);
	
	*schema = s;
	
	xfree(filepath);
	__return true;
	
retbad:
	xfree(filepath);
	__return false;
	
} /* SCHEMA_assign_def */


/*
 * SCHEMA_push [X]
 * Create a new schema on the front of the list
 * schema		Pointer to the head of the list
 * definition	Definition name (must have a *.def)
 */
void SCHEMA_push (SCHEMA ** schema, char * definition)
{
	SCHEMA *new_element;
	SCHEMA *s = *schema;
	
__STACK(SCHEMA_push)
	
	// Check to see if the definition matches an
	// existing schema (SCHEMAs must be unique)
	if (SCHEMA_get(s, definition) != (SCHEMA *)NULL) {
		pgout(mDEBUG3, "schema %s exists", s->name);
		__return;
	}
	
	new_element = (SCHEMA *)xalloc(sizeof(SCHEMA));
	
	// Add data elements here
	if (! SCHEMA_assign_def(&new_element, definition)) {
		pgout(0, "SCHEMA_push: unable to assign definition to schema");
	}
	
	new_element->next = *schema;

	*schema = new_element;
	
	__return;
	
} /* SCHEMA_push */


/*
 * SCHEMA_print [X]
 * Print a SCHEMA type to stdout
 * schema	 Pointer to the list to print
 * label	 Label to identify the print list
 */
char * SCHEMA_print (SCHEMA * schema, char * label)
{
	SCHEMA *s = schema;
	char *b = NULL;
	
__STACK(SCHEMA_print)
	
	while (s) {
		char *b1 = NULL, *b2 = NULL, *b3 = NULL;
		
		// Print the table info
		str_append(&b, "TABLE %s : %s {\n\tpgname=[%s] reclen=[%d]\n",
					s->name, label, s->pgname, s->reclen);

		// Print the columns
		b1 = COLUMN_print(s->column, s->name, 1);
		
		// Print the modifier
		b2 = MODIFY_print(s->modify, s->name);
		
		// Print the indexes
		b3 = INDEX_print(s->index, s->name);
		
		str_append(&b, "%s%s%s\n}\n", b1, b2, b3);
		
		str_free(&b1);
		str_free(&b2);
		str_free(&b3);
					
		s = s->next;
	}
	
	__return b;
	
} /* SCHEMA_print */


/*
 * SCHEMA_get [X]
 * Return a schema matching definition
 * current		Pointer to the current list
 * definition	Definition name (must have a *.def)
 */
SCHEMA * SCHEMA_get (SCHEMA * schema, char * definition)
{
	SCHEMA *s = schema;
	char *real_def = NULL;
	
__STACK(SCHEMA_get)
	
	if (! strncmp(definition, "rptmp", 5)) {
		real_def = str_dup("rptmp");
	} else {
		real_def = str_dup(definition);
	}
	
	while (s) {
		if (! strcmp(s->name, real_def)) {
			str_free(&real_def);
			__return s;
		}	
		s = s->next;
	}
	
	str_free(&real_def);
	
	__return (SCHEMA *)NULL;
		
} /* SCHEMA_get */


/*
 * SCHEMA_create_insert [X]
 * Create an INSERT sql statement from SCHEMA
 * schema		Pointer to object containing values
 */
char * SCHEMA_create_insert (CONTEXT * context)
{
	SCHEMA *s = context->schema;
	COLUMN *c;
	char *sql = NULL;
	char *sql_col = NULL;
	char *sql_val = NULL;
	
__STACK(SCHEMA_create_insert)
	
	c = s->column;
	
	while (c) {
		if (c->value) {
			str_append(&sql_col, "%s,", c->name);
			// Note: the "E" indicates a new literal byte escape
			// syntax for PostgreSQL 8.2.x
			if (c->datatype == ISAM_TYPE_BOOLEAN && (! strcmp(c->value, "null"))) {
				str_append(&sql_val, "null,", c->value);
			} else
			if (c->datatype == ISAM_TYPE_DECIMAL) {
				str_append(&sql_val, "%s,", c->value);
			} else
				str_append(&sql_val, "E'%s',", c->value);
		}
		
		c = c->next;
	}
	
	str_trim_char(&sql_col, ',');
	str_trim_char(&sql_val, ',');
	
	str_append(&sql,
		"INSERT INTO %s ( %s ) VALUES ( %s )"
		,s->pgname
		,sql_col
		,sql_val
		);
	
	str_free(&sql_col);
	str_free(&sql_val);
		
	__return sql;
	
} /* SCHEMA_create_insert */


/*
 * SCHEMA_create_update [X]
 * Create an UPDATE sql statement from SCHEMA
 * context		Pointer to the current context
 */
char * SCHEMA_create_update (CONTEXT * context)
{
	SCHEMA *s = context->schema;
	COLUMN *c;
	char *sql = NULL;
	char *sql_set = NULL;
	
__STACK(SCHEMA_create_update)
	
	c = s->column;
	
	// Iterate through each COLUMN in the SCHEMA
	while (c) {
		// If it has a value, update it		
		if (c->value) {
			if (c->datatype == ISAM_TYPE_BOOLEAN && (! strcmp(c->value, "null"))) {
				str_append(&sql_set, " %s=null,", c->name);
			} else
			if (c->datatype == ISAM_TYPE_DECIMAL) {
				str_append(&sql_set, "%s=%s,", c->name, c->value);
			} else
				str_append(&sql_set, " %s='%s',", c->name, c->value);
		}
		
		c = c->next;
	}
	
	str_trim_char(&sql_set, ',');
	
	// Always update the table by its primal key
	str_append(&sql,
		"UPDATE %s SET%s WHERE oid='%s'"
		,s->pgname
		,sql_set
		,context->oid_last
		);
		
	str_free(&sql_set);
	
	pgout(mDEBUG3, "sql=[%s]", sql);
	
	__return sql;
	
} /* SCHEMA_create_update */


/*
 * SCHEMA_delete [X]
 * Delete a SCHEMA object
 * schema		Pointer to the list to print
 */
void SCHEMA_delete (SCHEMA ** schema)
{
	SCHEMA *s = *schema;
	
__STACK(SCHEMA_delete)
	
	while (s) {
		SCHEMA *next = s->next;
		
		str_free(&s->name);
		str_free(&s->pgname);
		str_free(&s->prefix);
		
		INDEX_delete(&s->index);
		COLUMN_delete(&s->column);
		MODIFY_delete(&s->modify);
				
		xfree(s);
	
		s = next;	
	}
	
	*schema = NULL;
	
	__return;
	
} /* SCHEMA_delete */


/*
 * SCHEMA_shutdown [X]
 * Delete resources associated with the schema module
 */
void SCHEMA_shutdown (void)
{
__STACK(SCHEMA_shutdown)

	pgout(mDEBUG3, "shutting down schema");
	
	str_free(&connstr);
	connstr = NULL;
	
	str_free(&set_schema);
	set_schema = NULL;
	
	__return;
	
} /* SCHEMA_shutdown */


/*
 * SCHEMA_pivot [x]
 * Pivoting a schema is unique to c-isam file "tables",
 * Where a schema must be adapted based on the "tables type".
 */
SCHEMA * SCHEMA_pivot (SCHEMA * schema, char * record)
{
	char str_comp[10];
	SCHEMA *s = schema;
	SCHEMA *tables_default = NULL;
	
__STACK(SCHEMA_pivot)
	
	sprintf(str_comp, "tables_%c%c", tolower(record[0]), tolower(record[1]));
	
	while (s) {

		if (! strcmp(s->name, str_comp)) {

			pgout(mDEBUG2, "pivoting tables schema to [%s]",
				str_comp);
				
			__return s;	
		}
				
		if (! strcmp(s->name, "tables")) {
			// Save it for later
			tables_default = s;
		}
		
		s = s->next;
	}

	if (tables_default) {	
		pgout(mDEBUG2, "pivoting tables schema to [tables]");
	}
		
	__return tables_default;
	
} /* SCHEMA_pivot */


// _____/ STMT functions \__________
/*
 * STMT_append [X]
 * Add a to top of list
 * stmt			Pointer to the tail of the list
 * column		Name of the column
 * operator		SQL operator
 * value		Value of the column
 */
void STMT_append (STMT ** stmt, char * column, char * operator,
	char * value, unsigned int datatype)
{
	STMT *current = *stmt;
	STMT *new_element = xalloc(sizeof(STMT));
	
__STACK(STMT_append)

	new_element->column = str_dup(column);
	new_element->operator = str_dup(operator);
	new_element->value = str_dup(value);
	new_element->datatype = datatype;
	
	new_element->next = *stmt;
	
	if (current == NULL) {
		new_element->head = new_element;
		new_element->next = NULL;
	} else {
		current->next = new_element;
		new_element->head = current->head;
		new_element->next = NULL;
	}	
	
	*stmt = new_element;
	
	__return;

} /* STMT_append */


/*
 * STMT_print [X]
 * Print a STMT type
 * stmt			Pointer to the list to print
 * label		Label to identify the print list
 */
char * STMT_print (STMT * stmt, char * label)
{
	STMT *st = stmt;
	char *b = NULL;
	
__STACK(STMT_print)
	
	while (st) {
		
		str_append(&b, "\tSTMT: %s {\n\t\tcolumn=[%s]\n\t\toperator=[%s]"
			"\n\t\tvalue=[%s]\n\t}\n"
			,label
			,st->column
			,st->operator
			,st->value
			);
			
		st = st->next;
	}
	
	__return b;

} /* STMT_print */


/*
 * STMT_delete [X]
 * Delete a STMT object
 * stmt		Pointer to the list to delete
 */
void STMT_delete (STMT ** stmt)
{
	STMT *st = *stmt;
	
__STACK(STMT_delete)
	
	while (st) {
		STMT *next = st->next;
		
		str_free(&st->column);
		str_free(&st->operator);
		str_free(&st->value);
		
		xfree(st);
		
		st = next;
	}
	
	*stmt = st;
	
	__return;
	
} /* STMT_delete */


// _____/ MODIFY functions \__________
/*
 * MODIFY_push [X]
 * Add a MODIFY to top of list
 * modify		Pointer which will allocated
 * definition	Definition name
 */
void MODIFY_push (MODIFY ** modify, char * definition)
{
	MODIFY *new_element = xalloc(sizeof(MODIFY));
	
__STACK(MODIFY_push)
	
	new_element->definition = str_dup(definition);
	
	new_element->next = *modify;
	*modify = new_element;
	
	__return;

} /* MODIFY_push */


/*
 * MODIFY_print [X]
 * Print a MODIFY type to stdout
 * modify		Pointer to the list to print
 * label		Label to identify the print list
 */
char * MODIFY_print (MODIFY * modify, char * label)
{
	MODIFY *m = modify;
	char *b = NULL;
	
__STACK(MODIFY_print)
	
	while (m) {
		
		str_append(&b, "\tMODIFY : %s {\n\t\tdefinition=[%s]\n\t}\n"
			,label
			,m->definition
			);
			
		m = m->next;
	}
	
	__return b;

} /* MODIFY_print */


/*
 * MODIFY_delete [X]
 * Delete a MODIFY object
 * modify		Pointer to the list to delete
 */
void MODIFY_delete (MODIFY ** modify)
{
	MODIFY *m = *modify;
	
__STACK(MODIFY_delete)
	
	while (m) {
		MODIFY *next = m->next;
		
		str_free(&m->definition);
		
		xfree(m);
		
		m = next;
	}
	
	*modify = m;
	
	__return;
	
} /* MODIFY_delete */


// _____/ INDEX functions \__________

/*
 * INDEX_push [X]
 * Create a new index on the front of the list
 * index		Pointer to the head of the list
 * schema_col	Pointer to the SCHEMA's columns
 * definition	Index definition string
 */
void INDEX_push (INDEX ** index, COLUMN * schema_col, char * definition)
{
	INDEX *new_element = xalloc(sizeof(INDEX));
	COLUMN *c;
	char *name, *is_unique;
	char *start, *end;
	
__STACK(INDEX_push)
	
	pgout(mDEBUG3, "definition=[%s]", definition);
	
	// Parse the definition
	start = end = name = definition;
	
	start = str_part(&end, '=');
	
	// Get column names	
	while (start = str_part(&end, ',')) {
		
		// See if there is a modifier
		is_unique = strchr(start, '[');

		if (! is_unique) {
			new_element->is_unique = false;
		} else {
			*is_unique = '\0'; *is_unique++;
			if (! strcmp(is_unique, "UNIQUE]")) {
				new_element->is_unique = true;
			}				
		}
		
		// Get the SCHEMA column matching the name
		c = COLUMN_get(schema_col, start);
		
		if (! c) {
			pgout(0, "%s: [%s] is not a valid column", name, start);
			if (start == end) break;
			start = end;
			continue;
		}
		
		COLUMN_push_copy(&new_element->column, c);

		if (start == end) break;
		start = end;
	}
	
	if (! new_element->column) {
		pgout(0, "%s: does not specify any columns", name);

		INDEX_delete(&new_element);

		__return;
	}
	
	new_element->name = str_dup(name);
		
	new_element->next = *index;

	*index = new_element;
	
	__return;
	
} /* INDEX_push */


/*
 * INDEX_append_node
 * Create a new index at the tail of the list
 * index		Pointer to the head of the list
 * schema_col	Pointer to the SCHEMA's columns
 * definition	Index definition string
 */
void INDEX_append_node (INDEX ** index, COLUMN * schema_col, char * definition)
{
	INDEX *current = *index;
	INDEX *new_element = xalloc(sizeof(INDEX));
	COLUMN *c;
	char *name, *is_unique;
	char *start, *end;
	
__STACK(INDEX_append_node)
	
	pgout(mDEBUG3, "definition=[%s]", definition);
	
	// Parse the definition
	start = end = name = definition;
	
	start = str_part(&end, '=');
	
	// Get column names	
	while (start = str_part(&end, ',')) {
		
		// See if there is a modifier
		is_unique = strchr(start, '[');

		if (! is_unique) {
			new_element->is_unique = false;
		} else {
			*is_unique = '\0'; *is_unique++;
			if (! strcmp(is_unique, "UNIQUE]")) {
				new_element->is_unique = true;
			}
		}
		
		// Get the SCHEMA column matching the name
		c = COLUMN_get(schema_col, start);
		
		if (! c) {
			pgout(0, "%s: [%s] is not a valid column", name, start);
			if (start == end) break;
			start = end;
			continue;
		}
		
		COLUMN_push_copy(&new_element->column, c);

		if (start == end) break;
		start = end;
	}
	
	if (! new_element->column) {
		pgout(0, "%s: does not specify any columns", name);

		INDEX_delete(&new_element);

		__return;
	}

	new_element->name = str_dup(name);
	
	if (current == NULL) {
		new_element->num = 1;
		*index = new_element;
	} else {
		while (current->next != NULL) {
			current = current->next;
		}
		
		new_element->num = current->num + 1;
		current->next = new_element;
	}
	
	__return;

} /* INDEX_append_node */


/*
 * INDEX_get [X]
 * Return an INDEX matching num
 * index		Pointer to the list
 * num			Integer to match
 */
INDEX * INDEX_get (INDEX * index, int num)
{
	INDEX *i = index;
	
__STACK(INDEX_get)
	
	while (i) {
		if (i->num == num) {
			__return i;
		}
		i = i->next;
	}
	
	__return (INDEX *)NULL;

} /* INDEX_get */


/*
 * INDEX_print [X]
 * Print a INDEX type to stdout
 * index		Pointer to the list to print
 * label		Label to identify the print list
 */
char * INDEX_print (INDEX * index, char * label)
{
	INDEX *i = index;
	char *b = NULL;
	
__STACK(INDEX_print)
	
	while (i) {
		
		str_append(&b, "\tINDEX %s : %s {\n\t\tis_unique=[%s]\n\t\tnum=[%d]\n"
			,i->name
			,label
			,i->is_unique ? "true" : "false"
			,i->num
			);
		
		// Print the columns
		if (i->column) {
			char *b2 = NULL;
			
			b2 = COLUMN_print(i->column, i->name, 2);
			
			str_append(&b, "%s", b2);
			
			str_free(&b2);
		}
		
		str_append(&b, "\t}\n");
			
		i = i->next;
	}
	
	__return b;

} /* INDEX_print */


/*
 * INDEX_get_keydesc [X]
 * Retreive an INDEX matching C-ISAM keydesc
 * index		Pointer to the list head
 * key			Keydesc containing structure to match against index
 */
INDEX * INDEX_get_keydesc (INDEX * index, struct keydesc * key)
{
	INDEX *i = index;
	int x;
	
__STACK(INDEX_get_keydesc)
	
	// Index must exist
	if (! i) {
		__return (INDEX *)NULL;
	}
	
	pgout(mDEBUG3, "getting schema index from keydesc");
	
	// Iterate through each index
	while (i) {
		COLUMN *c = i->column;
		int searching = 0;
		bool satisfied = false;
		bool tryagain = false;
		
		pgout(mDEBUG3, "looking through index [%s]", i->name);
		
		if (! key) {
			pgout(0, "error: keydesc for index beginning with [%s] is empty",
				i->name); 
		}
		
		// If the first column doesn't match the starting position of the
		// first key part, we can skip the rest and try the next index
		if (c->startpos != (unsigned int)key->k_part[searching].kp_start) {
			i = i->next;
			continue;
		}
		
		// Each column in the index
		while (c) {
			int typelength = (c->datatype == ISAM_TYPE_INTEGER) ? 2 : c->length;
			unsigned int keystart;
			unsigned int keyend;
			
			keystart = key->k_part[searching].kp_start;
			keyend = keystart + key->k_part[searching].kp_leng;
			
			if (c->startpos >= keystart &&
				((c->startpos + typelength) <= keyend)) {
				satisfied = true;				
			} else {
				satisfied = false;
				
				// This is to skip an infinite loop where there is a column
				// to key mismatch
				if (tryagain) {
					break;
				} else {
					tryagain = true;
				}
				
				// Try again, but search the next key part, column may
				// match it
				searching++;
				continue;
			}
			
			c = c->next;
		}
		
		if (satisfied) {
			if (searching < key->k_nparts) {
				__return i;
			} else {
				satisfied = false;
			}
		}
		
		i = i->next;

	} //__________________ INDEX

	__return (INDEX *)NULL;
	
} /* INDEX_get_keydesc */


/*
 * INDEX_delete [X]
 * Delete an INDEX object
 * index		Pointer to the list to delete
 */
void INDEX_delete (INDEX ** index)
{
	INDEX *i = *index;
	
__STACK(INDEX_delete)
	
	while (i) {
		INDEX *next = i->next;
		
		str_free(&i->name);
		
		COLUMN_delete(&i->column);
		
		xfree(i);
		
		i = next;
	}
	
	*index = i;
	
	__return;
	
} /* INDEX_delete */


/*
 * INDEX_reverse
 * Reverse an INDEX list
 * index		Pointer to the index head
 */
void INDEX_reverse (INDEX ** index)
{
	INDEX *result = NULL;
	INDEX *current = *index;
	INDEX *next;
	
__STACK(INDEX_reverse)
	
	while (current != NULL) {
		next = current->next;
		current->next = result;
		result = current;
		
		// Reverse the COLUMN list so we can iterate it in order
		COLUMN_reverse(&current->column);
		
		current = next;
	}
	
	*index = result;
	
	__return;

} /* INDEX_reverse */


// _____/ CONTEXT functions \__________

static int CONTEXT_fdpool_get (void)
{
	int x;
	
__STACK(CONTEXT_fdpool_get)
	
	if (! is_fdpool_initialized) {
		for (x=1; x < MAXFDS; x++) {
			fdpool[x] = 0;
		}

		is_fdpool_initialized = true;	
		__return fdpool[0] = 1;
	}
	
	for (x=0; x < MAXFDS; x++) {
		if (! fdpool[x]) {
			pgout(mDEBUG3, "issuing fd #%d", x+1);
			__return fdpool[x] = x+1;
		}
	}
	
	__return 0; // All out of fd's... bad
	
} /* CONTEXT_fdpool_get */


static void CONTEXT_fdpool_delete (int fd)
{
__STACK(CONTEXT_fdpool_delete)

	if (fd <= 0) {
		__return;
	}
	
	pgout(mDEBUG3, "deleting fd #%d", fd);
	fdpool[fd - 1] = 0;
	
	__return;

} /* CONTEXT_fdpool_delete */


/*
 * CONTEXT_push [X]
 * Create a new context on the front of the list
 * current		Pointer to the current list
 * schema		Schema associated with the context
 */
void CONTEXT_push (CONTEXT ** current, SCHEMA * schema)
{
	CONTEXT *new_element = xalloc(sizeof(CONTEXT));
	
__STACK(CONTEXT_push)
		
	new_element->id = context_id++;		// Increment the context id
	new_element->isfd = CONTEXT_fdpool_get();
	new_element->schema = schema;		// Point to the right schema
	
	new_element->next = *current;
	*current = new_element;
	
	__return;
	
} /* CONTEXT_push */


/*
 * CONTEXT_get [X]
 * Return a context matching C-ISAM bridge file descriptor
 * current		Pointer to the current list
 * isfd			C-ISAM bridge file descriptor
 */
CONTEXT * CONTEXT_get (CONTEXT * current, int isfd)
{
	CONTEXT *cx = current;
	
__STACK(CONTEXT_get)
	
	if (! cx) {
		__return (CONTEXT *)NULL;
	}
	
	while (cx) {
		if (cx->isfd == isfd) {
			__return cx;
		}	
		cx = cx->next;
	}
	
	__return (CONTEXT *)NULL;
	
} /* CONTEXT_get */


/*
 * CONTEXT_delete_node [X]
 * Delete a context node from a list
 * list		Pointer to the context list
 * delnode	POinter to the node to delete
 */
void CONTEXT_delete_node (CONTEXT ** list, CONTEXT * delnode)
{
	CONTEXT *c;
	CONTEXT *head, *next, *prev;
	
__STACK(CONTEXT_delete_node)
	
	prev = c = *list;				// De-ref the list ptr

	if (c == delnode) {				// IF delnod equal to the head THEN
		head = c->next;				// the list head belongs on next
	} else {
		head = c;					// ELSE the list head belongs where it is
	}
	
	// Iterate through the list
	while (c) {
		next = c->next;				// Save a copy of the next ptr
		
		if (c == delnode) {			// IF current ptr matches the node
			
			pgout(mDEBUG3, "deleting context node isfd=[%d] cursor_name=[%s]"
				,c->isfd
				,c->cursor_name ? c->cursor_name : "NONE"
				);
			
			prev->next = c->next;	// Unlink the node
			
			// If context is in a cursor, close the cursor
			if (c->cursor_name) {
				RES *tmpres;
				char *tmpsql = NULL;
				
				str_append(&tmpsql,
					"CLOSE %s"
					,c->cursor_name
					);
				
				if ((tmpres = pg_exec(c->conn, tmpsql)) == (RES *)NULL) {
					pgout(0, "unable to close cursor=[%s], isfd=%d, schema=[%s]",
						c->cursor_name, c->isfd, c->schema->name);
				}
			
				RES_delete(&tmpres);
				str_free(&tmpsql);
				str_free(&c->cursor_name);
			}
			
			// Let fd go back into the pool
			CONTEXT_fdpool_delete(c->isfd);
			
			// Free node resources
			str_free(&c->oid_last);
			str_free(&c->sql_last);
			str_free(&c->sql_temp);
			str_free(&c->cursor_name);
			
			xfree(c);				// Free it
			break;					// That's all, take a break
		}

		prev = c;					// Save the previous ptr (for unlinking)
		
		c = next;					// Advance the list ptr
	}
	
	*list = head;					// Re-ref back to list head
	
	__return;
	
} /* CONTEXT_delete_node */


/*
 * CONTEXT_delete [X]
 * Delete an entire context list
 * list		Pointer to the context list
 */
void CONTEXT_delete (CONTEXT ** context)
{
	CONTEXT *c = *context;
	
__STACK(CONTEXT_delete)
	
	// Iterate through the list
	while (c) {
		CONTEXT *next = c->next;	// Save a copy of the next ptr
		
		// Let fd go back into the pool
		CONTEXT_fdpool_delete(c->isfd);
		
		str_free(&c->oid_last);
		str_free(&c->sql_last);
		str_free(&c->sql_temp);
		str_free(&c->cursor_name);
		
		xfree(c);
		
		c = next;					// Advance the list ptr
	}
	
	*context = NULL;				// Re-ref back to list head
	
	__return;
	
} /* CONTEXT_delete */


/*
 * CONTEXT_print [X]
 * Print a CONTEXT type to stdout
 * context		Pointer to the list to print
 * label		Label to identify the print list
 */
char * CONTEXT_print (CONTEXT * context, char * label)
{
	CONTEXT *cx = context;
	char *b = NULL;
	
__STACK(CONTEXT_print)
	
	while (cx) {
		
		str_append(&b, "\tCONTEXT %ld : %s {\n\t\tisfd=[%d] schema=[%s] cx->cursor_name=[%s]\n\t}\n"
			,cx->id
			,label
			,cx->isfd
			,cx->schema->name
			,cx->cursor_name ? cx->cursor_name : "NONE"
			);

		cx = cx->next;
	}
	
	__return b;

} /* CONTEXT_print */
