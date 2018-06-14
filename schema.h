/*
 * schema.h: C-ISAM schema definitions for Postgres
 */

#ifndef _SCHEMA_H
#define _SCHEMA_H

// TYPE DEFINITIONS

/*
 * STMT
 * Holds SQL statement components
 */
typedef struct STMT_T {
	char *column;
	char *operator;
	char *value;
	unsigned int datatype;
	struct STMT_T *head;
	struct STMT_T *next;
} STMT;

/*
 * CONN
 * Holds Postgres connection info
 */
typedef struct CONN_T {
	PGconn *pgconn;			// Postgres data connection
	bool in_transaction;	// Is the connection in a transaction state?
	bool is_connected;		// Flag indicating connection state
} CONN;

/*
 * RES
 * Holds Postgres resource info
 */
typedef struct RES_T {
	int tuples;				// Tuples returned by query
	int nfields;			// Number of fields
	PGresult *pgres;
} RES;

/*
 * MODIFY
 * Holds SQL modifiers (i.e. ALTER TABLE...)
 */
typedef struct MODIFY_T {
	char *definition;		// SQL statment modifying the table schema
	struct MODIFY_T *next;
} MODIFY;

/*
 * COLUMN
 * Holds column definitions
 */
typedef struct COLUMN_T {
	char *name;				// Name of the field
	char *params;			// Postgres parameters
	bool is_phantom;		// Phantom columns
	unsigned char *value;	// Storage for values
	unsigned int startpos;	// Starting offset
	unsigned int length;	// Length (always should be 12)
	unsigned int codelength;// Significant bytes in code 
	unsigned int datatype;	// Data type (ISAM_TYPE_CHAR|DECIMAL|CODE)
	size_t sz_value;		// Size of value storage (used for bytea conversions)
	struct COLUMN_T *prev;
	struct COLUMN_T *next;
} COLUMN;

/* 
 * INDEX
 * Defines properties for an index
 */
typedef struct INDEX_T {
	char *name;				// Name of the index (will create this name in PG)
	bool is_unique;			// Is the index unique?
	int num;				// The index #
	COLUMN *column;			// Columns making up the index (col must exist)
	struct INDEX_T *next;
} INDEX;

/*
 * SCHEMA
 * Holds schema definition for tables
 */
typedef struct SCHEMA_T {
	char *name;				// Table name
	char *pgname;			// Postgres table name (normally ecn_*)
	char *prefix;			// Table prefix
	bool is_convertable;	// Will have the word CONVERT appended
	bool is_pivotable;		// Is the schema pivotable (i.e. "tables*")?
	bool nocreate;			// Do we skip "CREATE TABLE" on isbuild [DEFAULT=no]?
	unsigned int reclen;	// Length of the C-ISAM record
	INDEX *index;			// Index definition list
	COLUMN *column;			// Column definition list
	MODIFY *modify;			// SQL modifiers
	struct SCHEMA_T *next;
} SCHEMA;

/*
 * CONTEXT
 * Holds context information based on C-ISAM file descriptors
 */
typedef struct CONTEXT_T {
	bool trans_cursor;		// Is the cursor "WITH HOLD" (no trans) or "WITHOUT HOLD" (trans)?
	bool in_read;			// Has the cursor been read from?
	bool special_case;		// Flag for special case scenarios
	bool reverse_direction;	// Should the cursor read in reverse direction?
	CONN *conn;				// Pointer to the current connection the context is using
	char *cursor_name;		// Name of the current cursor associated w/the context
	char *sql_last;			// Stores the sql stmt associated with the cursor declaration
	char *oid_last;			// Holds the last OID obtained by isread
	char *sql_temp;			// Stores extended sql clauses for temporary use later
	int isfd;				// C-ISAM bridge file descriptor
	int mode;				// isstart mode associated with the cursor
	INDEX *index;			// Pointer to the index used by the last isstart
	SCHEMA *schema;			// Pointer to the schema
	unsigned long id;		// Cursor ID
	struct CONTEXT_T *next;	
} CONTEXT;


// FUNCTION PROTOTYPES


// _____/ CONN functions \__________
/*
 * CONN_new
 * Create a new connection to a Postgres database
 */
CONN * CONN_new (void);

/*
 * CONN_delete
 * Delete a connection (NOTE: since CONN is reusable, certain members stay intact)
 * conn			Connection object
 */
bool CONN_delete (CONN * conn);


/*
 * CONN_begin | commit | rollback
 * Transaction control functions
 * conn			Connection object
 */
bool CONN_begin (CONN * conn);
bool CONN_commit (CONN * conn);
bool CONN_rollback (CONN * conn);


// _____/ RES functions \__________
/*
 * RES_delete
 * Delete a resource
 * res			Pointer to the resource
 */
void RES_delete (RES ** res);

/*
 * RES_print
 * Print the columns/rows in a res
 * res			Pointer to the resource
 */
void RES_print (RES * res);

/*
 * RES_get_oid
 * Obtain an oid from a RES
 * res			Pointer to the resource
 * oidstr		Pointer to the oid
 */
void RES_get_oid (RES * res, char ** oidstr);


// _____/ COLUMN functions \__________
/*
 * COLUMN_push
 * Create a new column on the front of the list
 * column		Pointer to the head of the list
 * name			Column name
 * startpos		Starting position in the record
 * length		Length of the column
 * codelength	If a code field, significant portion of the column
 * coltype		Type of column (ISAM_TYPE_CHAR|DECIMAL|CODE)
 */
void COLUMN_push (COLUMN ** column, char * name, char * length,
	char * codelength, int coltype, char * params);
	
/*
 * COLUMN_reverse
 * Reverse a COLUMN list
 * column		Pointer to the column head
 */
void COLUMN_reverse (COLUMN ** column);

/*
 * COLUMN_push_copy
 * Add a copy of a column to another list
 * to			Pointer which will allocated and copied into
 * from			Pointer containing structure to copy
 */
void COLUMN_push_copy (COLUMN ** to, COLUMN * from);

/*
 * COLUMN_print
 * Print a COLUMN type to stdout
 * coldef		Pointer to the list to print
 * label		A label to identify the print list
 * indent_level	Level of indentation (\t)
 */
char * COLUMN_print (COLUMN * column, char * label, int indent_level);

/*
 * COLUMN_get
 * Retrieve a column by it's name
 * column		Pointer to the list to print
 * name			Name of the column
 */
COLUMN * COLUMN_get (COLUMN * column, char * name);

/*
 * COLUMN_clean
 * Clean a column's values
 * column		Pointer to the object to clean
 */
void COLUMN_clean (COLUMN * column);

/*
 * COLUMN_from_record
 * Fills a column's values from record
 * column		Pointer to object receiving values from record
 * record		Generic record pointer containting values
 */
void COLUMN_from_record (COLUMN * column, char * record);

/*
 * COLUMN_to_record
 * Fills a record from COLUMN
 * column		Pointer to object containing values
 * record		Generic record pointer receiving values
 */
void COLUMN_to_record (COLUMN * column, char ** record);

/*
 * COLUMN_from_res
 * Fills a column's values from a res
 * column		Pointer to object receiving value from res
 * res			Resource object containing values
 */
void COLUMN_from_res (COLUMN ** column, RES * res);

/*
 * COLUMN_delete
 * Delete a COLUMN object
 * column		Pointer to the list to delete
 */
void COLUMN_delete (COLUMN ** column);


// _____/ SCHEMA functions \__________
/*
 * SCHEMA_push
 * Create a new schema on the front of the list
 * schema		Pointer to the head of the list
 * definition	Definition name (must have a *.def)
 */
void SCHEMA_push (SCHEMA **schema, char *definition);

/*
 * SCHEMA_get
 * Return a schema matching definition
 * current		Pointer to the current list
 * definition	Definition name (must have a *.def)
 */
SCHEMA * SCHEMA_get (SCHEMA *current, char *definition);

/*
 * SCHEMA_print
 * Print a SCHEMA type to stdout
 * schema		Pointer to the list to print
 * label		Label to identify the print list
 */
char * SCHEMA_print (SCHEMA *schema, char *label);

/*
 * SCHEMA_create_insert
 * Create an INSERT sql statement from context's schema
 * schema		Pointer to object containing values
 */
char * SCHEMA_create_insert (CONTEXT *context);

/*
 * SCHEMA_create_update [X]
 * Create an UPDATE sql statement from SCHEMA
 * context		Pointer to the current context
 */
char * SCHEMA_create_update (CONTEXT *context);

/*
 * SCHEMA_delete
 * Delete a SCHEMA object
 * schema		Pointer to the list to delete
 */
void SCHEMA_delete (SCHEMA **schema);

/*
 * SCHEMA_pivot [x]
 * Pivoting a schema is unique to c-isam file "tables",
 * Where a schema must be adapted based on the "tables type".
 */
SCHEMA * SCHEMA_pivot (SCHEMA *schema, char *record);


// _____/ STMT functions \___________
/*
 * STMT_append
 */
void STMT_append (STMT **stmt, char *column, char *operator,
	char *value, unsigned int datatype);

/*
 * STMT_print
 */
char * STMT_print (STMT *stmt, char *label);

/*
 * STMT_delete
 * Delete a STMT object
 */
void STMT_delete (STMT **stmt);


// _____/ MODIFY functions \___________
/*
 * MODIFY_push
 * Add a MODIFY to top of list
 * modify		Pointer which will allocated
 * definition	Definition name
 */
void MODIFY_push (MODIFY **modify, char *definition);

/*
 * MODIFY_print
 * Print a MODIFY type to stdout
 * modify		Pointer to the list to print
 * label		Label to identify the print list
 */
char * MODIFY_print (MODIFY *modify, char *label);

/*
 * MODIFY_delete
 * Delete a MODIFY object
 * modify		Pointer to the list to delete
 */
void MODIFY_delete (MODIFY **modify);


// _____/ INDEX functions \___________
/*
 * INDEX_push
 * Create a new index on the front of the list
 * index		Pointer to the head of the list
 * schema_col	Pointer to the SCHEMA's columns
 * definition	Index definition string
 */
void INDEX_push (INDEX **index, COLUMN *schema_col, char *definition);

/*
 * INDEX_append_node
 * Create a new index at the tail of the list
 * index		Pointer to the head of the list
 * schema_col	Pointer to the SCHEMA's columns
 * definition	Index definition string
 */
void INDEX_append_node (INDEX **index, COLUMN *schema_col, char *definition);

/*
 * INDEX_get [X]
 * Return an INDEX matching num
 * index		Pointer to the list
 * num			Integer to match
 */
INDEX * INDEX_get (INDEX *index, int num);

/*
 * INDEX_print
 * Print a INDEX type to stdout
 * index		Pointer to the list to print
 * label		Label to identify the print list
 */
char * INDEX_print (INDEX *index, char *label);

/*
 * INDEX_get_keydesc
 * Retreive an INDEX matching C-ISAM keydesc
 * index		Pointer to the list head
 * key			Keydesc containing structure to match against index
 */
INDEX * INDEX_get_keydesc (INDEX *index, struct keydesc *keydesc);

/*
 * INDEX_delete
 * Delete an INDEX object
 * index		Pointer to the list to delete
 */
void INDEX_delete (INDEX **index);

/*
 * INDEX_reverse
 * Reverse an INDEX list
 * index		Pointer to the index head
 */
void INDEX_reverse (INDEX **index);


// _____/ CONTEXT functions \__________

/*
 * CONTEXT_push
 * Create a new context on the front of the list
 * current		Pointer to the current list
 * schema		Schema associated with the context
 */
void CONTEXT_push (CONTEXT **current, SCHEMA *schema);

/*
 * CONTEXT_delete_node
 * Delete a context node from a list
 * list		Pointer to the context list
 * delnode	POinter to the node to delete
 */
void CONTEXT_delete_node (CONTEXT **list, CONTEXT *delnode);

/*
 * CONTEXT_delete [X]
 * Delete an entire context list
 * list		Pointer to the context list
 */
void CONTEXT_delete (CONTEXT **context);

/*
 * CONTEXT_get
 * Return a context matching C-ISAM bridge file descriptor
 * current		Pointer to the current list
 * isfd			C-ISAM bridge file descriptor
 */
CONTEXT * CONTEXT_get (CONTEXT *current, int isfd);

/*
 * CONTEXT_print
 * Print a CONTEXT type to stdout
 * context		Pointer to the list to print
 * label		Label to identify the print list
 */
char * CONTEXT_print (CONTEXT *context, char *label);

#endif // _SCHEMA_H
