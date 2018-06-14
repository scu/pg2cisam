/*
 * isamtest.c
 * 
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>

#include <isam.h>
#include <decimal.h>

#ifdef TARGET_PGISAM
#include <pgisam.h>
#endif //TARGET_PGISAM

#define MAXBUFSZ 4096

// Typedefs
typedef enum bool { err = (-1), false = 0, true } bool;
typedef unsigned char byte;
typedef unsigned short int word16;
typedef unsigned int word32;
typedef enum OP { Add, Sub, Mul, Div, Cmp,
	CnvInt, CnvLong, CnvDouble, CnvFloat } OP;

// Externs

// Static data
static unsigned long sum = 0L;
static unsigned long sumlen = 0L;
static unsigned long testnum = 0L;
static bool VERBOSE = false;

// Static function prototypes
static void cleanup (void);
static void exit_handler (int extstat);
static void signal_handler (int signo);
static void usage (void);
static bool decimal_test_main (char *filename);
static word16 checksum(byte *addr, word32 count);
static void sumprintf (bool print, char *fmt, ...);
static bool decimal_action (char *cmd, char *arg1, char *arg2);
static bool sum_test_main (char *isamfilename);


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
	
	sumprintf(true, "exit %s [exstat=%d]\n", str_status, exstat);

	cleanup();
	exit(exstat);
	
} /* exit_handler */


/* signal_handler
 */
static void signal_handler(int signo)
{
	sumprintf(true, "signal %d received\n", signo);
	exit_handler(EXIT_FAILURE);
	
} /* signal_handler */


/* usage
 */
static void usage(void)
{
	fprintf(stderr, "isamtest [-v?] operation [args...]\n"
	    "  Operation                  Description\n"
	    "  decimals <decimalfile>     Test decimals\n"
	    "  sum <isamfile>             Read isam file by each index and sum the results\n"
		"    -v                       Verbose\n"
		"    -?                       Print this message\n"
		);

} /* usage */


/* checksum
 */
static word16 checksum(byte *addr, word32 count)
{
	register word32 sum = 0;

	// Main summing loop
	while (count > 1) {
		sum = sum + *(char *) addr++;
		count = count - 2;
	}

	// Add left-over byte, if any
	if (count > 0)
		sum = sum + *((byte *) addr);

	// Fold 32-bit sum to 16 bits
	while (sum>>16)
		sum = (sum & 0xFFFF) + (sum >> 16);

	return (~sum);

} /* checksum */


/* sumprintf
 */
static void sumprintf (bool print, char *fmt, ...)
{
	va_list ap;
	size_t buflen;
	char buf[MAXBUFSZ];

	va_start(ap, fmt);

	vsprintf(buf, fmt, ap);

	// Build the "sum"
	buflen = strlen(buf);
	sum += checksum((byte *)buf, buflen);
	sumlen += buflen * sizeof(char);

	if (print)
		fprintf(stderr, "%s", buf);

	va_end(ap);

} /* sumprintf */


/* decimal_action
 */
static bool decimal_action (char *cmd, char *arg1, char *arg2)
{
	OP Op;
	dec_t dec1, dec2, result, unpacked_result;
	char pack[8], rbuf[32];
	int int_result;
	long long_result;
	double double_result;
	float float_result;
		
	if (!strcmp(cmd, "add")) {
		Op = Add;
	} else
	if (!strcmp(cmd, "sub")) {
		Op = Sub;
	} else
	if (!strcmp(cmd, "mul")) {
		Op = Mul;
	} else
	if (!strcmp(cmd, "div")) {
		Op = Div;
	} else
	if (!strcmp(cmd, "cmp")) {
		Op = Cmp;
	} else
	if (!strcmp(cmd, "cnvint")) {
		Op = CnvInt;
	} else
	if (!strcmp(cmd, "cnvlong")) {
		Op = CnvLong;
	} else
	if (!strcmp(cmd, "cnvdouble")) {
		Op = CnvDouble;
	} else
	if (!strcmp(cmd, "cnvfloat")) {
		Op = CnvFloat;
	} else
	goto retbad;

	// Convert characters to decimals
	deccvasc(arg1, strlen(arg1), &dec1);
	if (arg2)
		deccvasc(arg2, strlen(arg2), &dec2);
	
	// Do the conversions
	switch (Op) {
		case CnvInt:
		sumprintf(true, "Converting INTEGER %s\n\tTo decimal:\n", arg1);
		deccvint(atoi(arg1), &result);
		memset(&rbuf, 0, 32);
		dectoasc(&result, rbuf, 16, -1);
		sumprintf(true, "\tResult:\t[%s]\n", rbuf);
		sumprintf(true, "\tTo INTEGER:\n");
		dectoint(&result, &int_result);
		sumprintf(true, "\tResult:\t[%d]\n", int_result);
		return true;
		
		case CnvLong:
		sumprintf(true, "Converting LONG %s\n\tTo decimal:\n", arg1);
		deccvlong(atol(arg1), &result);
		memset(&rbuf, 0, 32);
		dectoasc(&result, rbuf, 16, -1);
		sumprintf(true, "\tResult:\t[%s]\n", rbuf);
		sumprintf(true, "\tTo LONG:\n");
		dectolong(&result, &long_result);
		sumprintf(true, "\tResult:\t[%ld]\n", long_result);
		return true;
		
		case CnvDouble:
		sumprintf(true, "Converting DOUBLE %s\n\tTo decimal:\n", arg1);
		deccvdbl(atof(arg1), &result);
		memset(&rbuf, 0, 32);
		dectoasc(&result, rbuf, 16, -1);
		sumprintf(true, "\tResult:\t[%s]\n", rbuf);
		sumprintf(true, "\tTo DOUBLE:\n");
		dectodbl(&result, &double_result);
		sumprintf(true, "\tResult:\t[%lf]\n", double_result);
		return true;
		
		case CnvFloat:
		sumprintf(true, "Converting FLOAT %s\n\tTo decimal:\n", arg1);
		deccvflt(atof(arg1), &result);
		memset(&rbuf, 0, 32);
		dectoasc(&result, rbuf, 16, -1);
		sumprintf(true, "\tResult:\t[%s]\n", rbuf);
		sumprintf(true, "\tTo FLOAT:\n");
		dectoflt(&result, &float_result);
		sumprintf(true, "\tResult:\t[%f]\n", float_result);
		return true;		
	}
	
	// Print numbers
	sumprintf(true, "Test #%d: %s %s %s %s: "
		,++testnum
		,Op == Add ? "Adding"
			: Op == Sub ? "Subtracting"
			: Op == Mul ? "Multiplying"
			: Op == Div ? "Dividing"
			: "Comparing"
		,Op == Sub ? arg2 : arg1
		,Op == Add ? "and"
			: Op == Sub ? "from"
			: Op == Cmp ? "to"
			: "by"
		,Op == Sub ? arg1 : arg2
		);
	
	// Perform the operation
	switch (Op) {
		case Add:
		decadd(&dec1, &dec2, &result);
		break;
		
		case Sub:
		decsub(&dec1, &dec2, &result);
		break;
		
		case Mul:
		decmul(&dec1, &dec2, &result);
		break;
		
		case Div:
		decdiv(&dec1, &dec2, &result);
		break;
		
		case Cmp:
		int_result = deccmp(&dec1, &dec2);
		switch (int_result) {
			case -1:
			sumprintf(true, "\ndecimal1 is LESS THAN decimal2\n");
			break;
			
			case 0:
			sumprintf(true, "\ndecimal1 is EQUAL TO decimal2\n");
			break;
			
			default:
			sumprintf(true, "\ndecimal1 is GREATER THAN decimal2\n");
		}
		return true;
	}
	
	// Pack the result
	memset(&pack, 0x20, 8);
	stdecimal(&result, pack, 8);
	
	// Unpack the result
	lddecimal(pack, 8, &unpacked_result);
	//memcpy(&unpacked_result, &result, sizeof(dec_t));
	
	// Convert the unpacked result into a char array
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, -1);
	sumprintf(true, "\n  1.-1 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 0);
	sumprintf(true, "3.0 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 2);
	sumprintf(true, "5.2 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, -1);
	sumprintf(true, "\n  2.-1 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 0);
	sumprintf(true, "4.0 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 2);
	sumprintf(true, "6.2 =[%s]\n", rbuf);
	
	// Skip the packed/unpacked
	memcpy(&unpacked_result, &result, sizeof(dec_t));
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, -1);
	sumprintf(true, "\n  7.-1 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 0);
	sumprintf(true, "9.0 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 2);
	sumprintf(true, "11.2=[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, -1);
	sumprintf(true, "\n  8.-1 =[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 0);
	sumprintf(true, "10.0=[%s]  ", rbuf);
	
	memset(&rbuf, 0, 32);
	dectoasc(&unpacked_result, rbuf, 16, 2);
	sumprintf(true, "12.2=[%s]\n\n", rbuf);
	
	return true;
	
retbad:
	return false;
	
} /* decimal_action */


/* decimal_test_main
 */
static bool decimal_test_main (char *filename)
{
	FILE *fd;
	char BUF[MAXBUFSZ];
		
	fd = fopen(filename, "r");
	if (!fd) {
		perror("fopen");
		return false;
	}
	
	while (fgets(BUF, MAXBUFSZ, fd) != (char *)NULL) {
		char *idx;
		char *cmd, *arg1=NULL, *arg2=NULL;
		
		// Ignore blank lines and comments
		if (BUF[0] == '\n' || BUF[0] == '#') continue;
				
		// Elimitate nl's and cr's
		if (BUF[strlen(BUF)-1] == '\n') BUF[strlen(BUF)-1] = '\0';
		if (BUF[strlen(BUF)-1] == '\r') BUF[strlen(BUF)-1] = '\0';
		
		cmd = BUF;
		if (!strcmp(cmd, "quit")) {
			fclose(fd);
			return true;
		}
		
		idx = index(BUF, ' ');
		if (!idx)
			goto retbad;
		*idx = '\0'; *idx++; arg1=idx;
		
		// arg2 is optional
		idx = index(arg1, ' ');
		if (idx) {
			*idx = '\0';
			*idx++;
			arg2=idx;
		}
		
		if (!decimal_action(cmd, arg1, arg2))
			goto retbad;
	}
	
	fclose(fd);
	return true;
	
retbad:
	fclose(fd);
	sumprintf(true, "ERROR: [%s] is not a valid directive\n", BUF);
	
	return false;
	
} /* decimal_test_main */


/* sum_test_main
 */
static bool sum_test_main (char *isamfilename)
{
	int isfd, numkeys, i, k, io;
	struct dictinfo info;
	struct keydesc key;
	char *record = NULL;
	
	// Don't sum the filename: legit difference
	fprintf(stderr, "ISAM filename ............... %s\n", isamfilename);
	
	isfd = isopen(isamfilename, ISINPUT+ISEXCLLOCK);
	if (isfd < 0) {
		sumprintf(true, "isopen failed, iserrno=%d\n", iserrno);
	}
	
	isindexinfo(isfd, (struct keydesc *)&info, 0);
	
	numkeys = info.di_nkeys & 0x7fff;
	
	sumprintf(true, "Record size (in bytes) ...... %d\n", info.di_recsize);
	sumprintf(true, "Number of keys .............. %d\n", numkeys);
	sumprintf(true, "Number of records ........... %d\n", info.di_nrecords);
	
	record = (char *)malloc(info.di_recsize + 1);
	
	// Read through each key
	for (i=1; i <= numkeys; i++) {
		isindexinfo(isfd, &key, i);
		
		// Print key information
		sumprintf(true, "\nReading by key #%d: Flags=%s, Parts=%d, Length=%d\n"
			,i
			,(key.k_flags & ISDUPS) ? "ISDUPS" : "ISNODUPS"
			,key.k_nparts
			,key.k_len
			);
		
		for (k=0; k < key.k_nparts; k++) {
			sumprintf(true, "  Part #%d, Start=%d, Length=%d, Type=%s%s%s%s\n"
				,k
				,key.k_part[k].kp_start
				,key.k_part[k].kp_leng
				,(key.k_part[k].kp_type == CHARTYPE) ? "CHARTYPE" : ""
				,(key.k_part[k].kp_type == INTTYPE) ? "INTTYPE" : ""
				,(key.k_part[k].kp_type == LONGTYPE) ? "LONGTYPE" : ""
				,(key.k_part[k].kp_type == DOUBLETYPE) ? "DOUBLETYPE" : ""
				);
		}
		
		// Prepare the record
		memset(record, 0x20, info.di_recsize + 1);
		
		io = isstart(isfd, &key, 1, record, ISFIRST);
		if (io < 0) {
			sumprintf(true, "isstart failed, iserrno=%d\n", iserrno);
			goto retbad;
		}
		
		while ((io = isread(isfd, record, ISNEXT)) >= 0) {
			sumprintf(VERBOSE, "%.*s]\n", info.di_recsize, record);		
		}
	}
	
	free(record);
	isclose(isfd);
	return true;
	
retbad:
	if (record) free(record);
	isclose(isfd);
	return false;
	
} /* sum_test_main */ 


int main (int argc, char ** argv)
{
	int c;
	bool exstat;
	char *operation;
	extern int optind;
	extern char *optarg;

	// Set signal handling routines
	signal(SIGTERM, signal_handler);
	signal(SIGHUP, signal_handler);
	signal(SIGINT, signal_handler);
	signal(SIGSEGV, signal_handler);

	// Parse options on command line
	while ((c = getopt(argc, argv, "v?")) != -1) {
		switch (c) {
			case 'v':
			VERBOSE=true;
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

	operation = strdup(argv[optind]);
	
	// Initialize PG-ISAM if we need to
#ifdef TARGET_PGISAM
	pgout_set("logs/isamtest.log");
	decimal_init();
	
	if (VERBOSE) {
		pgout_set_display();
	} else {
		unsetenv("PGISAM");
	}
	
	pgout_zero();
	pgout(mDTSTAMP, "isamtest %s: program started", operation);
	
	// Initialize the program
	if (!init_program()) {
		pgout(mDISPLAY, "pgutil: could not initialize the program");
		exit_handler(false);
	}
#endif // TARGET_PGISAM
	
	if (!strcmp(operation, "decimals")) {
		if (argc != optind+2) {
			usage();
			exit(EXIT_FAILURE);
		}
		exstat = decimal_test_main(argv[argc-1]);
	} else
	if (!strcmp(operation, "sum")) {
		if (argc != optind+2) {
			usage();
			exit(EXIT_FAILURE);
		}
		exstat = sum_test_main(argv[argc-1]);
	} else
	{
		usage();
		exit(EXIT_FAILURE);
	}
	
	fprintf(stderr, "\n\tChecksum = %lX%lX\n\n", sum, sumlen);
	
#ifdef TARGET_PGISAM
	shutdown_program();
#endif //TARGET_PGISAM
	
	exit_handler(exstat ? false : true);

} /* main */
