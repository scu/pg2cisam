/*
 * issum.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stdarg.h>
#include <string.h>

#include <isam.h>
#include <decimal.h>

#define MAXBUFSZ 4096

// Typedefs
typedef enum bool { err = (-1), false = 0, true } bool;
typedef unsigned char byte;
typedef unsigned short int word16;
typedef unsigned int word32;

// Externs

// Static data
static unsigned long sum = 0L;
static unsigned long sumlen = 0L;
static bool VERBOSE = false;

// Static function prototypes
static void cleanup (void);
static void exit_handler (int extstat);
static void signal_handler (int signo);
static void usage (void);
static word16 checksum(byte *addr, word32 count);
static bool issum_main (char *isamfilename);
static void sumprintf (bool print, char *fmt, ...);


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
	
	sumprintf(true, "exit %s [exstat=%d]", str_status, exstat);

	cleanup();
	exit(exstat);
	
} /* exit_handler */


/* signal_handler
 */
static void signal_handler(int signo)
{
	sumprintf(true, "signal %d received", signo);
	exit_handler(EXIT_FAILURE);
	
} /* signal_handler */


/* usage
 */
static void usage(void)
{
	fprintf(stderr, "issum [-v?] isamfile\n"
		"  -v        Verbose (print each line to stderr)"
		"  -?        Print this message\n"
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

	if (print) {
		fprintf(stderr, "%s", buf);
		fprintf(stderr, "\n");
	}

	va_end(ap);

} /* sumprintf */


/* issum_main
 */
static bool issum_main (char *isamfilename)
{
	int isfd, numkeys, i, k, io;
	struct dictinfo info;
	struct keydesc key;
	char *record = NULL;
	
	// Don't sum the filename: legit difference
	fprintf(stderr, "ISAM filename ............... %s\n", isamfilename);
	
	isfd = isopen(isamfilename, ISINPUT+ISEXCLLOCK);
	if (isfd < 0) {
		sumprintf(true, "isopen failed, iserrno=%d", iserrno);
	}
	
	isindexinfo(isfd, (struct keydesc *)&info, 0);
	
	numkeys = info.di_nkeys & 0x7fff;
	
	sumprintf(true, "Record size (in bytes) ...... %d", info.di_recsize);
	sumprintf(true, "Number of keys .............. %d", numkeys);
	sumprintf(true, "Number of records ........... %d", info.di_nrecords);
	
	record = (char *)malloc(info.di_recsize + 1);
	
	// Read through each key
	for (i=1; i <= numkeys; i++) {
		isindexinfo(isfd, &key, i);
		
		// Print key information
		sumprintf(true, "\nReading by key #%d: Flags=%s, Parts=%d, Length=%d"
			,i
			,(key.k_flags & ISDUPS) ? "ISDUPS" : "ISNODUPS"
			,key.k_nparts
			,key.k_len
			);
		
		for (k=0; k < key.k_nparts; k++) {
			sumprintf(true, "  Part #%d, Start=%d, Length=%d, Type=%s%s%s%s"
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
			sumprintf(true, "isstart failed, iserrno=%d", iserrno);
			goto retbad;
		}
		
		while ((io = isread(isfd, record, ISNEXT)) >= 0) {
			sumprintf(VERBOSE, "%.*s]", info.di_recsize, record);			
		}
	}
	
	free(record);
	isclose(isfd);
	return true;
	
retbad:
	if (record) free(record);
	isclose(isfd);
	return false;
	
} /* issum_main */ 


int main (int argc, char ** argv)
{
	int c;
	bool exstat;
	char *isamfilename;
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
			VERBOSE = true;
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
	
	isamfilename = strdup(argv[optind]);
	
	exstat = issum_main(isamfilename);
	
	fprintf(stderr, "\n\tChecksum = %lX%lX\n\n", sum, sumlen);
	
	exit_handler(exstat ? false : true);

} /* main */
