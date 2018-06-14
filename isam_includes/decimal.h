/* decimal.h
 * PG-ISAM reverse engineered decimal type
 */

#ifndef _DECIMAL_H
#define _DECIMAL_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef TARGET_CISAM
#include <decimal64.h>
#define deccopy dl_deccopy
#define deccvint dl_deccvint
#define dectoint dl_dectoint
#define deccvlong dl_deccvlong
#define dectolong dl_dectolong
#define deccvdbl dl_deccvdbl
#define dectodbl dl_dectodbl
#define deccvflt dl_deccvflt
#define dectoflt dl_dectoflt
#define stdecimal dl_stdecimal
#define lddecimal dl_lddecimal
#define decsub dl_decsub
#define decadd dl_decadd
#define decmul dl_decmul
#define decdiv dl_decdiv
#define deccmp dl_deccmp
#define dectoasc dl_dectoasc
#define deccvasc dl_deccvasc

typedef decimal64 dec_t;
#else

#define DECSIZE 16

struct decimal {
	short dec_exp;			// exponent base 100
	short dec_pos;			// sign: 1=pos, 0=neg, -1=null
	short dec_ndgts;		// number of significant digits
	char dec_dgts[DECSIZE];	// actual digits base 100
};

typedef struct decimal dec_t;

#define	ACCSIZE	(DECSIZE + 1)

/* Packed format
 */
typedef struct decacc {
	short	dec_exp;
	short	dec_pos;
	short	dec_ndgts;
	char	dec_dgts[ACCSIZE];
} dec_a;


/* A decimal null will be represented internally by setting dec_pos
 * equal to DECPOSNULL
 */
#define DECPOSNULL	(-1)


/* PSEUDO FUNCTIONS
 * declen, sig = # of significant digits, rd # digits to right of decimal,
 * returns # bytes required to hold such
 */
#define DECLEN(sig,rd)		(((sig) + ((rd)&1) + 3) / 2)
#define DECLENGTH(len)		DECLEN(PRECTOT(len), PRECDEC(len))
#define DECPREC(size)		((size - 1) << 9) + 2)
#define PRECTOT(len)		(((len) >> 8) & 0xff)
#define PRECDEC(len)		((len) & 0xff)
#define PRECMAKE(len,dlen)	(((len) << 8) + (dlen))

/* 
 * Value of an integer that generates a decimal flagged DECPOSNULL
 *     an int of 2 bytes produces 0x8000
 *     an int of 4 bytes produces 0x80000000
 */
#define VAL_DECPOSNULL(type)	(1L << ((sizeof (type) * 8) - 1))
#endif //TARGET_CISAM

/*
 * DECIMALTYPE function prototypes
 */
void decimal_init(void);
void dectostr (unsigned char **buf, char *dec_str, int sz);
void deccopy (dec_t *, dec_t *);
int deccvint (int, dec_t *);
int dectoint (dec_t *, int *);
int deccvlong (long, dec_t *);
int dectolong (dec_t *, long *);
int deccvfix (long, dec_t *);
void dectofix (dec_t *, long *);
int deccvdbl (double, dec_t *);
int dectodbl (dec_t *, double *);
int deccvflt (float, dec_t *);
int dectoflt (dec_t *, float *);
int deccvreal (double, dec_t *, int);
int dectoreal (dec_t *, double *, int);
int stdecimal (dec_t *, unsigned char *, int);
int lddecimal (unsigned char *, int, dec_t *);
void comp100 (char *, int);
int round100 (unsigned char *, int);
int decsub (dec_t *, dec_t *, dec_t *);
int decadd (dec_t *, dec_t *, dec_t *);
int decmul (dec_t *, dec_t *, dec_t *);
int decdiv (dec_t *, dec_t *, dec_t *);
int deccmp (dec_t *, dec_t *);
int dectoasc (dec_t *, char *, int, int);
int deccvasc (char *, int, dec_t *);
char *dececvt (dec_t *, int, int *, int *);
char *decfcvt (dec_t *, int, int *, int *);
char *decefcvt (dec_t *, int, int *, int *, int);

#ifdef __cplusplus
}
#endif

#endif /* _DECIMAL_H */
