/* sys.h: System routines
 */

#ifndef _SYS_H
#define _SYS_H

#include "pgisam.h"

// Type definitions
enum bool { err = (-1), false = 0, true };
typedef enum bool bool;

// Memory allocation
void * xalloc(size_t size);

// Free memory
void xfree(void * value);

/*
 * fprintb
 * Print bytes in hex
 */
void fprintb (FILE *fd, char *label, void *buf, size_t sz);

#endif // _SYS_H
