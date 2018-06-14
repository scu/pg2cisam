/*
 * xstring.c: Strings Routines
 */

#define _GNU_SOURCE

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>

#include "sys.h"

// Static function prototypes
static size_t str_block_len (char * str, size_t len);
static void str_justify_right (char * str, size_t len);

// CODE STARTS HERE
#ifndef _MIXED
#ifndef PLATFORM_LINUX
/* vasprintf and asprintf
 * My implementationo of the GNU_SOURCE equivalents.
 * Should be POSIX-compliant.
 */
int vasprintf (char ** result, const char *format, va_list * args)
{
	const char *p = format;

	/* Add one to make sure that it is never zero, which might cause malloc
	 * to return NULL.
	 */
	int total_width = strlen(format) + 1;
	va_list ap;
	
__STACK(vasprintf)

	memcpy((void *)&ap, (void *)args, sizeof(va_list));

	while (*p != '\0') {
		if (*p++ == '%') {
			while (strchr("-+ #0", *p))
				++p;

			if (*p == '*') {
	      		++p;
				total_width += abs(va_arg (ap, int));
			} else
				total_width += strtoul(p, (char **) &p, 10);

			if (*p == '.') {
				++p;
				if (*p == '*') {
					++p;
					total_width += abs(va_arg (ap, int));
				} else
					total_width += strtoul(p, (char **) &p, 10);
			}

			while (strchr ("hlL", *p))
				++p;

			// Should be big enough for any format specifier except %s and floats.
			total_width += 30;

			switch (*p) {
				case 'd':
				case 'i':
				case 'o':
				case 'u':
				case 'x':
				case 'X':
				case 'c':
					(void)va_arg(ap, int);
					break;
				case 'f':
				case 'e':
				case 'E':
				case 'g':
				case 'G':
					(void)va_arg(ap, double);
					/* Since an ieee double can have an exponent of 307, we'll
					 * make the buffer wide enough to cover the gross case.
					 */
					total_width += 307;
					break;
				case 's':
					total_width += strlen(va_arg (ap, char *));
					break;
				case 'p':
				case 'n':
					(void) va_arg(ap, char *);
					break;
			}
			
			p++;
		}
	}

	*result = (char *)malloc(total_width);

	if (*result != NULL) {
		__return vsprintf(*result, format, *args);
	} else {
		__return 0;
	}

} /* vasprintf */


int asprintf (char **resultp, const char *format, ...)
{
	va_list args;
	int result;
	
__STACK(asprintf)

	va_start(args, format);
	result = vasprintf(resultp, format, &args);
	va_end(args);
	
	__return result;

} /* asprintf */
#endif // PLATFORM_LINUX
#endif // _MIXED

bool str_is_blank (char * BUF, size_t size)
{
	unsigned int x;
	
__STACK(str_is_blank)
		
	for (x=0; x < size; x++) {
		if (BUF[x] != ' ') {
			__return false;
		}	
	}
	
	__return true;
	
} /* str_is_blank */


size_t str_padlength (char * BUF, size_t size)
{
	unsigned int x;
	
__STACK(str_padlength)
	
	for (x=size; x > 0; x--) {
		if (BUF[x-1] != ' ') {
			__return (size - x);
		}	
	}
	
	__return (size_t)0;
	
} /* str_padlength */


void str_trim_char (char ** str, char c)
{
	size_t target;
	char *ptr;
	
__STACK(str_trim_char)
	
	ptr = *str;
	
	target = strlen(ptr) - 1;
	
	if (target < 1) {
		__return;
	}
	
	if (ptr[target] == c) {
		ptr[target] = '\0';
	}
	
	*str = ptr;
	
	__return;
	
} /* str_trim_char */


bool str_greater_than (char * value, char * compared_to)
{
__STACK(str_greater_than)

	do {
		if (*value == *compared_to) {
			*compared_to++;
			continue;
		}

		if (*value > *compared_to) {
			__return true;
		} else {
			__return false;
		}

	} while (*value++);

	__return false;

} /* str_greater_than */


void str_to_lower_quoted (char * BUF)
{
	size_t x;
	bool INQUOTES=false, INVARNAME=false;
	
__STACK(str_to_lower_quoted)

	for (x=0; x <= strlen (BUF); x++) {
		if (BUF[x] == 0x22) {
			if (INQUOTES)
					INQUOTES=false;
			else    INQUOTES=true;
			continue;
		}

		if ((BUF[x] == '$') && (!INQUOTES))
			INVARNAME = true;

		if (((BUF[x] == ' ') || (BUF[x] == '=')) && (INVARNAME))
			INVARNAME = false;

			if (!INQUOTES && !INVARNAME)
				BUF[x] = tolower(BUF[x]);
        }
        
	__return;

} /* str_to_lower_quoted */


void str_to_upper (char * BUF)
{
	size_t x;
	
__STACK(str_to_upper)

	for (x=0; x <= strlen(BUF); x++)
		BUF[x] = toupper(BUF[x]);
		
	__return;

} /* str_to_upper */


void str_to_lower (char * BUF)
{
	size_t x;
	
__STACK(str_to_lower)

	for (x=0; x <= strlen(BUF); x++)
		BUF[x] = tolower(BUF[x]);
		
	__return;

} /* str_to_lower */
	
	
bool str_is_whitespace (char * BUF)
{
	char *tmp;
	
__STACK(str_is_whitespace)
	
	tmp = BUF;
	
	do {
		if ((*BUF != '\t') && (*BUF != ' ') && (*BUF != '\n') 
			&& (*BUF != '\r') && (*BUF != 0x00)) {
			BUF = tmp;
			__return false;
		}
	} while (*BUF++);	
	
	BUF = tmp;
	
	__return true;
	
} /* str_is_whitespace */
	

bool str_contains_whitespace (char * BUF)
{
	char *tmp;
	
__STACK(str_contains_whitespace)
	
	tmp = BUF;
	
	do {
		if (*BUF == 0x00) break;
		if ((*BUF == '\t') || (*BUF == ' ') || (*BUF == '\n')
			|| (*BUF == '\r')) {
			BUF = tmp;
			__return true;
		}
	} while (*BUF++);	
	
	BUF = tmp;
	
	__return false;

} /* str_contains_whitespace */
	

bool str_is_numeric (char * BUF)
{
	char *tmp;
	
__STACK(str_is_numeric)
	
	tmp = BUF;
	
	do {
		if ((! isdigit(*BUF)) && (*BUF != 0x00)) {
			BUF = tmp;
			__return false;
		}
		
	} while (*BUF++);
	
	BUF = tmp;
	
	__return true;
	
} /* str_is_numeric */


static size_t str_block_len (char * str, size_t len)
{
	size_t i;
	
__STACK(str_block_len)

	i = len;
	
	do {
		i--;
		if ((str[i] != ' ') && (str[i] != '\0')) {
			i++;
			break;
		}
	} while (i > 0);

	__return i;

} /* str_block_len */


static void str_justify_right (char * str, size_t len)
{
	size_t sz;
	
__STACK(str_justify_right)

	sz = str_block_len(str, len);
	
	if ((sz > 0) && (sz < len)) {
		do {
			for (sz = len - 2; sz != 0; sz--) {
				str[sz + 1] = str[sz];
			}
			str[0] = ' ';
		} while ((str[len - 1] == '\0') ||
				(str[len - 1] == ' '));
	}
	
	__return;

} /* str_justify_right */


/* str_is_block_numeric
 * Reads a block of data to determine whether the contents are numeric
 */
bool str_is_block_numeric (char * BUF, int size)
{
	int i;
	bool IS_NUMERIC = true;
	
__STACK(str_is_block_numeric)

	// Determine whether the block is numeric
	for (i = 0; i < size; i++) {
		
		if ((BUF[i] == ' ') || (BUF[i] == 0x00)) continue;
		
		if (! isdigit(BUF[i])) {
			IS_NUMERIC = false;
			break;
		}
	}
	
	__return IS_NUMERIC;

} /* str_is_block_numeric */


void str_justify_key (char * BUF, size_t size)
{
	size_t i;
	bool IS_NUMERIC = true;
	
__STACK(str_justify_key)

	for (i = 0; i < size; i++) {
		
		if ((BUF[i] == ' ') || (BUF[i] == 0x00)) continue;
		
		if (! isdigit(BUF[i])) {
			IS_NUMERIC = false;
			break;
		}
	}
	
	if (IS_NUMERIC)
		str_justify_right(BUF, size);
		
	__return;

} /* str_justify_key */


bool str_is_float (char * BUF)
{
	char *tmp;
	
__STACK(str_is_float)
	
	tmp = BUF;
	
	do {
		if (*BUF == '.') continue;
		
		if ((! isdigit(*BUF)) && (*BUF != 0x00)) {
			BUF = tmp;
			__return false;
		}
		
	} while (*BUF++);
	
	BUF = tmp;
	
	__return true;
	
} /* str_is_float */
	

char * str_dup (char * BUF)
{
__STACK(str_dup)

	if (! BUF) {
		__return (char *)NULL;
	}
	
	__return (char *)strdup(BUF);
	
} /* str_dup */
	

void str_append (char ** str, char * strToAdd, ...)
{
	int len_old, len_new, newsz;
	va_list ap;
	char *vastring = NULL;
	char *buf = *str;
	
__STACK(str_append)
	
	// No BUF yet, so assume zero length
	if (buf == (char *)NULL)
			len_old = 0;
	else	len_old = strlen(buf);
	
	va_start(ap, strToAdd);
	
	// CONSUMER: vastring
	vasprintf(&vastring, strToAdd, ap);
	
	len_new = strlen(vastring);
	
	newsz = len_old + len_new + 1;
	
	// Reallocate BUF to the length of strToAdd + 1 (for null term)
	buf = (char *)realloc(buf, newsz);
	
	memcpy(&buf[len_old], vastring, len_new);
	buf[newsz-1] = '\0';

	va_end(ap);
	
	// DESTROYER: vastring
	xfree(vastring);
				
	*str = buf;
	
	__return;
	
} /* str_append */


char * str_add_mem (char * BUF, char * strToAdd, size_t sz)
{
	int len_old, newsz;
	
__STACK(str_add_mem)
		
	// No BUF yet, so assume zero length
	if (BUF == (char *)NULL)
			len_old = 0;
	else	len_old = strlen(BUF);
	
	newsz = len_old + sz + 1;
	
	// Reallocate BUF to the length of strToAdd + 1 (for null term)
	BUF = (char *)realloc(BUF, newsz);
	
	memcpy(&BUF[len_old], strToAdd, sz);
	BUF[newsz-1] = '\0';

	__return BUF;
	
} /* str_add_mem */
	
	
char * str_date_time_now (void)
{
	time_t now;
	char BUF[80];
	char *ret;
	
__STACK(str_date_time_now)
	
	now = time(NULL);
	
	strftime(BUF, 80, "%Y%m%d%H%M%S", localtime(&now));
	
	ret = (char *)strdup(BUF);
	
	__return ret;
	
} /* str_date_time_now */


char * str_sql_encode (char * BUF)
{
	char *b	= NULL;	// The new buffer
	
__STACK(str_sql_encode)
	
	// Can't process zer0 size
	if (! BUF || (! strlen(BUF))) {
		__return (char *)NULL;
	}
		
	do {
		// Don't process the NUL terminator
		if (! *BUF) break;
		
		// Convert single quotes to ''
		if (*BUF == 0x27) {
			str_append(&b, "''");
			continue;
		}
		
		str_append(&b, "%c", *BUF);
		
	} while (*(BUF)++);
	
	__return b;
	
} /* str_sql_encode */


char * str_protect (char * BUF, char * protect, char protectChar)
{
	char *tmp;

__STACK(str_protect)
	
	// If it doesn't have the protect string, return unchanged
	if (! strstr(BUF, protect)) {
		__return BUF;
	}
	
	asprintf(&tmp, "%c%s%c", protectChar, BUF, protectChar);
	
	free(BUF);
	
	BUF = (char *)strdup(tmp);
	
	free(tmp);
	
	__return BUF;
	
} /* str_protect */


char * str_part (char ** str, char delim)
{
	char *start, *end;

__STACK(str_part)
	
	start = end = *str;
		
	end = (char *)index(start, (int)delim);

	if (! end) {
		__return start;
	}
		
	*end = '\0'; *end++;
	
	*str = end;
	
	__return start;	
	
} /* str_part */


/* Using str_free now, w/a cast
void str_ufree (unsigned char ** value)
{
	unsigned char *str = *value;
	
__STACK(str_ufree)
__ARGS("str=%s", str)
	
	if (! str) {
		__return;
	}
	
	free(str);
	
	*value = (unsigned char *)NULL;
	
	__return;

}  str_ufree */


void str_free (char ** value)
{
	char *str = *value;
	
__STACK(str_free)
	
	if (! str) {
		__return;
	}
	
	free((void *)str);
	
	*value = (char *)NULL;
	
	__return;

} /* str_free */


char * str_basename (char * filename)
{
	char * idx;
	
__STACK(str_basename)
	
	idx = strrchr(filename, '/');
	
	if (! idx) {
		__return (char *)strdup(filename);
	}
	
	__return (char *)strdup(++idx);
	
} /* str_basename */


bool str_is_filled (char * BUF, char c)
{
__STACK(str_is_filled)

	if (! BUF) {
		__return false;
	}
	if (! strlen(BUF)) {
		__return false;
	}
	
	do {
		// Don't process the NUL terminator
		if (! *BUF) break;
		
		if (*BUF != c) {
			__return false;
		}
		
	} while (*(BUF)++);
	
	__return true;
	
} /* str_is_filled */
