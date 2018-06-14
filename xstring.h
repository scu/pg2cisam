/*
 * xstring.h: Strings Routines
 */

#ifndef _XSTRING_H
#define _XSTRING_H

#include <stdarg.h>

void str_to_upper (char * BUF);
void str_to_lower (char * BUF);
void str_to_lower_quoted (char * BUF);
void str_to_upper (char * BUF);
bool str_is_blank (char * BUF, size_t size);
bool str_greater_than (char * value, char * compared_to);
bool str_is_whitespace (char * BUF);
bool str_contains_whitespace (char * BUF);
bool str_is_numeric (char * BUF);
bool str_is_float (char * BUF);
char * str_date_time_now (void);
void str_append (char ** str, char * strToAdd, ...);
char * str_add_mem (char * BUF, char * strToAdd, size_t sz);
char * str_sql_encode (char * BUF);
char * str_protect (char * BUF, char * protect, char protectChar);
char * str_dup (char * BUF);
char * str_part (char ** str, char delim);
bool str_is_block_numeric (char * BUF, int size);
void str_justify_key (char * BUF, size_t size);
void str_free (char ** value);
size_t str_padlength (char * BUF, size_t size);
void str_trim_char (char ** str, char c);
char * str_basename (char * filename);
bool str_is_filled (char * BUF, char c);

#ifndef _MIXED
#ifndef PLATFORM_LINUX
int vasprintf (char **resultp, const char *format, va_list * args);
int asprintf (char **resultp, const char *format, ...);
#endif //PLATFORM_LINUX
#endif //_MIXED

#endif // _XSTRING_H
