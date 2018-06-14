/*
 * iserrcodes.h: C-ISAM error codes and descriptions
 */

#ifndef _ISERRCODES_H
#define _ISERRCODES_H

static struct iserrlist_t {
	int errcode;
	char *description;
} iserrlist[] = {
	100, "duplicate record",
	101, "file not open",
	102, "illegal argument",
	103, "illegal key desc",
	104, "too many files open",
	105, "bad isam file format",
	106, "non-exclusive access",
	107, "record locked",
	108, "key already exists",
	109, "is primary key",
	110, "end/begin of file",
	111, "no record found",
	112, "no current record",
	113, "file locked",
	114, "file name too long",
	115, "can't create lock file",
	116, "can't alloc memory",
	117, "bad custom collating",
	118, "cannot read log rec",
	119, "bad log record",
	120, "cannot open log file",
	121, "cannot write log rec",
	122, "no transaction",
	123, "no shared memory",
	124, "no begin work yet",
	125, "can't use nfs",
	127, "no primary key",
	128, "no logging",
	131, "no free disk space",
	132, "row size too big",
	133, "audit trail exists",
	134, "no more locks",
	153, "must be in ISMANULOCK mode",
	154, "lock timeout expired",
	155, "primary and mirror chunk bad",
	156, "can't attach to shared memory",
	157, "interrupted isam call",
	158, "operation disallowed on SMI pseudo table",
	159, "invalid collation specifier",
	171, "locking or NODESIZE change",
	900, "no schema definition",
	0, 0
};

#endif // _ISERRCODES_H
