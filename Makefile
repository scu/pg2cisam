# Begin Makefile

SHELL=/bin/sh
LOG=log

# Not targeting any other platform as of Aug, 2008
TARGET=LINUX

# Notices
CC_NOTICE=printf "%-24s |__ compiling --> $@\n" $*.c | tee -a $(LOG)
AR_NOTICE=printf "%-24s ( archiving )\n" $@ | tee -a $(LOG)
LD_NOTICE=printf "%-24s ( linking )\n" $@ | tee -a $(LOG)

# Build flags
CC=/usr/bin/cc
AR=/usr/bin/ar
ARFLAGS=r
PLATFORM_FLAGS=-DPLATFORM_LINUX
INCLUDE_PATH=-I/usr/include/postgresql -I. -Iisam_includes -I$(HW_INCLUDE_PATH)
LIB_FLAGS=-L. -L$(HW_LIB_PATH)
DEBUG_FLAGS=-g -DDEBUG
EXTRA_CFLAGS=$(PLATFORM_FLAGS) $(INCLUDE_PATH) $(LIB_FLAGS) $(DEBUG_FLAGS)
PGLIBS=-lm -lpq -ldecimal -lbridge
CFLAGS=-D_$(TARGET) $(EXTRA_CFLAGS) -D_MIXED
CPFLAGS=-f
ISLIBS=-lvbisam -lbridge

exeobjs=pgutil isamtest-vb isamtest-pg $(pgisamobjs) $(cisamobjs)

.c.o: 
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) -o $*.o -c $*.c 2>&1 | tee -a $(LOG)

libobjs=libpgisam

all: rmlog $(libobjs) $(exeobjs)
full: clean all

rmlog:
	@> $(LOG)
	@-rm -f libpgisam.a

clean:
	-rm -f *.o *.la *.a *.lo *.so gmon.out $(exeobjs)
	@> $(LOG)
	
install:
	cp $(CPFLAGS) libpgisam.a ${HW_LIB_PATH}
	cp $(CPFLAGS) pgisam.h ${HW_INCLUDE_PATH}
	cp $(CPFLAGS) isam_includes/* ${HW_INCLUDE_PATH}
	cp $(CPFLAGS) pgutil ${HW_BIN_PATH}
	
# libbridge
# Helper functions
pgbridge-cisam.o: pgbridge.c
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM -opgbridge-cisam.o -c pgbridge.c

pgbridge.o: pgbridge.c
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM -opgbridge.o -c pgbridge.c
	
wrappers-pgisam.o: wrappers.c
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM -owrappers-pgisam.o -c wrappers.c
	
wrappers-cisam.o: wrappers.c
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM -owrappers-cisam.o -c wrappers.c	
	
pgdecimal.o: pgdecimal.c
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) -D_DECLIB -DTARGET_PGISAM -opgdecimal.o -c pgdecimal.c
	
schema.o: schema.c
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM -oschema.o -c schema.c
	
libpgisamobjs=sys.o xstring.o pgres.o pgbridge.o pgdecimal.o schema.o
libpgisam: libbridge $(libpgisamobjs)
	@$(AR_NOTICE)
	@$(AR) $(ARFLAGS) libpgisam.a $(libpgisamobjs) \
		2>&1 | tee -a $(LOG)

isamtest-pg: isamtest.c
	@$(CC_NOTICE)
	@$(CC) -I. -Iisam_includes -I$(HW_INCLUDE_PATH) -L. -L$(HW_LIB_PATH) \
		isamtest.c -D_DECLIB -DTARGET_PGISAM -oisamtest-pg -lpgisam $(PGLIBS)
	
isamtest-vb: isamtest.c
	@$(CC_NOTICE)
	@$(CC) $(CFLAGS) isamtest.c -DTARGET_CISAM -oisamtest-vb $(ISLIBS) 

pgutilobj=pgres.o pgutil.o sys.o pgbridge-cisam.o pgdecimal.o schema.o xstring.o
pgutil: libbridge $(pgutilobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM $(LDFLAGS) -o pgutil \
		$(pgutilobj) $(ISLIBS) $(PGLIBS) \
		2>&1 | tee -a $(LOG)

regress1-cisamobj=regress1-cisam.o sys.o wrappers-cisam.o isbridge.o xstring.o
regress1-cisam: libbridge $(regress1-cisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM $(LDFLAGS) -o regress1-cisam \
		$(regress1-cisamobj) $(ISLIBS) \
		2>&1 | tee -a $(LOG)
	
regress1-pgisamobj=regress1-pgisam.o wrappers-pgisam.o
regress1-pgisam: libbridge $(regress1-pgisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM $(LDFLAGS) -o regress1-pgisam \
		$(regress1-pgisamobj) -lpgisam $(PGLIBS) \
		2>&1 | tee -a $(LOG)

regress2-cisamobj=regress2-cisam.o sys.o wrappers-cisam.o isbridge.o xstring.o
regress2-cisam: libbridge $(regress2-cisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM $(LDFLAGS) -o regress2-cisam \
		$(regress2-cisamobj) $(ISLIBS) \
		2>&1 | tee -a $(LOG)
	
regress2-pgisamobj=regress2-pgisam.o wrappers-pgisam.o
regress2-pgisam: libbridge $(regress2-pgisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM $(LDFLAGS) -o regress2-pgisam \
		$(regress2-pgisamobj) -lpgisam $(PGLIBS) \
		2>&1 | tee -a $(LOG)

regress3-cisamobj=regress3-cisam.o sys.o wrappers-cisam.o isbridge.o xstring.o
regress3-cisam:	libbridge $(regress3-cisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM $(LDFLAGS) -o regress3-cisam \
		$(regress3-cisamobj) $(ISLIBS) \
		2>&1 | tee -a $(LOG)
	
regress3-pgisamobj=regress3-pgisam.o wrappers-pgisam.o
regress3-pgisam: libbridge $(regress3-pgisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM $(LDFLAGS) -o regress3-pgisam \
		$(regress3-pgisamobj) -lpgisam $(PGLIBS) \
		2>&1 | tee -a $(LOG)
		
regress4-cisamobj=regress4-cisam.o sys.o wrappers-cisam.o isbridge.o xstring.o
regress4-cisam:	libbridge $(regress4-cisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM $(LDFLAGS) -o regress4-cisam \
		$(regress4-cisamobj) $(ISLIBS) \
		2>&1 | tee -a $(LOG)
	
regress4-pgisamobj=regress4-pgisam.o wrappers-pgisam.o
regress4-pgisam: libbridge $(regress4-pgisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM $(LDFLAGS) -o regress4-pgisam \
		$(regress4-pgisamobj) -lpgisam $(PGLIBS) \
		2>&1 | tee -a $(LOG)

regress5-cisamobj=regress5-cisam.o sys.o wrappers-cisam.o isbridge.o xstring.o
regress5-cisam:	libbridge $(regress5-cisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM $(LDFLAGS) -o regress5-cisam \
		$(regress5-cisamobj) $(ISLIBS) \
		2>&1 | tee -a $(LOG)
	
regress5-pgisamobj=regress5-pgisam.o wrappers-pgisam.o
regress5-pgisam: libbridge $(regress5-pgisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM $(LDFLAGS) -o regress5-pgisam \
		$(regress5-pgisamobj) -lpgisam $(PGLIBS) \
		2>&1 | tee -a $(LOG)
		
regress6-cisamobj=regress6-cisam.o sys.o wrappers-cisam.o isbridge.o xstring.o
regress6-cisam:	libbridge $(regress6-cisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_CISAM $(LDFLAGS) -o regress6-cisam \
		$(regress6-cisamobj) $(ISLIBS) \
		2>&1 | tee -a $(LOG)
	
regress6-pgisamobj=regress6-pgisam.o wrappers-pgisam.o
regress6-pgisam: libbridge $(regress6-pgisamobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM $(LDFLAGS) -o regress6-pgisam \
		$(regress6-pgisamobj) -lpgisam $(PGLIBS) \
		2>&1 | tee -a $(LOG)

labobj=lab.o wrappers-pgisam.o
lab: libbridge $(labobj)
	@$(LD_NOTICE)
	@$(CC) $(CFLAGS) -DTARGET_PGISAM $(LDFLAGS) -o lab \
		$(labobj) -lpgisam $(PGLIBS) \
		2>&1 | tee -a $(LOG)

# End ~/project/pgisam/Makefile
