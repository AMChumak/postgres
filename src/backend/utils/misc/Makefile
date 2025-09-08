#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/misc
#
# IDENTIFICATION
#    src/backend/utils/misc/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/misc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = \
	conffiles.o \
	guc_composite.o \
	guc.o \
	guc-file.o \
	guc_composite_gram.o\
	guc_composite_scan.o\
	guc_funcs.o \
	guc_tables.o \
	help_config.o \
	injection_point.o \
	pg_config.o \
	pg_controldata.o \
	pg_rusage.o \
	ps_status.o \
	queryenvironment.o \
	rls.o \
	sampling.o \
	stack_depth.o \
	superuser.o \
	timeout.o \
	tzparser.o

# This location might depend on the installation directories. Therefore
# we can't substitute it into pg_config.h.
ifdef krb_srvtab
override CPPFLAGS += -DPG_KRB_SRVTAB='"$(krb_srvtab)"'
endif

include $(top_srcdir)/src/backend/common.mk

# See notes in src/backend/parser/Makefile about the following two rules
guc_composite_gram.h: guc_composite_gram.c
	touch $@

guc_composite_gram.c: BISONFLAGS += -d

# Force these dependencies to be known even without dependency info built:
guc_composite_gram.o guc_composite_scanner.o: guc_composite_gram.h


clean:
	rm -f guc-file.c
	rm -f guc_composite_scan.c
	rm -f guc_composite_gram.c
	rm -f guc_composite_gram.h
