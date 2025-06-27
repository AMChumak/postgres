EXTENSION = pg_stdopt

PG_CONFIG = pg_config

MODULE_big = pg_stdopt

OBJS = pg_stdopt.o

DATA = pg_stdopt--1.0.1.sql
PG_CONFIG = pg_config

MODULES = pg_stdopt

ifdef USE_PGXS

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_stdopt
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

endif