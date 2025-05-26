# contrib/BiHA/Makefile

MODULE_big = BiHA
OBJS = $(WIN32RES) biha.o

EXTENSION = BiHA

DATA = hello_world--1.0.sql hello_world--1.0--1.1.sql hello_world--1.1--1.2.sql

ifdef USE_PGXS

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/BiHA
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

endif