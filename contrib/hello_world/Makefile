# contrib/hello_world/Makefile

MODULE_big = hello_world
OBJS = $(WIN32RES) hello.o

EXTENSION = hello_world

DATA = hello_world--1.0.sql hello_world--1.0--1.1.sql hello_world--1.1--1.2.sql

ifdef USE_PGXS

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_hello #wtf
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

endif