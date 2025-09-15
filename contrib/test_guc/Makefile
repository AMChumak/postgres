# contrib/test_guc/Makefile
MODULE_big = test_guc
OBJS = \
	$(WIN32RES) \
	test_guc.o
EXTENSION = test_guc
DATA = test_guc--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_guc
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif