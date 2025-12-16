# scylla_fdw/Makefile
#
# Makefile for scylla_fdw - PostgreSQL Foreign Data Wrapper for ScyllaDB
#

MODULE_big = scylla_fdw
OBJS = \
	scylla_fdw.o \
	scylla_fdw_modify.o \
	scylla_fdw_helper.o \
	scylla_deparse.o \
	scylla_typemap.o \
	scylla_connection.o

EXTENSION = scylla_fdw
DATA = scylla_fdw--1.0.sql
PGFILEDESC = "scylla_fdw - foreign data wrapper for ScyllaDB"

REGRESS = scylla_fdw

# ScyllaDB cpp-rs-driver configuration
# Adjust these paths based on your installation
SCYLLA_DRIVER_INCLUDE ?= /usr/local/include
SCYLLA_DRIVER_LIB ?= /usr/local/lib/aarch64-linux-gnu

PG_CPPFLAGS = -I$(SCYLLA_DRIVER_INCLUDE)
SHLIB_LINK = -L$(SCYLLA_DRIVER_LIB) -Wl,-rpath,$(SCYLLA_DRIVER_LIB) -lscylla-cpp-driver -lstdc++

# For C++ compilation
CXX = g++
CXXFLAGS = -std=c++17 -fPIC -I$(SCYLLA_DRIVER_INCLUDE)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/scylla_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Override the default rule for .cpp files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -I$(shell $(PG_CONFIG) --includedir-server) -c -o $@ $<

# Ensure C++ object files are linked properly
scylla_connection.o: scylla_connection.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -I$(shell $(PG_CONFIG) --includedir-server) -c -o $@ $<

# Installation targets
install: all installdirs
	$(INSTALL_SHLIB) $(MODULE_big)$(DLSUFFIX) '$(DESTDIR)$(pkglibdir)/'
	$(INSTALL_DATA) $(DATA) '$(DESTDIR)$(datadir)/extension/'
	$(INSTALL_DATA) scylla_fdw.control '$(DESTDIR)$(datadir)/extension/'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

# Clean up
clean:
	rm -f $(OBJS) $(MODULE_big)$(DLSUFFIX)
	rm -f *.gcda *.gcno

# Development helpers
.PHONY: format
format:
	clang-format -i *.c *.h *.cpp

.PHONY: check-syntax
check-syntax:
	$(CC) $(CFLAGS) $(CPPFLAGS) -I$(shell $(PG_CONFIG) --includedir-server) -fsyntax-only *.c
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -I$(shell $(PG_CONFIG) --includedir-server) -fsyntax-only *.cpp
