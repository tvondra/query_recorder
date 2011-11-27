MODULE_big = query_recorder
OBJS = src/record.o

EXTENSION = query_recorder
DATA = sql/query_recorder--1.0.0.sql
MODULES = query_recorder

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: record.so

record.so : src/record.o

%.o : src/%.c
