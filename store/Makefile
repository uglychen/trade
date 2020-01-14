# CC -- the c compiler to use
# CXX -- the c++ compiler to use
# LD -- the linker to use
# CFLAGS -- compilation flag for c source files
# CXXFLAGS -- compilation flags for c++ source files
# CPPFLAGS -- flags for the c-preprocessor (typically include file paths and symbols defined on the command line), used by c and c++
# LDFLAGS -- linker flags
# LDLIBS -- libraries to link

CC=gcc
CXX=g++
RM=rm -f
CPPFLAGS=-g -Wall -std=gnu++11 -D_GLIBCXX_USE_CXX11_ABI=0 -I../base -I../include -I../include/hiredis -I../include/libmongoc -I../include/libbson
LDFLAGS=-L../lib
LDLIBS=-ljsoncpp -lhiredis -lmariadbclient -lmongoc -lbson -lpthread -ldl -lssl -lcrypto -lresolv -lsasl2 -lrt

SRCS=store_main.cpp store_loop.cpp store.cpp \
	 ../base/logging.cpp ../base/config.cpp ../base/utils.cpp ../base/mysql_utils.cpp ../base/redis_utils.cpp
OBJS=$(subst .cpp,.o,$(SRCS))

all: target

target: $(OBJS)
	$(CXX) $(LDFLAGS) -o trade_store $(OBJS) $(LDLIBS)

depend: .depend

.depend: $(SRCS)
	$(RM) ./.depend
	$(CXX) $(CPPFLAGS) -MM $^>>./.depend;

clean:
	$(RM) $(OBJS)

distclean: clean
	$(RM) *~ .depend

include .depend
