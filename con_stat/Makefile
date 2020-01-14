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
CPPFLAGS=-g -Wall -std=gnu++11 -I../base -I../include -I../include/librabbitmq -I../include/hiredis
LDFLAGS=-L../lib
LDLIBS=-lrabbitmq -ljsoncpp -lhiredis -lssl -lcrypto

SRCS=trade_main.cpp trade.cpp statistics.cpp \
	 ../base/logging.cpp ../base/config.cpp ../base/utils.cpp ../base/lock.cpp ../base/amqp_utils.cpp ../base/redis_utils.cpp
OBJS=$(subst .cpp,.o,$(SRCS))

all: target

target: $(OBJS)
	$(CXX) $(LDFLAGS) -o con_stat $(OBJS) $(LDLIBS)

depend: .depend

.depend: $(SRCS)
	$(RM) ./.depend
	$(CXX) $(CPPFLAGS) -MM $^>>./.depend;

clean:
	$(RM) $(OBJS)

distclean: clean
	$(RM) *~ .depend

include .depend
