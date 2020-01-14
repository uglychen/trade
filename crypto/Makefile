CC=gcc
CXX=g++
RM=rm -f
CPPFLAGS=-g -Wall -std=gnu++11 -I../base
LDFLAGS=-L../lib
LDLIBS=-lssl -lcrypto

SRCS=crypto.cpp ../base/logging.cpp ../base/utils.cpp
OBJS=$(subst .cpp,.o,$(SRCS))

all: target

target: $(OBJS)
	$(CXX) $(LDFLAGS) -o crypto $(OBJS) $(LDLIBS)

depend: .depend

.depend: $(SRCS)
	$(RM) ./.depend
	$(CXX) $(CPPFLAGS) -MM $^>>./.depend;

clean:
	$(RM) $(OBJS)

distclean: clean
	$(RM) *~ .depend

include .depend
