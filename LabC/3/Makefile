CC?=cc
CFLAGS=-O3
FUTHARK_BACKEND?=multicore

ifeq ($(FUTHARK_BACKEND),opencl)
OS=$(shell uname -s)
ifeq ($(OS),Darwin)
LDFLAGS=-framework OpenCL -lm
else
LDFLAGS=-lOpenCL -lm
endif
else ifeq ($(FUTHARK_BACKEND),cuda)
LDFLAGS=-lcuda -lnvrtc -lm
else ifeq ($(FUTHARK_BACKEND),c)
LDFLAGS= -lm
else ifeq ($(FUTHARK_BACKEND),multicore)
LDFLAGS=-lpthread -lm
else
$(error Unknown FUTHARK_BACKEND: $(FUTHARK_BACKEND).  Must be 'opencl', 'cuda', 'multicore', or 'c')
endif

all: ising-tui

ising.h: ising.fut
	futhark $(FUTHARK_BACKEND) --library $<

ising.o: ising.c
	$(CC) -c -o $@ $^ $(CFLAGS)

ising-tui.o: ising-tui.c ising.h
	$(CC) -c -o $@ $< $(CFLAGS)

ising-tui: ising-tui.o ising.o
	$(CC) -o $@ $^ $(LDFLAGS)
