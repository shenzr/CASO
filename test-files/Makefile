CC = gcc
CFLAGS = -g -Wall -O2 -lm -pthread
Jerasure_dir = ../Jerasure

default:
	$(CC) $(CFLAGS) -o test_encoding test_encoding.c $(Jerasure_dir)/galois.o $(Jerasure_dir)/reed_sol.o $(Jerasure_dir)/jerasure.o
