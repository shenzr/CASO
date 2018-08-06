CC = gcc
CFLAGS = -g -Wall -O2 -lm -lrt 


default:
	@echo "Compile general.c"
	$(CC) -c general.c -o general.o $(CFLAGS)

	@echo "Compile partial_stripe_writes.c"
	$(CC) -o caso_test caso_test.c general.o Jerasure/galois.o Jerasure/reed_sol.o Jerasure/jerasure.o $(CFLAGS) 
