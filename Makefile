CC = gcc
CFLAGS = -g -Wall -O2 -lm -lrt
Jerasure_Dir=Jerasure

default:
	@echo "Compile general.c"
	$(CC) -c general.c -o general.o $(CFLAGS) 

	@echo "Compile partial_stripe_writes.c"
	$(CC) -o caso_test caso_test.c general.c $(Jerasure_Dir)/galois.o $(Jerasure_Dir)/reed_sol.o $(Jerasure_Dir)/jerasure.o $(CFLAGS) 

clean: 
	rm -f *.o caso_test
	rm -f $(Jerasure_Dir)/*.o 
