CC = gcc
CFLAGS = -g -Wall -O2 -lm -lrt


default:

	@echo "Compile general.c"
	$(CC) $(CFLAGS) -c general.c -o general.o 

	@echo "Compile partial_stripe_writes.c"
	$(CC) $(CFLAGS) -o partial_stripe_writes partial_stripe_writes.c general.o 

	#@echo "Compile degraded_reads.c"
	#$(CC) $(CFLAGS) -o degraded_reads degraded_reads.c general.o 
