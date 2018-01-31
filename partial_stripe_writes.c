#define _GNU_SOURCE 

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <aio.h>

#include "common.h"
#include "general.h"

int main(int argc, char *argv[]){

  if(argc!=3){
  	printf("./s trace_name begin_stripe_ratio !\n");
	exit(1);
  	}

  printf("\n\n+++++++trace=%s========\n", argv[1]);

  int i;
  int count;
  int num_stripe;
  int begin_timestamp_num;
  int striping_io_count;
  int contiguous_io_count;

  double begin_stripe_ratio=atoi(argv[2])*1.0/100;

  cur_rcd_idx=0;
  total_access_chunk_num=0;
  num_timestamp=0;
  max_access_chunks_per_timestamp=-1;

  num_rele_chunks=0;

  for(i=0;i<max_aces_blk_num;i++)
  	trace_access_pattern[i]=-1;

  for(i=0;i<max_aces_blk_num;i++)
  	mark_if_relevant[i]=0;;

  for(i=0;i<max_aces_blk_num;i++)
  	freq_access_chunk[i]=0;

  //printf("------>calculate_chunk_num\n");
  calculate_chunk_num(argv[1]);
  sorting_trace_access_pattern();
  //printf("<------calculate_chunk_num\n");

  int count_larger_2=0;
  count=0;
  for(i=0;i<cur_rcd_idx;i++){
  	if(freq_access_chunk[i]>=2){
		count++;
		count_larger_2+=freq_access_chunk[i];
  		}
  	}

  printf("cur_rcd_idx=%d, access_freq_all_chunks=%d \n", cur_rcd_idx, total_access_chunk_num); 
  printf("num_chunks_accessed_more_2_times=%d, freq_chunks_access_more_2_times=%d\n", count, count_larger_2);

  // calculate the partial stripe writes io 
  begin_timestamp_num=begin_stripe_ratio*num_timestamp;

  // determine the begin_timestamp
  char begin_timestamp[100];

  printf("------>calculate_chunk_num\n");

  determine_begin_timestamp(argv[1], begin_timestamp, begin_timestamp_num);

  printf("begin_timestamp=%s, begin_timestamp_num=%d\n", begin_timestamp, begin_timestamp_num);
  printf("<------calculate_chunk_num\n");

  int max_access_chunk=sort_trace_pattern[0];
  int min_access_chunk=sort_trace_pattern[cur_rcd_idx-1];
  int start_striping_chunk=min_access_chunk/erasure_k*erasure_k;

  // calculate the extra partial stripe writes in sequential stripe
  striping_io_count=calculate_psw_io_striping_stripe(argv[1], begin_timestamp); // i.e., organizing the data by following horizontal data placement
  contiguous_io_count=calculate_psw_io_continugous_stripe(argv[1], begin_timestamp, max_access_chunk); // i.e., organizing the data by following vertical data placement

  printf("striping_io_count=%d, contiguous_io_count=%d\n",striping_io_count, contiguous_io_count);

  //calculate_caso_chunk_num(argv[1],begin_timestamp);

  //printf("max_access_chunks_per_timestamp=%d\n",max_access_chunks_per_timestamp);
  int *total_access=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp);// record all the accessed blocks at every timestamp
  int *total_access_index=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp); // it records the index of each chunk of total_access in the trace_access_pattern
  int *total_access_caso_rele_index=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp); // it records the mapping from the relevant chunk's index in total_access to that of the relevant_chunk_table
  int *mark_relevant_access_table=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp); // it mark if a chunk is relevant at each timestamp
  int *num_chunk_per_timestamp=(int*)malloc(sizeof(int)*begin_timestamp_num);  // it records the number of accessed chunks in every timestamp before erasure coding
  int *sort_caso_rcd_pattern=(int*)malloc(sizeof(int)*caso_rcd_idx); //it records the sorted values of the data for correlation analysis in CASO
  int *sort_caso_rcd_index=(int*)malloc(sizeof(int)*caso_rcd_idx);

  printf("!!!! caso_rcd_idx=%d\n", caso_rcd_idx);

  printf("------>calculate_relevance_chunk_num\n");
  for(i=0; i<caso_rcd_idx; i++)
  	sort_caso_rcd_pattern[i]=trace_access_pattern[i];

  for(i=0; i<caso_rcd_idx; i++)
  	sort_caso_rcd_index[i]=i;

  caso_correlation_degree(argv[1], total_access, total_access_index, num_chunk_per_timestamp, mark_relevant_access_table, begin_timestamp_num, sort_caso_rcd_pattern, sort_caso_rcd_index);


  calculate_relevance_chunk_num(argv[1], total_access, total_access_index, num_chunk_per_timestamp, mark_relevant_access_table, begin_timestamp_num, sort_caso_rcd_pattern, sort_caso_rcd_index);

  int caso_num_rele_chunks=0;
  for(i=0;i<caso_rcd_idx;i++){
  	if(mark_if_relevant[i]>=1)
		caso_num_rele_chunks++;
  	}
  printf("caso_num_rele_chunks=%d\n",caso_num_rele_chunks);









/*
  
  calculate_relevance_chunk_num(argv[1], total_access, total_access_index, num_chunk_per_timestamp, mark_relevant_access_table, begin_timestamp_num, sort_caso_rcd_pattern, sort_caso_rcd_index);
  printf("<------calculate_relevance_chunk_num\n");

  // calculate the number of num of relevant chunks
  // and record them 
  int caso_num_rele_chunks=0;

  for(i=0;i<caso_rcd_idx;i++){
  	if(mark_if_relevant[i]>=1)
		caso_num_rele_chunks++;
  	}

  printf("caso_num_rele_chunks=%d\n",caso_num_rele_chunks);

  int *caso_relevant_chunks=(int*)malloc(sizeof(int)*caso_num_rele_chunks); // it records the relevant chunks in the trace_access_pattern according to their accessed time
  int *caso_relevant_set=(int*)malloc(sizeof(int)*caso_num_rele_chunks*max_num_relevent_chunks_per_chunk); // it records the index of relevant chunks for every relenvat data chunk
  int *caso_relevant_degree=(int*)malloc(sizeof(int)*caso_num_rele_chunks*max_num_relevent_chunks_per_chunk); // it records the degree of relevant chunks to every relevant data chun
  int *caso_rele_chunk_index=(int*)malloc(sizeof(int)*caso_num_rele_chunks);

  // initialize the caso_relevant_set and caso_relevant_degree
  for(i=0; i<caso_num_rele_chunks*max_num_relevent_chunks_per_chunk; i++){
  	caso_relevant_set[i]=-1; 
	caso_relevant_degree[i]=0;
  	}


  // initialize caso_rele_chunk_index and caso_relevant_chunks
  count=0;
  for(i=0;i<caso_rcd_idx;i++){
  	if(mark_if_relevant[i]>=1){

		caso_rele_chunk_index[count]=i;
		count++;

  		}
  	}
  	
  count=0;
  
  for(i=0;i<caso_rcd_idx;i++){

	if(mark_if_relevant[i]>=1)
		caso_relevant_chunks[count++]=trace_access_pattern[i];

  	}

  printf("---->caso_num_rele_chunks=%d\n",caso_num_rele_chunks);

  calculate_relevance_degree(argv[1], mark_relevant_access_table, caso_relevant_set, caso_relevant_degree, 
  	total_access, total_access_index, total_access_caso_rele_index, num_chunk_per_timestamp, caso_relevant_chunks, caso_num_rele_chunks, begin_timestamp_num);

  printf("<-----caso_num_rele_chunks=%d\n",caso_num_rele_chunks);
  num_stripe=(max_access_chunk-start_striping_chunk)/(block_size*erasure_k);

  //printf("num_stripe=%d\n",num_stripe);

















  int *chunk_to_stripe_map=(int*)malloc(sizeof(int)*cur_rcd_idx); // it records the stripe id for each chunk
  int *chunk_to_stripe_chunk_map=(int*)malloc(sizeof(int)*cur_rcd_idx); // it records the chunk id in the stripe for each chunk
  
  stripe_organization(caso_relevant_chunks, caso_relevant_set, caso_relevant_degree, chunk_to_stripe_map, chunk_to_stripe_chunk_map, num_stripe, 
  	caso_num_rele_chunks, caso_rele_chunk_index);

  int caso_psw_io=calculate_psw_io_caso_stripe(argv[1], begin_timestamp, chunk_to_stripe_map); 
  //printf("caso_psw_io=%d\n", caso_psw_io);


  double *caso_time, *striping_time, *continugous_time;
  double f=0, g=0, h=0;
  
  caso_time=&f;
  striping_time=&g;
  continugous_time=&h; 

  psw_time_caso(argv[1],begin_timestamp, chunk_to_stripe_map, chunk_to_stripe_chunk_map,caso_time);
  psw_time_striping(argv[1], begin_timestamp,striping_time);
  psw_time_continugous(argv[1], begin_timestamp,continugous_time);

  printf("caso_psw_io=%d, striping_io_count=%d, contiguous_io_count=%d\n", caso_psw_io, striping_io_count, contiguous_io_count);
  printf("caso_time=%.2lf, striping_time=%.2lf, continugous_time=%.2lf\n", *caso_time, *striping_time, *continugous_time);

  free(caso_relevant_set);
  free(caso_relevant_degree);
  free(caso_relevant_chunks);
  free(caso_rele_chunk_index);
  free(chunk_to_stripe_map);
  free(chunk_to_stripe_chunk_map);

*/

  free(mark_relevant_access_table);
  free(num_chunk_per_timestamp);
  free(total_access);
  free(total_access_index);
  free(total_access_caso_rele_index);
  free(sort_caso_rcd_pattern);
  free(sort_caso_rcd_index);

  return 0;

}

