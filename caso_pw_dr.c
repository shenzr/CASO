#define _GNU_SOURCE 
#define __USE_LARGEFILE64

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <aio.h>
#include <sys/time.h>

#include "common.h"
#include "general.h"

#define chunk_size_kb_unit block_size/1024
#define disk_capacity 1024*1024*100

int main(int argc, char *argv[]){

  if(argc!=4){
  	printf("./s trace_name begin_stripe_ratio code_type!\n");
	exit(1);
  	}

  printf("\n\n+++++++trace=%s========\n", argv[1]);

  int i;
  int count;
  int begin_timestamp_num;
  int temp_chunk_size_kb=chunk_size_kb_unit;
  int temp_erasure_k=erasure_k;
  int num_disk_stripe;
  int num_lg;
  double begin_stripe_ratio; 

  /* ===== initialize the parameters ====== */
  begin_stripe_ratio=atoi(argv[2])*1.0/100;
  strcpy(code_type, argv[3]);
  cur_rcd_idx=0;
  total_access_chunk_num=0;
  num_timestamp=0;
  max_access_chunks_per_timestamp=-1;
  num_rele_chunks=0;

  /* ====== judge the input code_type ======*/
  num_lg=erasure_m/lg_prty_num;

  if(strcmp(code_type, "rs")==0)
  	num_disk_stripe=erasure_k+erasure_m;

  else if(strcmp(code_type, "lrc")==0)
  	num_disk_stripe=erasure_k+erasure_m+lg_prty_num*num_lg;

  else {

	printf("ERR: input code_type should be 'rs' or 'lrc' \n");
	exit(1);

  	}
  
  struct timeval bg_tm, ed_tm;

  for(i=0;i<max_aces_blk_num;i++)
  	trace_access_pattern[i]=-1;

  for(i=0;i<max_aces_blk_num;i++)
  	freq_access_chunk[i]=0;

  printf("------>calculate_chunk_num\n");
  calculate_chunk_num(argv[1]);
  sorting_trace_access_pattern();
  printf("<------calculate_chunk_num\n");

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

  printf("num_timestamp=%d, validate_timestamp=%d\n", num_timestamp, begin_timestamp_num);

  // determine the begin_timestamp
  char begin_timestamp[100];
  determine_begin_timestamp(argv[1], begin_timestamp, begin_timestamp_num);

  count=0;
  for(i=0; i<caso_rcd_idx; i++)
  	if(freq_access_chunk[i]>=2)
		count++;

  printf("caso_rcd(freq>=2)=%d\n", count);

  printf("begin_timestamp=%s, begin_timestamp_num=%d\n", begin_timestamp, begin_timestamp_num);
  printf("<------calculate_chunk_num\n");

  int max_access_chunk=sort_trace_pattern[0];

  printf("max_access_chunk/temp_erasure_k=%d, disk_capacity=%d\n", max_access_chunk/temp_erasure_k, disk_capacity/temp_chunk_size_kb);

  if(max_access_chunk/temp_erasure_k > disk_capacity){
	  printf("max_access_chunk exceeds disk_capacity!, max_access_chunk=%d, disk_capacity=%d\n", max_access_chunk/temp_erasure_k, disk_capacity);
	  exit(1);
  	}

  printf("max_access_chunks_per_timestamp=%d\n", max_access_chunks_per_timestamp);

  //printf("max_access_chunks_per_timestamp=%d\n",max_access_chunks_per_timestamp);
  int* analyze_chunks_time_slots=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp);// record all the accessed blocks at every timestamp
  int* access_time_slots_index=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp); // it records the index of each chunk of total_access in the trace_access_pattern
  int* num_chunk_per_timestamp=(int*)malloc(sizeof(int)*begin_timestamp_num);  // it records the number of accessed chunks in every timestamp before erasure coding
  int* sort_caso_rcd_pattern=(int*)malloc(sizeof(int)*caso_rcd_idx); //it records the sorted values of the data for correlation analysis in CASO
  int* sort_caso_rcd_index=(int*)malloc(sizeof(int)*caso_rcd_idx);
  int* sort_caso_rcd_freq=(int*)malloc(sizeof(int)*caso_rcd_idx);
  int* chunk_to_stripe_map=(int*)malloc(sizeof(int)*cur_rcd_idx); // it records the stripe id for each chunk
  int* chunk_to_stripe_chunk_map=(int*)malloc(sizeof(int)*cur_rcd_idx); // it records the chunk id in the stripe for each chunk
  int* chunk_to_local_group_map=(int*)malloc(sizeof(int)*cur_rcd_idx); // it records which local group a chunk is organized at
  int orig_index;

  for(i=0; i<caso_rcd_idx; i++)
  	sort_caso_rcd_pattern[i]=trace_access_pattern[i];

  for(i=0; i<caso_rcd_idx; i++)
  	sort_caso_rcd_index[i]=i;

  QuickSort_index(sort_caso_rcd_pattern, sort_caso_rcd_index, 0, caso_rcd_idx-1);

  for(i=0; i<caso_rcd_idx; i++){
  	
  	orig_index=sort_caso_rcd_index[i];
	sort_caso_rcd_freq[i]=freq_access_chunk[orig_index];

  	}
  	
  gettimeofday(&bg_tm, NULL);

  caso_stripe_ognztn(argv[1], analyze_chunks_time_slots, num_chunk_per_timestamp, begin_timestamp_num, sort_caso_rcd_pattern, sort_caso_rcd_index, 
  					 sort_caso_rcd_freq, chunk_to_stripe_map, chunk_to_stripe_chunk_map, chunk_to_local_group_map);

  gettimeofday(&ed_tm, NULL);

  printf("caso_analyze_time=%.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

  double *caso_time, *striping_time, *continugous_time;
  double f=0, g=0, h=0;
  
  caso_time=&f;
  striping_time=&g;
  continugous_time=&h; 

  /* ========== Perform partial stripe writes ========= */
  printf("+++++++++ partial stripe writes test +++++++++\n");
  psw_time_caso(argv[1],begin_timestamp, chunk_to_stripe_map, chunk_to_stripe_chunk_map, caso_time, chunk_to_local_group_map);
  psw_time_striping(argv[1], begin_timestamp, striping_time, chunk_to_local_group_map);
  psw_time_continugous(argv[1], begin_timestamp,continugous_time, chunk_to_local_group_map);

  /* ========== Perform degraded reads ========= */
  int *caso_num_extra_io;
  int c=0;
  caso_num_extra_io=&c;

  int *striping_num_extra_io;
  int d=0;
  striping_num_extra_io=&d;

  int *continugous_num_extra_io;
  int e=0;
  continugous_num_extra_io=&e;

  printf("+++++++++ degraded read test +++++++++\n");
  dr_time_caso(argv[1], begin_timestamp, chunk_to_stripe_map, chunk_to_stripe_chunk_map, caso_num_extra_io, caso_time); 
  dr_time_striping(argv[1], begin_timestamp, striping_num_extra_io, striping_time);
  dr_time_continugous(argv[1], begin_timestamp, continugous_num_extra_io, continugous_time);

  printf("caso_dr_extra_io_per_disk_failure=%.2lf, striping_dr_extra_io_per_disk_failure=%.2lf, continugous_dr_extra_io_per_disk_failure=%.2lf\n", 
  		(*caso_num_extra_io)*1.0/num_disk_stripe, (*striping_num_extra_io)*1.0/num_disk_stripe, (*continugous_num_extra_io)*1.0/num_disk_stripe);

  free(chunk_to_stripe_map);
  free(chunk_to_stripe_chunk_map);
  free(num_chunk_per_timestamp);
  free(analyze_chunks_time_slots);
  free(sort_caso_rcd_pattern);
  free(sort_caso_rcd_index);
  free(access_time_slots_index);
  free(chunk_to_local_group_map);
  free(sort_caso_rcd_freq);

  return 0;

}

