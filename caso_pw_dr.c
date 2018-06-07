#define _GNU_SOURCE 
#define __USE_LARGEFILE64

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <aio.h>
#include <sys/time.h>

#include "config.h"
#include "general.h"

int main(int argc, char *argv[]){

  if(argc!=4){
  	printf("./s trace_name begin_stripe_ratio code_type!\n");
	exit(1);
  	}

  int i;
  int count;
  int begin_timestamp_num;
  int num_disk_stripe;
  double begin_stripe_ratio; 

  /* ===== initialize the parameters ====== */
  begin_stripe_ratio=atoi(argv[2])*1.0/100;
  strcpy(code_type, argv[3]);
  total_access_chunk_num=0;
  num_timestamp=0;
  max_access_chunks_per_timestamp=-1;
  num_rele_chunks=0;

  /* ====== judge the input code_type ======*/

  if(strcmp(code_type, "rs")==0)
  	num_disk_stripe=erasure_k+erasure_m;

  else if(strcmp(code_type, "lrc")==0)
  	num_disk_stripe=erasure_k+erasure_m+lg_prty_num*num_lg;

  else {

	printf("ERR: input code_type should be 'rs' or 'lrc' \n");
	exit(1);

  	}

  if(strcmp(code_type, "rs")==0)
  	printf("\n+++++++ trace=%s, RS(%d,%d) ========\n", argv[1], erasure_k, erasure_m);

  else if(strcmp(code_type, "lrc")==0)
  	printf("\n+++++++ trace=%s, LRC(%d,2,%d) ========\n", argv[1], erasure_k, erasure_m);

  struct timeval bg_tm, ed_tm;

  memset(access_bucket, -1, sizeof(int)*max_aces_blk_num);
  memset(order_access_bucket, 0, sizeof(int)*max_aces_blk_num);
  memset(trace_access_pattern, -1, sizeof(int)*max_aces_blk_num);
  memset(freq_access_chunk, 0, sizeof(int)*max_aces_blk_num);

  calculate_chunk_num(argv[1]);

  int count_larger_2=0;
  count=0;
  for(i=0;i<cur_rcd_idx;i++){
  	if(freq_access_chunk[i]>=2){
		count++;
		count_larger_2+=freq_access_chunk[i];
  		}
  	}

  //printf("cur_rcd_idx=%d, access_freq_all_chunks=%d \n", cur_rcd_idx, total_access_chunk_num); 
  //printf("num_chunks_accessed_more_2_times=%d, freq_chunks_access_more_2_times=%d\n", count, count_larger_2);

  // calculate the partial stripe writes io 
  begin_timestamp_num=begin_stripe_ratio*num_timestamp;

  //printf("num_timestamp=%d, validate_timestamp=%d\n", num_timestamp, begin_timestamp_num);

  // determine the begin_timestamp
  char begin_timestamp[100];
  determine_begin_timestamp(argv[1], begin_timestamp, begin_timestamp_num);

  count=0;
  for(i=0; i<caso_rcd_idx; i++)
  	if(freq_access_chunk[i]>=2)
		count++;

  //printf("max_access_chunks_per_timestamp=%d\n", max_access_chunks_per_timestamp);

  //printf("max_access_chunks_per_timestamp=%d\n",max_access_chunks_per_timestamp);
  int* analyze_chunks_time_slots=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp);// record all the accessed blocks at every timestamp
  int* access_time_slots_index=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp); // it records the index of each chunk of total_access in the trace_access_pattern
  int* num_chunk_per_timestamp=(int*)malloc(sizeof(int)*begin_timestamp_num);  // it records the number of accessed chunks in every timestamp before erasure coding
  int* sort_caso_rcd_pattern=(int*)malloc(sizeof(int)*caso_rcd_idx); //it records the sorted values of the data for correlation analysis in CASO
  int* sort_caso_rcd_index=(int*)malloc(sizeof(int)*caso_rcd_idx);
  int* sort_caso_rcd_freq=(int*)malloc(sizeof(int)*caso_rcd_idx);
  
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

  caso_stripe_ognztn(argv[1], analyze_chunks_time_slots, num_chunk_per_timestamp, begin_timestamp_num, sort_caso_rcd_pattern, sort_caso_rcd_index, sort_caso_rcd_freq);

  gettimeofday(&ed_tm, NULL);

  //printf("caso_analyze_time=%.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

  double *caso_time, *striping_time, *continugous_time;
  double f=0, g=0, h=0;
  
  caso_time=&f;
  striping_time=&g;
  continugous_time=&h; 

  /* ========== Perform partial stripe writes ========= */
  //printf("+++++++++ partial stripe writes test +++++++++\n");
  psw_time_striping(argv[1], begin_timestamp, striping_time);
  psw_time_caso(argv[1],begin_timestamp, caso_time);
  //psw_time_continugous(argv[1], begin_timestamp,continugous_time);

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

  //printf("+++++++++ degraded read test +++++++++\n");
  //dr_time_caso(argv[1], begin_timestamp, caso_num_extra_io, caso_time); 
  //dr_time_striping(argv[1], begin_timestamp, striping_num_extra_io, striping_time);
  //dr_time_continugous(argv[1], begin_timestamp, continugous_num_extra_io, continugous_time);

  //printf("caso_dr_extra_io_per_disk_failure=%.2lf, striping_dr_extra_io_per_disk_failure=%.2lf, continugous_dr_extra_io_per_disk_failure=%.2lf\n\n", 
  		//(*caso_num_extra_io)*1.0/num_disk_stripe, (*striping_num_extra_io)*1.0/num_disk_stripe, (*continugous_num_extra_io)*1.0/num_disk_stripe);

#if debug
  printf("after_sort: ognzd_crrltd_chnk\n");
  print_matrix(sort_ognzd_crrltd_chnk, ognz_crrltd_cnt, 1);

  printf("after_sort: ognzd_crrltd_chnk_index\n");
  print_matrix(sort_ognzd_crrltd_chnk_index, ognz_crrltd_cnt, 1);

  printf("after_sort: ognzd_crrltd_chnk_lg\n");
  for(i=0; i<ognz_crrltd_cnt; i++)
	  printf("%d ",ognzd_crrltd_chnk_lg[i]);
  printf("\n");
#endif

  free(num_chunk_per_timestamp);
  free(analyze_chunks_time_slots);
  free(sort_caso_rcd_pattern);
  free(sort_caso_rcd_index);
  free(access_time_slots_index);
  free(sort_caso_rcd_freq);
  free(sort_ognzd_crrltd_chnk);
  free(sort_ognzd_crrltd_chnk_index);
  free(ognzd_crrltd_chnk_lg);

  return 0;

}

