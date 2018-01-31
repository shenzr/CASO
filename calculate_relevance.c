#define _GNU_SOURCE 

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>


#include "common.h"
#include "general.h"



int main(int argc, char *argv[]){

  if(argc!=2){
  	printf("./s trace_name!\n");
	exit(1);
  	}

  int i;
  int count;

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
  
  calculate_chunk_num(argv[1]);

  int count_larger_2=0;
  count=0;
  for(i=0;i<cur_rcd_idx;i++){
  	if(freq_access_chunk[i]>=2){
		count++;
		count_larger_2+=freq_access_chunk[i];
  		}
  	}

  // calculate the partial stripe writes io 
  int begin_timestamp_num=num_timestamp;


  // determine the begin_timestamp
  char begin_timestamp[100];

  printf("------>calculate_chunk_num\n");

  determine_begin_timestamp(argv[1], begin_timestamp, begin_timestamp_num);

  printf("begin_timestamp=%s, begin_timestamp_num=%d\n", begin_timestamp, begin_timestamp_num);
  
  //printf("<------calculate_chunk_num\n");
  //printf("striping_io_count=%d, contiguous_io_count=%d\n",striping_io_count, contiguous_io_count);
  // calculate the extra partial stripe writes in caso
  // caso_strp_io_count=calculate_psw_io_caso(argv[1], begin_timestamp);
  //printf("------>calculate_caso_chunk_num\n");
  //calculate_caso_chunk_num(argv[1],begin_timestamp);
  //printf("caso_rcd_idx=%d\n",caso_rcd_idx);
  //printf("<------calculate_caso_chunk_num\n");

  //printf("max_access_chunks_per_timestamp=%d\n",max_access_chunks_per_timestamp);
  int *total_access=malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp);// record all the accessed blocks at every timestamp
  int *total_access_index=malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp);// record the index of each chunk of access_table in the trace_access_pattern
  int *mark_relevant_access_table=malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp); // it mark if a chunk is relevant at each timestamp
  int *num_chunk_per_timestamp=malloc(sizeof(int)*begin_timestamp_num);  // it records the number of accessed chunks in every timestamp before erasure coding
  int *sort_caso_rcd_pattern=(int*)malloc(sizeof(int)*caso_rcd_idx); //it records the sorted values of the data for correlation analysis in CASO
  int *sort_caso_rcd_index=(int*)malloc(sizeof(int)*caso_rcd_idx);

  printf("!!!! caso_rcd_idx=%d\n", caso_rcd_idx);

  printf("------>calculate_relevance_chunk_num\n");
  for(i=0; i<caso_rcd_idx; i++)
  	sort_caso_rcd_pattern[i]=trace_access_pattern[i];

  for(i=0; i<caso_rcd_idx; i++)
  	sort_caso_rcd_index[i]=i;

  printf("------>calculate_relevance_chunk_num\n");
  calculate_relevance_chunk_num(argv[1], total_access, total_access_index, num_chunk_per_timestamp, mark_relevant_access_table, begin_timestamp_num, sort_caso_rcd_pattern, sort_caso_rcd_index);
  printf("<------calculate_relevance_chunk_num\n");

  // calculate the ratio between the accesses times to relevant chunks and the access times to all the chunks 

  count=0;
  int temp_count=0;

  printf("cur_rcd_idx=%d\n",cur_rcd_idx);
  
  for(i=0;i<cur_rcd_idx;i++){

	if(mark_if_relevant[i]>=1){
		count+=freq_access_chunk[i];
		temp_count++;
		}

  	}
  

  printf("(num_rele_chunks)/(num_chunks)=%.2lf\n", temp_count*1.0/cur_rcd_idx);
  printf("(access_freq_relevant_chunks)/(access_freq_all_chunks)=%.2lf\n",count*1.0/total_access_chunk_num);
  
  free(total_access);
  free(num_chunk_per_timestamp);
  free(total_access_index);
  free(mark_relevant_access_table);
  free(num_chunk_per_timestamp);
  free(sort_caso_rcd_index);
  free(sort_caso_rcd_pattern);

  return 0;
   
}

