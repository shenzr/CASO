// this program reads read patterns from workloads and measure the ratio 
// (#the number of blocks requested with other blocks in more than 2 I/O operations)/(# the number of accessed blocks in a workload)

#define _GNU_SOURCE 

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <math.h>
#include <aio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <limits.h>
#include <unistd.h>

#include "jerasure/jerasure.h"
#include "jerasure/galois.h"
#include "common.h"

#define inf 999999999
#define num_assume_timestamp 1000000
#define disk_capacity 250*1024*1024*1024/block_size
#define max_num_peer_chunks 100000  // we call the chunks that are accessed within a timestamp peer chunks

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))


int num_distinct_chunks_timestamp[num_assume_timestamp];

/*
// given the io_matrix and the stripe_map, calculate the number of disk seeks 
int calculate_disk_seeks(int *stripe_map, int stripe_count, int *io_matrix){



}
*/


void print_matrix(int* matrix, int len, int width){

  int i,j; 
  for(i=0; i<width; i++){

	for(j=0; j<len; j++)
		printf("%d ", matrix[i*len+j]);

	printf("\n");
	
  	}
}

void trnsfm_char_to_int(char *char_data, long long *data){

  int i=0;
  *data=0LL;

  while(char_data[i]!='\0'){
	if(char_data[i]>='0' && char_data[i]<='9'){
	  (*data)*=10;
  	  (*data)+=char_data[i]-'0';
		}
	i++;
  	}
}

int calculate_num_io(int *io_request, int col, int width){

	int num=0;
	int i;
	
	for(i=0; i<col*width; i++)
		if(io_request[i]==1)
			num++;

	return num;

}


/* this function truncate a component from a string according to a given divider */
void new_strtok(char string[], char divider, char result[]){

  int i,j;

  for(i=0;string[i]!='\0';i++){

	if(string[i]!=divider)
		result[i]=string[i];

	else break;
	
  	}

  // if the i reaches the tail of the string 
  if(string[i]=='\0')
  	result[i]='\0';

  // else it finds the divider
  else {

	// seal the result string 
	result[i]='\0';

	// shift the string and get a new string

	for(j=0;string[j]!='\0';j++)
		string[j]=string[j+i+1];
		
  	}
}


void calculate_chunk_num(char *trace_name){

	total_num_req=0;

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  perror(fp);
	  exit(0);
  	  }


     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';
	 char pre_timestamp[100];
	 
	 int i,j;
	 int access_start_block, access_end_block;
	 int cur_index;
	 int num_distict_chunks_per_timestamp;

	 int count;
	 int temp_count;
	 int if_begin;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;
	 num_distict_chunks_per_timestamp=0;
	 if_begin=1;

     //the max_access_chunks_per_timestamp is not the exact value, it did not consider the multiple accesses 
     //to a chunk within a timestamp
	 max_access_chunks_per_timestamp=-1;

	 for(i=0;i<num_assume_timestamp;i++)
	 	num_distinct_chunks_timestamp[i]=0;

	 ////printf("%s\n",pre_timestamp);

     while(fgets(operation, sizeof(operation), fp)) {

		count++;
		total_num_req++;

		//printf("count=%d\n",count);

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);

	    //printf("\n\n\ncount=%d, timestamp=%s, op_type=%s, offset=%s, size=%s\n", count, timestamp, op_type, offset, size);
        // printf("cur_rcd_idx=%d\n",cur_rcd_idx);
		// analyze the access pattern 
		// if it is accessed in the same timestamp

		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;
		
		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;

		total_access_chunk_num += access_end_block - access_start_block + 1;

		//if it is the begining of the first pattern of a timestamp, then collect the info of last timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){
				
			//printf("\n%s\n",pre_timestamp);
			strcpy(pre_timestamp,timestamp);
			temp_count=access_end_block-access_start_block+1;
			
            //if it is not the first timestamp, then initialize parameters
		    if(if_begin==0){

			   num_distinct_chunks_timestamp[num_timestamp]=num_distict_chunks_per_timestamp;
			   num_distict_chunks_per_timestamp=0;

               //record the maximum number of accessed chunks in a timestamp
			   if(temp_count>max_access_chunks_per_timestamp)
			   	 max_access_chunks_per_timestamp=temp_count;

			   num_timestamp++;

		    	}

			else
				if_begin=0;
				
			}

		else
			temp_count+=access_end_block-access_start_block+1;
			
	   for(i=access_start_block; i<=access_end_block; i++){
	   
		   for(j=0; j<cur_rcd_idx; j++){
			   if(trace_access_pattern[j]==i){
				   freq_access_chunk[j]++;
				   break;
				   }
			   }
	   
	   
		   if(j>=cur_rcd_idx){
			   trace_access_pattern[cur_rcd_idx]=i;
			   freq_access_chunk[cur_rcd_idx]++;
			   num_distict_chunks_per_timestamp++;
			   cur_rcd_idx++;
			   }
		   }

	   
       }

	 // for the access pattern in the last timestamp
	 if(temp_count>max_access_chunks_per_timestamp)
	 	max_access_chunks_per_timestamp=temp_count;

	 num_distinct_chunks_timestamp[num_timestamp]=num_distict_chunks_per_timestamp;
	 num_timestamp++;

	 fclose(fp);

	 //printf("num_distict_chunks_per_timestamp:\n");

	 temp_count=0;

	 for(i=0;i< num_timestamp; i++){
	 	printf("%d ",num_distinct_chunks_timestamp[i]);
		temp_count+=num_distinct_chunks_timestamp[i];
	 	}
	 //printf("\n");

	 printf("total_distinct_chunks=%d\n", temp_count);

#if debug

     printf("trace_access_pattern:\n");
	 for(i=0;i<cur_rcd_idx;i++)
	 	printf("%d ", trace_access_pattern[i]);
	 printf("\n");
	 
#endif
	 
	
}



void determine_begin_timestamp(char *trace_name, char begin_timestamp[], int begin_timestamp_num){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }
	 
     char operation[100];
	 char* timestamp=NULL;
	 char pre_timestamp[100];

	 int temp_count=0;

	 caso_rcd_idx=0;
	 
     while(fgets(operation, sizeof(operation), fp)) {

		//printf("count=%d\n",count);

		timestamp=strtok(operation,"\t");

		// calculate the numeber of timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

			caso_rcd_idx+=num_distinct_chunks_timestamp[temp_count];
			temp_count++;

			if(temp_count==begin_timestamp_num){

			   strcpy(begin_timestamp, timestamp);
			   break;

				}

			strcpy(pre_timestamp,timestamp);

			}
	   
       }

	 printf("test_caso_rcd_idx=%d\n", caso_rcd_idx);

	 fclose(fp);
	
}

// this function calculates the number of accessed chunks analyzed in caso 
void calculate_caso_chunk_num(char *trace_name, char begin_timestamp[]){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }


     char operation[100];
	 char timestamp[100];
	 char divider='\t';
	 char pre_timestamp[100];
	 
	 int cur_index;
	 int count;
	 int temp_count;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=-1;

	 fgets(operation, sizeof(operation), fp);
	 new_strtok(operation,divider, pre_timestamp);
	 fseek(fp,0,SEEK_SET);

	 caso_rcd_idx=0;

     count=0;
     while(fgets(operation, sizeof(operation), fp)) {

		//printf("count=%d\n",count);

		new_strtok(operation,divider, timestamp);

		if(strcmp(pre_timestamp,timestamp)!=0){

		  // calculate the numeber of timestamp
		  if(strcmp(begin_timestamp,timestamp)!=0){

			caso_rcd_idx+=num_distinct_chunks_timestamp[count];

			}

		  else break;
		  

		  strcpy(pre_timestamp, timestamp);

		  count++;
		  
     	  }
	   
       }

	 printf("======caso_rcd_idx=%d\n", caso_rcd_idx);

	 fclose(fp);
	
}


void insert_chunk_to_bucket(int* bucket, int bucket_num, int bucket_depth, int chunk_id){

	int bucket_id;
	int i;

	bucket_id=chunk_id%bucket_num;

	for(i=0; i<bucket_depth; i++){

		if(bucket[i*bucket_num+bucket_id]==chunk_id)
			break;

		if(bucket[i*bucket_num+bucket_id]==-1){

			bucket[i*bucket_num+bucket_id]=chunk_id;
			break;

			}
		}
}


void find_correlated_chunks(int bgn_tmstmp_num, int* total_access, int* sort_caso_rcd_pattern, int* num_chunk_per_timestamp){

	 int i,j; 
	 int chk_idx1, chk_idx2;
	 int chunk_id1, chunk_id2;
	 int sort_chk_idx1, sort_chk_idx2;	
	 int count;
	 int num_correlated_chunk;
	 int num_correlated_pattern;
	 int bucket_num, bucket_depth;
	 int cell_num;
	 int flag;
	 int temp_chunk;

	 bucket_depth=20;
	 bucket_num=caso_rcd_idx/bucket_depth+1;
	 cell_num=bucket_depth*bucket_num;

	 int* rcd_peer_chks=(int*)malloc(caso_rcd_idx*sizeof(int)*max_num_peer_chunks); 
	 int* freq_peer_chks=(int*)malloc(caso_rcd_idx*sizeof(int)*max_num_peer_chunks);
	 //use a bucket to store the correlated chunks
	 int* correlate_chunk_bucket=(int*)malloc(sizeof(int)*cell_num); //the number of correlated data chunks should be no larger than caso_rcd_idx

	 memset(rcd_peer_chks, -1, sizeof(int)*caso_rcd_idx*max_num_peer_chunks);
	 memset(freq_peer_chks, 0, sizeof(int)*caso_rcd_idx*max_num_peer_chunks);
	 memset(correlate_chunk_bucket, -1, sizeof(int)*cell_num);

  	 for(i=0; i<caso_rcd_idx; i++)
	 	rcd_peer_chks[i*max_num_peer_chunks]=sort_caso_rcd_pattern[i];

     //record the chunks that are accessed within a timestamp and update the frequency when they are simultaneously accessed
	 for(i=0; i<bgn_tmstmp_num-1;i++){

		if(num_chunk_per_timestamp[i]==1)
			continue;

		for(chk_idx1=0; chk_idx1<num_chunk_per_timestamp[i]-1; chk_idx1++){

			for(chk_idx2=chk_idx1+1; chk_idx2<num_chunk_per_timestamp[i]; chk_idx2++){

				chunk_id1=total_access[i*max_access_chunks_per_timestamp+chk_idx1];
				chunk_id2=total_access[i*max_access_chunks_per_timestamp+chk_idx2];

				sort_chk_idx1=binary_search(sort_caso_rcd_pattern, caso_rcd_idx, chunk_id1);
				sort_chk_idx2=binary_search(sort_caso_rcd_pattern, caso_rcd_idx, chunk_id2);

				//update rcd_peer_chks and freq_peer_chks
				for(j=1; j<max_num_peer_chunks; j++){

                    //if the chunk has been recorded, then update frequency
					if(rcd_peer_chks[sort_chk_idx1*max_num_peer_chunks+j]==chunk_id2){
						
						freq_peer_chks[sort_chk_idx1*max_num_peer_chunks+j]++;
						break;
						
						}

                    //otherwise, record the chunk and update frequency
					if(rcd_peer_chks[sort_chk_idx1*max_num_peer_chunks+j]==-1){

						rcd_peer_chks[sort_chk_idx1*max_num_peer_chunks+j]=chunk_id2;
						freq_peer_chks[sort_chk_idx1*max_num_peer_chunks+j]++;
						break;

						}

					}

				}

			}

	 	}

	 //calculate correlated chunk num
	 num_correlated_chunk=0;
	 num_correlated_pattern=0;
	 
	 for(i=0; i<caso_rcd_idx; i++){

		flag=0;

		for(j=1; j<max_num_peer_chunks; j++){
			
			if(freq_peer_chks[i*max_num_peer_chunks+j]>=2){

				//insert the chunk into the bucket
				temp_chunk=rcd_peer_chks[i*max_num_peer_chunks+j];
				insert_chunk_to_bucket(correlate_chunk_bucket, bucket_num, bucket_depth, temp_chunk);
				flag=1;

				}

			if(freq_peer_chks[i*max_num_peer_chunks+j]==0)
				break;
			}

		if(flag==1)
			insert_chunk_to_bucket(correlate_chunk_bucket, bucket_num, bucket_depth, rcd_peer_chks[i*max_num_peer_chunks]);

	 	}

	 //count the correlated data chunks
	 num_correlated_chunk=0;
	 for(i=0; i<cell_num; i++)
	 	if(correlate_chunk_bucket[i]!=-1)
			num_correlated_chunk++;

	 printf("=====>num_correlated_chunk=%d\n", num_correlated_chunk);

	 printf("correlated_chunk_bucket=%d\n");
	 print_matrix(correlate_chunk_bucket, bucket_num, bucket_depth);
	 

     free(rcd_peer_chks);
	 free(freq_peer_chks);
	 free(correlate_chunk_bucket);
	 
}


void caso_correlation_degree(char *trace_name,  int *total_access, int *total_access_index, int *num_chunk_per_timestamp, 
	int *mark_relevant_access_table, int bgn_tmstmp_num, int* sort_caso_rcd_pattern, int* sort_caso_rcd_index){

     //read the data from csv file
     FILE *fp;

	 if((fp=fopen(trace_name,"r"))==NULL){
	   //printf("open file failed\n");
	   exit(0);
	   }
	 
     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';
	 char pre_timestamp[100];

	 int i,j;
	 int k,h;
	 int p;
	 int count_timestamp;
	 int count;
	 int access_start_block, access_end_block;
	 int cur_index;
	 int temp_chunk_i, temp_chunk_j;
	 int num_flag;
	 int pre_chunk;
	 int pre_k;
	 int pre_chunk_index;
	 int sort_index, real_index;
	 int if_begin;	 

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0; 
	 count_timestamp=0;
	 if_begin=1;

	 printf("=======>test:\n");
	 printf("max_access_chunks_per_timestamp=%d\n", max_access_chunks_per_timestamp);

	 for(i=0;i<bgn_tmstmp_num*max_access_chunks_per_timestamp;i++)
	 	total_access[i]=0;

	 for(i=0;i<bgn_tmstmp_num*max_access_chunks_per_timestamp;i++)
	 	total_access_index[i]=0;

	 for(i=0;i<bgn_tmstmp_num*max_access_chunks_per_timestamp;i++)
	 	mark_relevant_access_table[i]=0;

	 for(i=0;i<bgn_tmstmp_num;i++)
	 	num_chunk_per_timestamp[i]=0;

	 QuickSort_index(sort_caso_rcd_pattern, sort_caso_rcd_index, 0, caso_rcd_idx-1);

	 //printf("=====\n");
	 
     /* record the access chunks per timestamp in a table */
	 while(fgets(operation, sizeof(operation), fp)){

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);
		
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;
		
		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;
		
		
		// analyze the access pattern 
		// if it is accessed in the same timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

		   //printf("count_timestamp=%d\n",count_timestamp);
		   if(if_begin==0){
		       num_chunk_per_timestamp[count_timestamp]=cur_index;
		       cur_index=0; 
		       count_timestamp++;

		       if(count_timestamp==bgn_tmstmp_num) 
			     break;
		   	}

		   else if_begin=0;
			
		   strcpy(pre_timestamp, timestamp);
		
			}

		for(k=access_start_block; k<=access_end_block; k++){

			for(p=0; p<cur_index; p++)
				if(total_access[count_timestamp*max_access_chunks_per_timestamp+p]==k)
					break;
				
			if(p<cur_index)
				continue;
			
			total_access[count_timestamp*max_access_chunks_per_timestamp+cur_index]=k;
		
			// record the index of k in total_access_index
			sort_index=binary_search(sort_caso_rcd_pattern, caso_rcd_idx, k);
			real_index=sort_caso_rcd_index[sort_index];
			total_access_index[count_timestamp*max_access_chunks_per_timestamp+cur_index]=real_index;
			cur_index++;
		 }

	 	}

	 find_correlated_chunks(bgn_tmstmp_num, total_access, sort_caso_rcd_pattern, num_chunk_per_timestamp);

	 //calculate the correlation degree

	 

}


void calculate_relevance_chunk_num(char *trace_name,  int *total_access, int *total_access_index, int *num_chunk_per_timestamp, 
	int *mark_relevant_access_table, int bgn_tmstmp_num, int* sort_caso_rcd_pattern, int* sort_caso_rcd_index){

    //read the data from csv file
     FILE *fp;

	 if((fp=fopen(trace_name,"r"))==NULL){
	   //printf("open file failed\n");
	   exit(0);
	   }
	 
     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';
	 char pre_timestamp[100];

	 int i,j;
	 int k,h;
	 int p;
	 int count_timestamp;
	 int count;
	 int access_start_block, access_end_block;
	 int cur_index;
	 int temp_chunk_i, temp_chunk_j;
	 int num_flag;
	 int pre_chunk;
	 int pre_k;
	 int pre_chunk_index;
	 int sort_index, real_index;
	 int if_begin;
	 

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0; 
	 count_timestamp=0;
	 if_begin=1;

	 printf("=======>test:\n");
	 printf("max_access_chunks_per_timestamp=%d\n", max_access_chunks_per_timestamp);

	 for(i=0;i<bgn_tmstmp_num*max_access_chunks_per_timestamp;i++)
	 	total_access[i]=0;

	 for(i=0;i<bgn_tmstmp_num*max_access_chunks_per_timestamp;i++)
	 	total_access_index[i]=0;

	 for(i=0;i<bgn_tmstmp_num*max_access_chunks_per_timestamp;i++)
	 	mark_relevant_access_table[i]=0;

	 for(i=0;i<bgn_tmstmp_num;i++)
	 	num_chunk_per_timestamp[i]=0;

	 QuickSort_index(sort_caso_rcd_pattern, sort_caso_rcd_index, 0, caso_rcd_idx-1);

	 //printf("=====\n");
	 
     /* record the access chunks per timestamp in a table */
	 while(fgets(operation, sizeof(operation), fp)){

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);
		
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;
		
		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;
		
		
		// analyze the access pattern 
		// if it is accessed in the same timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

		   //printf("count_timestamp=%d\n",count_timestamp);
		   if(if_begin==0){
		       num_chunk_per_timestamp[count_timestamp]=cur_index;
		       cur_index=0; 
		       count_timestamp++;

		       if(count_timestamp==bgn_tmstmp_num) 
			     break;
		   	}

		   else if_begin=0;
			
		   strcpy(pre_timestamp, timestamp);
		
			}

		for(k=access_start_block; k<=access_end_block; k++){

			for(p=0; p<cur_index; p++)
				if(total_access[count_timestamp*max_access_chunks_per_timestamp+p]==k)
					break;
				
			if(p<cur_index)
				continue;
			
			total_access[count_timestamp*max_access_chunks_per_timestamp+cur_index]=k;
		
			// record the index of k in total_access_index
			sort_index=binary_search(sort_caso_rcd_pattern, caso_rcd_idx, k);
			real_index=sort_caso_rcd_index[sort_index];
			total_access_index[count_timestamp*max_access_chunks_per_timestamp+cur_index]=real_index;
			cur_index++;
		 }

	 	}


/*
     printf("----> num_chunk_per_timestamp:\n");
	 for(i=0;i<num_timestamp;i++)
	 	printf("%d ", num_chunk_per_timestamp[i]);
	 printf("\n");
*/
	 /* calculate the relevance betwen any two chunks */
/*
	 printf("----> total_access:\n");

     for(i=0;i<num_timestamp;i++){

		for(j=0;j<max_access_chunks_per_timestamp;j++)
			printf("%d ", total_access[i*max_access_chunks_per_timestamp+j]);

		printf("\n");
     	}
*/

     printf("+++++++++bgn_tmstmp_num=%d\n", bgn_tmstmp_num);

	 for(i=0;i<bgn_tmstmp_num-1;i++){

        //if(i%100==0) 
			//printf("i=%d, num_timestamp=%d\n", i, time_range);

        // if there is only one chunk accessed in this timestamp, then pass it
		if(num_chunk_per_timestamp[i]==1)
			continue;

		for(j=i+1;j<bgn_tmstmp_num;j++){

			if(num_chunk_per_timestamp[j]==1)
				continue;

			num_flag=0;

			for(k=0; k<num_chunk_per_timestamp[i]; k++){

				temp_chunk_i=total_access[i*max_access_chunks_per_timestamp+k];

				for(h=0; h<num_chunk_per_timestamp[j]; h++){

					temp_chunk_j=total_access[j*max_access_chunks_per_timestamp+h];

					if(temp_chunk_i == temp_chunk_j){

						num_flag++;

                        // if there is more than two chunks appeared in the two access patterns
                        // then they are relevant and recorded
                       
						if(num_flag==2){ // record the pre_chunk and current chunk

						  mark_if_relevant[total_access_index[i*max_access_chunks_per_timestamp+pre_chunk_index]]++;
						  mark_if_relevant[total_access_index[i*max_access_chunks_per_timestamp+k]]++;

						  //printf("pre_k=%d,max_access_chunks_per_timestamp=%d\n",pre_k,max_access_chunks_per_timestamp);
						  // record the pre_k and k at the i-th timestamp as relevant chunks
						  mark_relevant_access_table[i*max_access_chunks_per_timestamp+pre_k]=1;
						  mark_relevant_access_table[i*max_access_chunks_per_timestamp+k]=1;
						  
							}


						if(num_flag>2){ // only record the current chunk

	                       mark_if_relevant[total_access_index[i*max_access_chunks_per_timestamp+k]]++;
						   
						   // record the k-th chunk at the i-th timestamp as relevant chunk
						   mark_relevant_access_table[i*max_access_chunks_per_timestamp+k]=1;
							
							}

						pre_chunk=temp_chunk_i;
						pre_chunk_index=k;
						pre_k=k;

						// if temp_chunk_i == temp_chunk_j, we do not need to perform the search for temp_chunk_i
						break;
						
						}

					}
				}
			}
	 	}

/*
	printf("relevant_chunks:\n");
	for(i=0;i<cur_rcd_idx;i++){

		if(mark_if_relevant[i]>=1)
			printf("%d ",trace_access_pattern[i]);

		}


	printf("\n");
*/

	fclose(fp);
	
}



void calculate_relevance_degree(char *trace_name, int *mark_relevant_access_table, int *caso_relevant_set, int *caso_relevant_degree, 
	int *total_access, int *total_access_index, int *total_access_caso_rele_index, int *num_chunk_per_timestamp, int *relevant_chunks_table, int caso_num_rele_chunks, 
	int begin_timestamp_num){

     int i, j; 
	 int k,h;
	 int p,q;
	 int flag;
	 int temp_chunk_k, temp_chunk_h;
	 int index_k, index_h;
	 int flag1, flag2;
	 int temp_flag;
	 int temp_chunk;
	 int pos_index_h_in_drs_for_k, pos_index_k_in_drs_for_h;

	 for(i=0;i<begin_timestamp_num*max_access_chunks_per_timestamp;i++)
	 	total_access_caso_rele_index[i]=-1; 

	 // preprocess the total_access_caso_rele_index to build the mapping from total_access to rele_chunk_index
	 for(i=0; i<begin_timestamp_num; i++){

		for(j=0; j<max_access_chunks_per_timestamp; j++){

			temp_chunk=total_access[i*max_access_chunks_per_timestamp+j];

			if(temp_chunk==-1)
				break;

			for(k=0; k<caso_num_rele_chunks; k++){

				if(relevant_chunks_table[k]==temp_chunk){
					total_access_caso_rele_index[i*max_access_chunks_per_timestamp+j]=k;
				    break;
					}

				}
			}
	 	}

	 printf("caso_num_rele_chunks=%d, begin_timestamp_num=%d\n",caso_num_rele_chunks, begin_timestamp_num);

  	 for(i=0;i<begin_timestamp_num-1;i++){

        //if(i%100==0) 
			//printf("i=%d, num_timestamp=%d\n", i, begin_timestamp_num);

        // if there is only one chunk accessed in this timestamp, then pass it
		if(num_chunk_per_timestamp[i]==1)
			continue;


        // find the relevant chunks marked at this timestamp
		for(k=0;k<num_chunk_per_timestamp[i]-1;k++){

			//printf("k=%d, num_chunk_per_timestamp[i]=%d\n",k, num_chunk_per_timestamp[i]);

			if(mark_relevant_access_table[i*max_access_chunks_per_timestamp+k]==1){

				temp_chunk_k=total_access[i*max_access_chunks_per_timestamp+k];

				for(h=k+1;h<num_chunk_per_timestamp[i];h++){

					if(mark_relevant_access_table[i*max_access_chunks_per_timestamp+h]==1){
						
					  temp_chunk_h=total_access[i*max_access_chunks_per_timestamp+h];

					  // if two chunks are the same, then pass them
					  if(temp_chunk_k==temp_chunk_h)
					  	continue;

					  index_k=total_access_caso_rele_index[i*max_access_chunks_per_timestamp+k];
					  index_h=total_access_caso_rele_index[i*max_access_chunks_per_timestamp+h];

					  
					  //printf("-----1 index_k=%d, index_h=%d, temp_chunk_k=%d, temp_chunk_h=%d\n", index_k, index_h, temp_chunk_k, temp_chunk_h)

                      // if the relevance of these two chunks has been recorded, then continue; 

					  for(q=0;q<max_num_relevent_chunks_per_chunk;q++){

						if(caso_relevant_set[index_k*max_num_relevent_chunks_per_chunk+q]==index_h) 
							break;

						if(caso_relevant_set[index_k*max_num_relevent_chunks_per_chunk+q]==-1)
							break;
					  	}

					  if(caso_relevant_set[index_k*max_num_relevent_chunks_per_chunk+q]==index_h)
					  	continue;


					  // record the position of index_h in caso_relevant_set
					  pos_index_h_in_drs_for_k=q;

					  for(q=0;q<max_num_relevent_chunks_per_chunk;q++){

						if(caso_relevant_set[index_h*max_num_relevent_chunks_per_chunk+q]==-1)
							break;
					  	}

					  // record the position of index_h in caso_relevant_set
					  pos_index_k_in_drs_for_h=q;			  
					
                      /* scan another timestamp to see if it has these two chunks */
					  
					  for(j=i+1;j<begin_timestamp_num;j++){


						if(num_chunk_per_timestamp[j]==1)
							continue;

                        flag1=0;
						flag2=0;

						for(p=0;p<num_chunk_per_timestamp[j];p++){

							if(total_access[j*max_access_chunks_per_timestamp+p]==temp_chunk_k)
								flag1++;

							if(total_access[j*max_access_chunks_per_timestamp+p]==temp_chunk_h)
								flag2++;

                            // if we find these two chunks, then we do not need to scan the remaining chunks 
							if(flag>=1 && flag2>=1)
								break;

							}

                        // if both of the two chunks appear at the j-th timestamp
                        // increase the relevance degree
						if(flag1>=1 && flag2>=1){

							// the two relevant chunks are index_h and index_k

							// find if index_h is recorded as the relevant chunk of index_k in the relevant_set

							temp_flag=0;

							//printf("index_k=%d, index_h=%d, temp_chunk_k=%d, temp_chunk_h=%d\n", index_k, index_h, temp_chunk_k, temp_chunk_h);

							if(caso_relevant_set[index_k*max_num_relevent_chunks_per_chunk+pos_index_h_in_drs_for_k]==-1){

								caso_relevant_set[index_k*max_num_relevent_chunks_per_chunk+pos_index_h_in_drs_for_k]=index_h;
								caso_relevant_set[index_h*max_num_relevent_chunks_per_chunk+pos_index_k_in_drs_for_h]=index_k;


								}
								
						    caso_relevant_degree[index_k*max_num_relevent_chunks_per_chunk+pos_index_h_in_drs_for_k]++;
							caso_relevant_degree[index_h*max_num_relevent_chunks_per_chunk+pos_index_k_in_drs_for_h]++;

							flag=1;
							
							}
							
					  	}
					  	}
						}

					}
				}

			}
	 
}


// this function is to calculate the priority of the relevant chunks in stripe organization 
int calculate_priority(int *stripe_map, int cur_chunk_num, int *temp_stripe_idx_in_rele_chunk_table, int *caso_relevant_set, int *caso_relevant_degree,  
                               int candidate_chunk_index, int *relevant_chunks_table, int caso_num_rele_chunks){

   int i,j;
   int count;

   int k;

   count=0;

   for(i=0;i<cur_chunk_num;i++){

	j=temp_stripe_idx_in_rele_chunk_table[i];

	// accumulate the priority
	for(k=0; k<max_num_relevent_chunks_per_chunk; k++){

		if(caso_relevant_set[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k]==j)
			break;

		if(caso_relevant_set[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k]==-1)
			break;

		}

	if(caso_relevant_set[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k]==j)
		count+=caso_relevant_degree[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k];

   	}


   return count;

}

void quick_sort_value(int *data, int left, int right){
			int temp = data[left];
			int p = left;
			int i = left, j = right;
	 
			while (i <= j)
			{
					while (data[j] >= temp && j >= p)
							j--;
					if(j >= p)
					{
							data[p] = data[j];
							p = j;
					}
	 
					while (data[i] <= temp && i <= p)
							i++;
					if (i <= p)
					{
							data[p] = data[i];
							p = i;
					}
			}
	
			data[p] = temp;
			
			////printf{""}
			//for(i=left;i<=right;i++) //printf("%d ",index[i]);
			////printf("\n");
	
			if(p - left > 1)
					quick_sort_value(data,left, p-1);
			if(right - p > 1)
					quick_sort_value(data,p+1, right);
	}





// it sorts the unrelevant chunks according to their access frequencies
// the function is to sort the array and move the corresponding index to the new position;
void QuickSort_index(int *data, int index[],int left, int right){
        int temp = data[left];
        int p = left;
	    int temp_value=index[left];
        int i = left, j = right;
 
        while (i <= j)
        {
                while (data[j] <= temp && j >= p)
                        j--;
                if(j >= p)
                {
                        data[p] = data[j];
						index[p]=index[j];
                        p = j;
                }
 
                while (data[i] >= temp && i <= p)
                        i++;
                if (i <= p)
                {
                        data[p] = data[i];
						index[p]=index[i];
                        p = i;
                }
        }

        data[p] = temp;
		index[p]=temp_value;
		
		////printf{""}
		//for(i=left;i<=right;i++) //printf("%d ",index[i]);
	    ////printf("\n");

        if(p - left > 1)
                QuickSort_index(data, index,left, p-1);
        if(right - p > 1)
                QuickSort_index(data, index,p+1, right);
}


// partial stripe writes 
int calculate_psw_io_striping_stripe(char *trace_name, char given_timestamp[]){

    //printf("=============> calculate_psw_io_striping_stripe:\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }


     char operation[100];
	 char *timestamp;
	 char *op_type;
	 char *offset;
	 char *size;
	 char pre_timestamp[100];

	 int i,j;
	 int access_start_block, access_end_block;
	 int cur_index;
	 int count;
	 int temp_count;
	 int flag;
	 int io_count;
	 int start_stripe, end_stripe;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;
	 flag=0;
	 io_count=0;

	 memset(pre_timestamp, '0', sizeof(char)*100);

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe
	 
	 int *stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);

	 int stripe_count;

	 stripe_count=0;
	 
     while(fgets(operation, sizeof(operation), fp)) {

		////printf("count=%d\n",count);

		timestamp=strtok(operation,"\t");
		op_type=strtok(NULL,"\t");
		offset=strtok(NULL,"\t");
		size=strtok(NULL,"\t");

        // if it has not reached the given_timestamp then continue
		if(strcmp(timestamp,given_timestamp)!=0 && flag==0)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Write")!=0) 
			continue;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;		

		// determine the stripes they belong 
		start_stripe=access_start_block/(erasure_k);
		end_stripe=access_end_block/(erasure_k);

		//printf("offset=%lld, write_size=%d\n", *offset_int, *size_int);
		//printf("access_start_block=%d, start_stripe=%d\n", access_start_block, start_stripe);
		//printf("access_end_block=%d, end_stripe=%d\n", access_end_block, end_stripe);

		// calculate the numeber of timestamp
		// if it is a new timestamp, then record the operated stripes
		if(strcmp(pre_timestamp,timestamp)!=0){

		   // calculate the partial stripe writes in the last timestamp
		   io_count+=stripe_count*erasure_m;

           // summarize the access in the last timestamp
		   //if(count>=1)
		      //printf("timestamp=%s, stripe_count=%d\n\n",pre_timestamp,stripe_count);

		   // list the new access in current timestamp
		   //printf("timestamp=%s, start_stripe=%d, end_stripe=%d\n", timestamp, start_stripe, end_stripe);

		   // re-initiate the stripes_per_timestamp
		   for(i=0; i<max_accessed_stripes; i++)
		   	stripes_per_timestamp[i]=-1;

           // record the current involved stripes in this operation
		   stripe_count=end_stripe-start_stripe+1;

		   for(i=start_stripe;i<=end_stripe;i++)
		   	stripes_per_timestamp[i-start_stripe]=i;

		   strcpy(pre_timestamp,timestamp);

		   count++;

			}

        // if it belongs to the same timestamp, then record the involved stripes
		else {

			for(i=start_stripe;i<=end_stripe;i++){

				//printf("stripe_count=%d, cur_stripe_id=%d\n",stripe_count, i);

				for(j=0;j<stripe_count;j++)
					if(stripes_per_timestamp[j]==i)
						break;

				if(j>=stripe_count){

					stripes_per_timestamp[stripe_count]=i;
					stripe_count++;

					}
				}

			//printf("timestamp=%s, start_stripe=%d, end_stripe=%d\n", timestamp, start_stripe, end_stripe);

			//for(k=0;k<stripe_count;k++)
				//printf("%d ",stripes_per_timestamp[k]);
			//printf("\n");


			}
     	}

	 //printf("timestamp=%s, stripe_count=%d\n\n",pre_timestamp,stripe_count);

     // for the last operation
	 io_count+=stripe_count*erasure_m;

	 fclose(fp);
	 free(stripes_per_timestamp);

	 //printf("<============= calculate_psw_io_striping_stripe:\n");

	 return io_count;
	 
}

int calculate_psw_io_caso_stripe(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map){

	printf("=============> calculate_psw_io_caso_stripe:\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }


     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';
	 
	 char pre_timestamp[100];

	 int i,j;
	 int access_start_block, access_end_block;
	 int cur_index;
	 int count;
	 int temp_count;
	 int flag;
	 int io_count;
	 int sort_index, real_index;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;


	 int temp_stripe_id;
	 int chunk_count;

	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // suppose the maximum accessed stripes is 100
	 int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);
	 int stripe_count;

	 stripe_count=0;
	 
     while(fgets(operation, sizeof(operation), fp)) {

		count++;

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);
		

        // if it has not reached the given_timestamp then continue
		if(strcmp(timestamp,given_timestamp)!=0 && flag==0)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Write")!=0) 
			continue;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;		


        // if a new timestamp comes
		if(strcmp(pre_timestamp,timestamp)!=0){

		   // calculate the partial stripe writes in the last timestamp
		   io_count+=stripe_count*erasure_m;

		   //printf("timestamp=%s, stripe_count=%d\n",pre_timestamp,stripe_count);

           // re-initiate the stripe_count
		   stripe_count=0;

		   chunk_count=0;

		   strcpy(pre_timestamp,timestamp); 

		   //printf("parity_update_io=%d\n",io_count);

		   //printf("cur_timestamp=%s\n", timestamp);

			}

		// determine the stripes they belong 
		// we compare each chunk in chunk_to_stripe_map 

		//printf("access_start_block=%d, access_end_block=%d\n",access_start_block,access_end_block);
		
		for(i=access_start_block; i<=access_end_block; i++){

			sort_index=binary_search(sort_trace_pattern, cur_rcd_idx, i);
			//printf("i=%d, sort_index=%d\n", i, sort_index);
			real_index=sort_pattern_index[sort_index];
			temp_stripe_id=chunk_to_stripe_map[real_index];
		
			for(j=0;j<stripe_count;j++){
		
				if(stripes_per_timestamp[j]==temp_stripe_id)
					break;
		
				}
		
			if(j>=stripe_count){
				stripes_per_timestamp[stripe_count++]=temp_stripe_id;
				//printf("----stripe_count=%d\n",stripe_count);
				}

			//printf("access_block_id=%d, stripe_id=%d\n", i, temp_stripe_id);
		
		
			}

     	}

	 //for the last operation, add the parity update io

     io_count+=stripe_count*erasure_m; 

	 printf("<============= calculate_psw_io_caso_stripe:\n");


	 fclose(fp);
	 free(stripes_per_timestamp);
	 return io_count;

}


// it calculates the caused io in partial stripe writes under contiguous data placement
int calculate_psw_io_continugous_stripe(char *trace_name, char given_timestamp[], int max_access_block){

	//printf("=============> calculate_psw_io_continugous_stripe:\n");


    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }

	int group_num_blocks=contiguous_block*erasure_k;


     char operation[100];
	 char *timestamp;
	 char *op_type;
	 char *offset;
	 char *size;
	 char pre_timestamp[100];

	 int i,j;
	 int access_start_block, access_end_block;
	 int cur_index;
	 int count;
	 int temp_count;
	 int flag;
	 int temp_stripe, num_stripes;
	 int io_count;
	 int temp_group_id, temp_group_block_id;
	 int temp_row_id;
	 int temp=contiguous_block;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;

	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe
	 
	 int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);

	 int stripe_count;
	 stripe_count=0;

	 num_stripes=max_access_block/erasure_k; // num_stripes is the maximum number of data chunks stored in a column

     while(fgets(operation, sizeof(operation), fp)) {

		count++;

		////printf("count=%d\n",count);

		timestamp=strtok(operation,"\t");
		op_type=strtok(NULL,"\t");
		offset=strtok(NULL,"\t");
		size=strtok(NULL,"\t");

        // if it has not reached the given_timestamp then continue
		if(strcmp(timestamp,given_timestamp)!=0 && flag==0)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Write")!=0) 
			continue;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;	

		//printf("access_start_block=%d, access_end_block=%d\n", access_start_block, access_end_block);

		// calculate the numeber of timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

		   // calculate the partial stripe writes in the last timestamp
		   io_count+=stripe_count*erasure_m;

		   //printf("timestamp=%s, stripe_count=%d\n",pre_timestamp,stripe_count);

           // record the current involved stripes in this operation

		   stripe_count=0;
		   
		   for(i=access_start_block; i<=access_end_block; i++){

			// locate the corresponding group and block_id in that group
			temp_group_id=i/group_num_blocks;
			temp_group_block_id=i%group_num_blocks;
			
			// locate the column in that group
			temp_row_id=temp_group_block_id%temp;

			//printf("temp_group_block_id=%d, contiguous_block=%d, temp_row_id=%d\n", temp_group_block_id, contiguous_block, temp_row_id);
			
			// locate the stripe_id 
			temp_stripe=temp_group_id*contiguous_block+temp_row_id; 

			//printf("access_block_id=%d, temp_group_id=%d, temp_row_id=%d, temp_stripe=%d\n", i, temp_group_id, temp_row_id, temp_stripe);

			for(j=0;j<stripe_count;j++)
				if(stripes_per_timestamp[j]==temp_stripe)
					break;

			if(j>=stripe_count){

				stripes_per_timestamp[stripe_count]=temp_stripe;
				stripe_count++;

				}
		   	}


		   strcpy(pre_timestamp,timestamp);

			}

        // if it belongs to the same timestamp, then record the involved stripes
		else {

			for(i=access_start_block; i<=access_end_block; i++){

                // locate the corresponding group and block_id in that group
				temp_group_id=i/group_num_blocks;
				temp_group_block_id=i%group_num_blocks;

				// locate the column in that group
				temp_row_id=temp_group_block_id%contiguous_block;

				// locate the stripe_id 
				temp_stripe=temp_group_id*contiguous_block+temp_row_id; 

				for(j=0; j<stripe_count; j++)
					if(stripes_per_timestamp[j]==temp_stripe)
						break;


				if(j>=stripe_count){

					stripes_per_timestamp[stripe_count]=temp_stripe;
					stripe_count++;

					}
				}

            //printf("timestamp=%s, start_stripe=%d, end_stripe=%d\n", timestamp, start_stripe, end_stripe);
			//for(k=0;k<stripe_count;k++)
				//printf("%d ",stripes_per_timestamp[k]);
			//printf("\n");


			}
     	}

	 // for the last operation
	 io_count+=stripe_count*erasure_m;


	 fclose(fp);
	 free(stripes_per_timestamp);

	 //printf("<============= calculate_psw_io_continugous_stripe:\n");
	 

	 return io_count;
	 
}


// calculate the ones in the matrix
int calculate_chunk_num_io_matrix(int *io_matrix, int len, int width){

  int res=0;
  int i;

  
  for(i=0;i<len*width;i++)
  	if(io_matrix[i]==1)
		res++;


  return res;
  
}


// this function takes the accessed stripes as input and outputs an array that records the stripe organization
void stripe_organization(int *relevant_chunks_table, int *caso_relevant_set, int *caso_relevant_degree,  
               int *chunk_to_stripe_map, int *chunk_to_stripe_chunk_map, int num_stripe, int caso_num_rele_chunks, int *caso_rele_chunk_index){

   printf("===========> stripe_organization:\n");

   int i,j;

   int first_chunk_index, second_chunk_index;
   int temp_max;

   int* mark_if_select_relevant_chunks=(int*)malloc(sizeof(int)*caso_num_rele_chunks); // it marks if a relevant chunk has been organized into stripes, it records the i-th relevant chunks
   int* unrelevant_chunks_index=(int*)malloc(sizeof(int)*(caso_rcd_idx-caso_num_rele_chunks)); // it records the index of unrelevant chunks
   int* unrelevant_chunks_access_freq=(int*)malloc(sizeof(int)*(caso_rcd_idx-caso_num_rele_chunks)); // it records the access frequency of unrelevant chunks
   int* temp_stripe=(int*)malloc(sizeof(int)*erasure_k); // it records the chunks in a temp stripe
   int* temp_stripe_idx_to_rele_chunk_table=(int*)malloc(sizeof(int)*erasure_k);
 
   // initiate the unrelevant chunks and their indices
   int count=0;
   
   for(i=0;i<caso_rcd_idx;i++){

	if(mark_if_relevant[i]==0){

		unrelevant_chunks_index[count]=i;
		unrelevant_chunks_access_freq[count]=freq_access_chunk[i];
		count++;
		
		}
   	}

   int stripe_count;
   int temp_count;
   int priority;
   int select_chunk_index;
   int flag;
   int rele_chunk_stripe;
   int candidate_chunk;

   count=0;
   stripe_count=0;

   // we first organize the relevant chunks 

   //printf("caso_num_rele_chunks=%d\n",caso_num_rele_chunks);
   rele_chunk_stripe=(caso_num_rele_chunks-1)/erasure_k;


   for(i=0;i<caso_num_rele_chunks;i++)
   	mark_if_select_relevant_chunks[i]=0;

   // we need to arrange the chunk_to_stripe_map in ascending order
   for(i=0;i<cur_rcd_idx;i++)
   	chunk_to_stripe_map[i]=-1;

   for(stripe_count=0; stripe_count<=rele_chunk_stripe; stripe_count++){

	  //if(stripe_count%100==0)
		//printf("stripe_count=%d, rele_chunk_stripe=%d\n", stripe_count, rele_chunk_stripe);

      // initialize temp_stripe_idx_to_rele_chunk_table
	  for(i=0; i<erasure_k; i++)
	  	temp_stripe_idx_to_rele_chunk_table[i]=-1;

	  temp_count=0;
	  flag=0;

	  // find out the first two most relevant data chunks 
	  temp_max=-999;
	
	  for(i=0;i<caso_num_rele_chunks;i++){
	
		  // if this chunk has been selected in a stripe, then pass it
		  if(mark_if_select_relevant_chunks[i]==1) 
			continue;


		  for(j=0;j<max_num_relevent_chunks_per_chunk;j++){
 
			if(caso_relevant_set[i*max_num_relevent_chunks_per_chunk+j]==-1) 
				break;

			candidate_chunk=caso_relevant_set[i*max_num_relevent_chunks_per_chunk+j];

			if(mark_if_select_relevant_chunks[candidate_chunk]==1)
				continue;

			if(caso_relevant_degree[i*max_num_relevent_chunks_per_chunk+j]>temp_max){

				first_chunk_index=i;
				second_chunk_index=candidate_chunk;
				temp_max=caso_relevant_degree[i*max_num_relevent_chunks_per_chunk+j];
				flag=1;

				}
			}

	 }

    // if there is no two direcly correlated data chunks, then random select two data chunks
	if(flag==0){

		for(i=0;i<caso_num_rele_chunks;i++)
			if(mark_if_select_relevant_chunks[i]==0){

				first_chunk_index=i;
				break;
				
				}

		for(i=first_chunk_index+1;i<caso_num_rele_chunks;i++)
			if(mark_if_select_relevant_chunks[i]==0){

				second_chunk_index=i;
				break;
				
				}
		}


	// mark the cell to indicate that these two chunks have been selected 
	mark_if_select_relevant_chunks[first_chunk_index]=1;
	mark_if_select_relevant_chunks[second_chunk_index]=1;


	/* insert the two chunks into the stripe */
	// for the first chunk
	chunk_to_stripe_map[caso_rele_chunk_index[first_chunk_index]]=stripe_count;
	chunk_to_stripe_chunk_map[caso_rele_chunk_index[first_chunk_index]]=0;

	temp_stripe[temp_count]=trace_access_pattern[caso_rele_chunk_index[first_chunk_index]];
	temp_stripe_idx_to_rele_chunk_table[temp_count]=first_chunk_index;
	temp_count++;


    // for the second chunk
	chunk_to_stripe_map[caso_rele_chunk_index[second_chunk_index]]=stripe_count;
	chunk_to_stripe_chunk_map[caso_rele_chunk_index[second_chunk_index]]=1;
	
	temp_stripe[temp_count]=trace_access_pattern[caso_rele_chunk_index[second_chunk_index]];
	temp_stripe_idx_to_rele_chunk_table[temp_count]=second_chunk_index;
	temp_count++;

/*
	printf("++++++ the %d-th stripe:++++++++\n", stripe_count);
	printf("first_chunk_index=%d, second_chunk_index=%d\n", first_chunk_index, second_chunk_index);
	printf("the first chunk=%d, the second chunk=%d, priority=%d\n", 
		trace_access_pattern[caso_rele_chunk_index[first_chunk_index]], 
		trace_access_pattern[caso_rele_chunk_index[second_chunk_index]],
		temp_max);
*/

	// after that, we perform greedy selection

	for(temp_count=2; temp_count<erasure_k; temp_count++){

	  flag=0; 
	  temp_max=-1;
	  
	  for(i=0;i<caso_num_rele_chunks;i++){

		if(mark_if_select_relevant_chunks[i]==1) 
				continue;

		// calculate the priority 
		priority=calculate_priority(temp_stripe, temp_count, temp_stripe_idx_to_rele_chunk_table, caso_relevant_set, caso_relevant_degree, i, relevant_chunks_table, caso_num_rele_chunks);

	    //printf("priority=%d\n",priority);

		if(priority>temp_max){

			temp_max=priority;
			select_chunk_index=i;
			flag=1;

				}
		 }	

	  // if there still exists relevant chunks, then organize the chunks that owns largest priority
	  if(flag==1){
	
		chunk_to_stripe_map[caso_rele_chunk_index[select_chunk_index]]=stripe_count;
		chunk_to_stripe_chunk_map[caso_rele_chunk_index[select_chunk_index]]=temp_count;

		
		temp_stripe[temp_count]=trace_access_pattern[caso_rele_chunk_index[select_chunk_index]];
		temp_stripe_idx_to_rele_chunk_table[temp_count]=select_chunk_index;
		
		mark_if_select_relevant_chunks[select_chunk_index]=1;


		//printf("select_chunk=%d, correlation_degree=%d, stripe_count=%d\n",trace_access_pattern[caso_rele_chunk_index[select_chunk_index]], 
			// temp_max, stripe_count);


		}
   	}

   	}

/*
   printf("mark_if_select_relevant_chunks:\n");
   for(i=0; i<caso_num_rele_chunks;i++)
   	 printf("%d ",mark_if_select_relevant_chunks[i]);
   printf("\n");
*/  
   /* organize the remaining chunks after being erasure coded into stripes */
   int *temp_access_chunks=(int*)malloc(sizeof(int)*caso_num_rele_chunks);

   // store the correlated chunks
   for(i=0;i<caso_num_rele_chunks;i++)
   	temp_access_chunks[i]=trace_access_pattern[caso_rele_chunk_index[i]];

   // sort the accessed chunks from small to large
   quick_sort_value(temp_access_chunks, 0, caso_num_rele_chunks-1);

   int temp_chunk;
   int temp_stripe_id, temp_stripe_chunk_id;

   for(i=0; i<cur_rcd_idx; i++){

	 temp_chunk=trace_access_pattern[i];

	 //printf("temp_chunk=%d\n",temp_chunk);

	 // locate the range of this chunk
	 for(j=0;j<caso_num_rele_chunks;j++){

		if(temp_access_chunks[j]>=temp_chunk)
			break;

	 	}

	 // if this chunk appears in the previous accesses
	 if(temp_access_chunks[j]==temp_chunk)
	 	continue;

	 // then there are j-1 chunks whose offsets are smaller than temp_chunk
	 // note that the stripe_id should be larger than rele_chunk_stripe+extra_stripe_count;
	 temp_stripe_id=(temp_chunk-j)/(erasure_k)+rele_chunk_stripe+1;
	 temp_stripe_chunk_id=(temp_chunk-j)%erasure_k;

	 //printf("num_organized_chunks=%d, temp_stripe_id=%d\n",j,temp_stripe_id);

	 // record this stripe_id
	 chunk_to_stripe_map[i]=temp_stripe_id;
	 chunk_to_stripe_chunk_map[i]=temp_stripe_chunk_id;

   	}

/*
   printf("++++++caso_rele_chunk_num=%d:\n", caso_num_rele_chunks);
   for(i=0;i<caso_num_rele_chunks;i++)
   	printf("%d ",trace_access_pattern[caso_rele_chunk_index[i]]);
   printf("\n");


   printf("\n++++++++chunk_to_stripe_map:\n");
   for(i=0;i<cur_rcd_idx;i++)
   	printf("chunk_id=%d, stripe_id=%d, stripe_chunk_id=%d\n",trace_access_pattern[i], chunk_to_stripe_map[i], chunk_to_stripe_chunk_map[i]);
   printf("\n");
*/

  
   free(mark_if_select_relevant_chunks);
   free(unrelevant_chunks_access_freq);
   free(unrelevant_chunks_index);
   free(temp_stripe);
   //free(sort_access_chunks);
   free(temp_stripe_idx_to_rele_chunk_table);

   //printf("caso_rcd_idx=%d\n", caso_rcd_idx);

   printf("<=========== stripe_organization:\n");
   
}

// @accessed_stripes: records the involved stripes in a timestamp
// @stripe_count: records the number of involved stripes in a timestamp
// @io_matrix: if a chunk is accessed, then the corresponding cell is marked as 1
void system_partial_stripe_writes(int *io_matrix, int *accessed_stripes, int stripe_count, int *total_write_block_num){

  int i,j;
  int io_amount;

  int io_index;


  int ret;
  int matrix_width;
  long long int init_offset;

  int pagesize;
  int fd_disk[15];

  pagesize=getpagesize();
  matrix_width=erasure_k+erasure_m;
  
  //count the io amount
  io_amount=calculate_chunk_num_io_matrix(io_matrix, stripe_count, erasure_k+erasure_m);

  *total_write_block_num+=io_amount;

  //record the name of disk array
  char *disk_array[15]={"/dev/sde","/dev/sdf","/dev/sdg","/dev/sdh","/dev/sdi","/dev/sdj","/dev/sdk","/dev/sdl","/dev/sdm","/dev/sdn",
  "/dev/sdo","/dev/sdp","/dev/sdq","/dev/sdr","/dev/sds"};

  /* perform the read operations */
  
  //record the start point and end point for each i/o
  struct aiocb *aio_list = (struct aiocb *)malloc(sizeof(struct aiocb)*io_amount);

  bzero(aio_list, sizeof (struct aiocb)*io_amount);

  io_index=0;
  srand((int)time(0)); 
  
  for(i=0;i<matrix_width;i++){

  	fd_disk[i]=open(disk_array[i], O_RDWR | O_DIRECT | O_SYNC);
	
	if(fd_disk[i]<0){
		printf("openfile failed, i=%d\n",i);
		exit(1);
		}

	for(j=0; j<stripe_count; j++){

		// if there is an io request, then initiate the aio_structure
		if(io_matrix[j*matrix_width+i]==1){

			aio_list[io_index].aio_fildes=fd_disk[i];
			aio_list[io_index].aio_nbytes=block_size;
			// make sure that the offset should not exceed the disk capacity. 
			aio_list[io_index].aio_offset=(off_t)((accessed_stripes[j])%disk_capacity)*block_size;
			aio_list[io_index].aio_reqprio=0;
			ret = posix_memalign((void**)&aio_list[io_index].aio_buf, pagesize, block_size);
			io_index++;
		}
  	}
  	}

  // perform the read operations
  for(i=0;i<io_index;i++){

		ret=aio_read(&aio_list[i]);
		if(ret<0)
		  perror("aio_read");
		
  	}
  

  for(i=0;i<io_index;i++){

  	while(aio_error(&aio_list[i]) == EINPROGRESS);
	if((ret = aio_return(&aio_list[i]))==-1){ 
		printf("io_index=%d, io_amount=%d\n", io_index, io_amount);
		printf("%d-th aio_read_offset=%.2lfMB\n", i, aio_list[i].aio_offset*1.0/1024/1024);
		printf("aio_read_return error, ret=%d, io_no=%d\n",ret,i);
		}

  	}

/*
  int *encoding_matrix;
  char **data, **coding;
  
  encoding_matrix = talloc(int, erasure_m*erasure_k);
  for (i = 0; i < erasure_m; i++) {
    for (j = 0; j < erasure_k; j++) {
      encoding_matrix[i*erasure_k+j] = galois_single_divide(1, i ^ (erasure_m + j), erasure_w);
    }
  }

  jerasure_matrix_encode(erasure_k, erasure_m, erasure_w, encoding_matrix, data, coding, block_size);
*/

  /* perform the write operations */
  
  for(i=0;i<io_index;i++){

		ret=aio_write(&aio_list[i]);
		if(ret<0){
		  perror("aio_write");
		  exit(1);
			}
  	}

  for(i=0;i<io_index;i++){

  	while(aio_error(&aio_list[i]) == EINPROGRESS);
	if((ret = aio_return(&aio_list[i]))==-1)
		printf("aio_write_return error, ret=%d, io_no=%d\n",ret,i);

  	}

/* free the aio structure */
 free(aio_list);

 for(i=0;i<matrix_width;i++)
   close(fd_disk[i]);


}


int psw_time_caso(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map, 
	int *chunk_to_stripe_chunk_map, double *time){

	printf("=============> psw_time_caso:\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }


     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';

	 int i,j;
	 int k;

	 
	 char pre_timestamp[100];

	 

	 int access_start_block, access_end_block;
	 int cur_index;

	 int count;
	 int temp_count;
	 int flag;

	 int io_count;



	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;


	 int temp_stripe_id, temp_chunk_id;
	 int chunk_count;

	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // suppose the maximum accessed stripes is 100

	 int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);
	 int *io_request=malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp

     // initialize the io_request
     // if a chunk is requested in the s_i-th stripe, then the corresponding cell is marked with 1
	 for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
	 	io_request[i]=0;

	 int stripe_count;
	 int write_count;
	 int rotation;
	 int sort_index, real_index;
	 int max_stripe_count=-1;

	 int *total_caso_io;
	 int c=0;
	 total_caso_io=&c;

	 stripe_count=0;
	 write_count=0;

	 struct timeval begin_time, end_time;

	 printf("cur_rcd_idx=%d\n", cur_rcd_idx);
     while(fgets(operation, sizeof(operation), fp)) {

		count++;

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);
		
        // if it has not reached the given_timestamp then continue
		if(strcmp(timestamp,given_timestamp)!=0 && flag==0)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Write")!=0) 
			continue;

		write_count++;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;		


        // if a new timestamp comes
		if(strcmp(pre_timestamp,timestamp)!=0){

		   // calculate the partial stripe writes in the last timestamp
		   io_count+=stripe_count*erasure_m;

		   // perform system write
		   gettimeofday(&begin_time, NULL);
		   system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
		   gettimeofday(&end_time, NULL);
		   *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

		   // re-initialize the io_request array
		   for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
		   	io_request[i]=0;

		   if(stripe_count>max_accessed_stripes)
		   	max_stripe_count=stripe_count;

           // re-initiate the stripe_count
		   stripe_count=0;
		   chunk_count=0;

		   strcpy(pre_timestamp,timestamp); 

		   //printf("parity_update_io=%d\n",io_count);

		   //printf("cur_timestamp=%s\n", timestamp);

			}

		// determine the stripes they belong 
		// we compare each chunk in chunk_to_stripe_map 

		//printf("access_start_block=%d, access_end_block=%d\n",access_start_block,access_end_block);
		
		for(i=access_start_block; i<=access_end_block; i++){

			sort_index=binary_search(sort_trace_pattern, cur_rcd_idx, i);
			real_index=sort_pattern_index[sort_index];
			temp_stripe_id=chunk_to_stripe_map[real_index];
			rotation=temp_stripe_id%(erasure_k+erasure_m);
			temp_chunk_id=chunk_to_stripe_chunk_map[real_index];
		
			for(j=0; j<stripe_count; j++){
		
				if(stripes_per_timestamp[j]==temp_stripe_id)
					break;
		
				}
		
			if(j>=stripe_count){
				stripes_per_timestamp[stripe_count]=temp_stripe_id;
				stripe_count++;
				//printf("----stripe_count=%d\n",stripe_count);
				}

			// update the io_request_matrix
			io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=1;

			for(k=erasure_k; k< (erasure_k+erasure_m); k++)
				io_request[j*(erasure_k+erasure_m)+(k+rotation)%(erasure_k+erasure_m)]=1;

			//printf("temp_chunk_id=%d, temp_stripe_id=%d\n", temp_chunk_id, temp_stripe_id);
		
		
			}

     	}

	 //for the last operation, add the parity update io
	 gettimeofday(&begin_time, NULL);
	 system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
	 gettimeofday(&end_time, NULL);
	 *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

	 write_count++;

     io_count+=stripe_count*erasure_m; 

	 printf("write_count=%d\n", write_count);

	 printf("caso_parity_io=%d, total_io_num=%d\n", io_count, *total_caso_io);
	 printf("max_stripe_count=%d, max_access_chunk_per_timestamp=%d\n", max_stripe_count,max_accessed_stripes);


	 fclose(fp);
	 free(stripes_per_timestamp);
	 free(io_request);

	 printf("<============= psw_time_caso:\n");

	 return io_count;

}




// partial stripe writes 
int psw_time_striping(char *trace_name, char given_timestamp[], double *time){

    printf("=============> psw_time_striping:\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }


     char operation[100];
	 char *timestamp;
	 char *op_type;
	 char *offset;
	 char *size;


	 int i,j;
	 int k;

	 
	 char pre_timestamp[100];
	 
	 

	 int access_start_block, access_end_block;
	 int cur_index;
	 int count;
	 int temp_count;
	 int flag;
	 int io_count;
	 int start_stripe, end_stripe;
	 int start_chunk_stripe, end_chunk_stripe;
	 int temp_stripe_id, temp_chunk_id;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;



	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe
	 int *stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);
	 int *io_request=(int*)malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp

	 int write_count;

     // initialize the io_request
     // if a chunk is requested in the s_i-th stripe, then the corresponding cell is marked with 1
	 for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
	 	io_request[i]=0;

	 int stripe_count;
	 int rotation;
	 int *total_write_block_num; 
	 int c=0;

	 stripe_count=0;
	 total_write_block_num=&c;
	 write_count=0;

	 struct timeval begin_time, end_time;
     while(fgets(operation, sizeof(operation), fp)) {

		////printf("count=%d\n",count);

		timestamp=strtok(operation,"\t");
		op_type=strtok(NULL,"\t");
		offset=strtok(NULL,"\t");
		size=strtok(NULL,"\t");

        // if it has not reached the given_timestamp then continue
		if(strcmp(timestamp,given_timestamp)!=0 && flag==0)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Write")!=0) 
			continue;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;		

		// determine the stripes they belong 
		start_stripe=access_start_block/(erasure_k);
		end_stripe=access_end_block/(erasure_k);

		// determine the chunk id in the stripe 
		start_chunk_stripe=access_start_block%erasure_k;
		end_chunk_stripe=access_end_block%erasure_k;

		// calculate the numeber of timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

		   // calculate the partial stripe writes in the last timestamp
		   io_count+=stripe_count*erasure_m;

		   // perform the system write
		   gettimeofday(&begin_time, NULL);
		   system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
		   gettimeofday(&end_time, NULL);
		   *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

           // summarize the access in the last timestamp
		   //if(count>=1)
		      //printf("timestamp=%s, stripe_count=%d\n\n",pre_timestamp,stripe_count);

		   // list the new access in current timestamp
		   // printf("timestamp=%s, start_stripe=%d, end_stripe=%d\n", timestamp, start_stripe, end_stripe);

		   // re-initiate the stripes_per_timestamp
		   for(i=0;i<stripe_count;i++)
		   	stripes_per_timestamp[i]=-1;

		   // re-initialize the io_request array
		   for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
		   	io_request[i]=0;

           // record the current involved stripes in this operation
		   stripe_count=0;

		   strcpy(pre_timestamp,timestamp);

		   count++;

		   write_count++;

			}

		for(i=access_start_block; i<=access_end_block; i++){

			temp_stripe_id=i/erasure_k;
			rotation=temp_stripe_id%(erasure_k+erasure_m);
			temp_chunk_id=i%erasure_k;
			
			for(j=0; j<stripe_count; j++){
		
				if(stripes_per_timestamp[j]==temp_stripe_id)
					break;
		
				}
		
			if(j>=stripe_count){
				stripes_per_timestamp[stripe_count]=temp_stripe_id;
				//printf("i=%d, temp_stripe_id=%d, temp_chunk_id=%d, stripe_count=%d\n", i, temp_stripe_id, temp_chunk_id, stripe_count);
				stripe_count++;
				//printf("----stripe_count=%d\n",stripe_count);
				}
/*
            printf("stripes_per_timestamp:\n");
			for(k=0; k<stripe_count; k++)
				printf("%d ", stripes_per_timestamp[k]);
			printf("\n");
*/
			// update the io_request_matrix
			io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=1;

			for(k=erasure_k; k< (erasure_k+erasure_m); k++)
				io_request[j*(erasure_k+erasure_m)+(k+rotation)%(erasure_k+erasure_m)]=1;
		
		
			}


     	}

     gettimeofday(&begin_time, NULL);
	 system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
	 gettimeofday(&end_time, NULL);
	 *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

	 write_count++;

	 //printf("num_of_write_op=%d, total_write_block_num=%d\n", write_count, *total_write_block_num);

	 //printf("timestamp=%s, stripe_count=%d\n\n",pre_timestamp,stripe_count);

     // for the last operation
	 io_count+=stripe_count*erasure_m;
/*
	 printf("access_start_block=%d, start_chunk_id=%d, start_stripe=%d, rotation=%d\n", access_start_block, access_start_block%erasure_k, start_stripe, rotation%(erasure_k+erasure_m));
	 printf("access_end_block=%d, end_chunk_id=%d, end_stripe=%d, rotation=%d\n", access_end_block, access_end_block%erasure_k, end_stripe, rotation);

     printf("striping io_request:\n");
	 for(i=0; i<stripe_count; i++){

		for(j=0; j< (erasure_k+erasure_m); j++)
			printf("%d ", io_request[i*(erasure_k+erasure_m)+j]);

		printf("\n");

	 	}
*/
	 fclose(fp);
	 free(stripes_per_timestamp);
	 free(io_request);

	 printf("<============= psw_time_striping:\n");

	 return io_count;
	 
}


// it calculates the caused io in partial stripe writes under contiguous data placement
int psw_time_continugous(char *trace_name, char given_timestamp[], double *time){

	printf("=============> psw_time_continugous:\n");
	
    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }

	int group_num_blocks=contiguous_block*erasure_k;


     char operation[100];
	 char *timestamp;
	 char *op_type;
	 char *offset;
	 char *size;


	 int i,j;
	 int k;

	 
	 char pre_timestamp[100];

	 

	 int access_start_block, access_end_block;
	 int cur_index;

	 int count;
	 int temp_count;
	 int flag;

	 int temp_stripe;

	 int io_count;



	 int temp_group_id, temp_group_block_id;
	 int temp_row_id;


	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;



	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe
	 
	 int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);
	 int *io_request=malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp

	 int stripe_count;
	 int temp_chunk_id;

	 int *total_write_block_num; 
	 int c=0;

	 total_write_block_num=&c;
	 
	 stripe_count=0;

     struct timeval begin_time, end_time;
     while(fgets(operation, sizeof(operation), fp)) {

		count++;

		////printf("count=%d\n",count);

		timestamp=strtok(operation,"\t");
		op_type=strtok(NULL,"\t");
		offset=strtok(NULL,"\t");
		size=strtok(NULL,"\t");

        // if it has not reached the given_timestamp then continue
		if(strcmp(timestamp,given_timestamp)!=0 && flag==0)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Write")!=0) 
			continue;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;	

		//printf("access_start_block=%d, access_end_block=%d\n", access_start_block, access_end_block);

		// calculate the numeber of timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

		   // calculate the partial stripe writes in the last timestamp
		   io_count+=stripe_count*erasure_m;

		   //printf("timestamp=%s, stripe_count=%d\n",pre_timestamp,stripe_count);
		   
           gettimeofday(&begin_time, NULL);
		   system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
		   gettimeofday(&end_time, NULL);
		   *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

           // record the current involved stripes in this operation

		   // re-initiate the stripes_per_timestamp
		   for(i=0;i<stripe_count;i++)
		   	stripes_per_timestamp[i]=-1;

		   // re-initialize the io_request array
		   for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
		   	io_request[i]=0;

           // record the current involved stripes in this operation
		   stripe_count=0;


		   strcpy(pre_timestamp,timestamp);

			}

        // if it belongs to the same timestamp, then record the involved stripes
		for(i=access_start_block; i<=access_end_block; i++){

                // locate the corresponding group and block_id in that group
				temp_group_id=i/group_num_blocks;
				temp_group_block_id=i%group_num_blocks;

				// locate the column in that group
				temp_row_id=temp_group_block_id%contiguous_block;

				// locate the stripe_id 
				temp_stripe=temp_group_id*contiguous_block+temp_row_id; 

				// locate the column_id
				temp_chunk_id=temp_group_id/contiguous_block;

				for(j=0; j<stripe_count; j++)
					if(stripes_per_timestamp[j]==temp_stripe)
						break;


				if(j>=stripe_count){

					stripes_per_timestamp[stripe_count]=temp_stripe;
					stripe_count++;

					}


				// update the io_request_matrix
				io_request[j*(erasure_k+erasure_m)+(temp_chunk_id)%(erasure_k+erasure_m)]=1;
				
				for(k=erasure_k; k< (erasure_k+erasure_m); k++)
					io_request[j*(erasure_k+erasure_m)+(k)%(erasure_k+erasure_m)]=1;
				

				}

            //printf("timestamp=%s, start_stripe=%d, end_stripe=%d\n", timestamp, start_stripe, end_stripe);
			//for(k=0;k<stripe_count;k++)
				//printf("%d ",stripes_per_timestamp[k]);
			//printf("\n");


     	}

	 // for the last operation
	 gettimeofday(&begin_time, NULL);
	 system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
     gettimeofday(&end_time, NULL);
	 *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;	 
	 io_count+=stripe_count*erasure_m;


	 fclose(fp);
	 free(stripes_per_timestamp);
	 free(io_request);

	 printf("<============= psw_time_continugous:\n");
	 

	 return io_count;
	 
}



// given the io_request (the failed element requested is marked as -1, and the requested element that 
// is available is marked as 1). What we do is to perform degraded reads
// @stripe_id_array: it records the stripe id that is involved in the requests
// @stripe_count: the number of stripes that are involved.

int degraded_reads(int *io_request, int *stripe_id_array, int stripe_count, int *num_extra_io, int if_continugous){

   int i, j; 
   int k;
   
   int temp_stripe, temp_rotation;
   int num_cur_req_chunks;
   int num_extra_req_chunks, cur_extra_chunk;
   int io_count;

   int flag;

   flag=0; 
   
   for(i=0; i<stripe_count; i++){

	num_cur_req_chunks=0;

	// calculate the number of elements to be read currently
	for(j=0; j< erasure_k+erasure_m; j++){

		if(io_request[i*(erasure_k+erasure_m)+j]==1)
			num_cur_req_chunks++;
		
		}

    // scan the io_request
	
	for(j=0; j< erasure_k+erasure_m; j++){

        // if a requested chunk is lost
		if(io_request[i*(erasure_k+erasure_m)+j]==-1){

			io_count=0;
			flag=1;

			temp_stripe=stripe_id_array[i];

			if(if_continugous==0)
			    temp_rotation=temp_stripe%(erasure_k+erasure_m);
			else 
				temp_rotation=0;

			// mark the extra chunks to be read
			// calculate the number of elements to be read currently 
			num_extra_req_chunks=erasure_k-num_cur_req_chunks;

            // if it has read no less than k chunks, then continue
			if(num_extra_req_chunks<=0) 
				continue;

			*num_extra_io+=num_extra_req_chunks-1;

			// we all read the parity chunks as the extra chunks 
			for(k=0; k< erasure_k+erasure_m; k++){

				cur_extra_chunk=(k+temp_rotation)%(erasure_k+erasure_m);
				if(io_request[i*(erasure_k+erasure_m)+cur_extra_chunk]==0){
					
					io_request[i*(erasure_k+erasure_m)+cur_extra_chunk]=1;
					io_count++;
					
					}

				if(io_count==num_extra_req_chunks)
					break;

				}

			}
		}
   	}

   return flag;
}



// @accessed_stripes: records the involved stripes in a timestamp
// @stripe_count: records the number of involved stripes in a timestamp
// @io_matrix: if a chunk is accessed, then the corresponding cell is marked as 1
void system_parallel_reads(int *io_matrix, int *accessed_stripes, int stripe_count, int *total_write_block_num){

  int i,j;
  int io_amount;

  int io_index;


  int ret;
  int matrix_width;


  int pagesize;
  int fd_disk[15];

  pagesize=getpagesize();
  matrix_width=erasure_k+erasure_m;
  
  //count the io amount
  io_amount=calculate_chunk_num_io_matrix(io_matrix, stripe_count, erasure_k+erasure_m);

  *total_write_block_num+=io_amount;

  //record the name of disk array
  char *disk_array[15]={"/dev/sde","/dev/sdf","/dev/sdg","/dev/sdh","/dev/sdi","/dev/sdj","/dev/sdk","/dev/sdl","/dev/sdm","/dev/sdn",
  "/dev/sdo","/dev/sdp","/dev/sdq","/dev/sdr","/dev/sds"};

  /* perform the read operations */
  
  //record the start point and end point for each i/o
  struct aiocb *aio_list = (struct aiocb *)malloc(sizeof(struct aiocb)*io_amount);

  bzero(aio_list, sizeof (struct aiocb)*io_amount);

  io_index=0;
  
  for(i=0;i<matrix_width;i++){

  	fd_disk[i]=open(disk_array[i], O_RDWR | O_DIRECT);
	
	if(fd_disk[i]<0)
		printf("openfile failed, i=%d\n",i);

	for(j=0; j<stripe_count; j++){

		// if there is an io request, then initiate the aio_structure
		if(io_matrix[j*matrix_width+i]==1){

			aio_list[io_index].aio_fildes=fd_disk[i];
			aio_list[io_index].aio_nbytes=block_size;
			// make sure that the offset should not exceed the disk capacity. 
			aio_list[io_index].aio_offset=((accessed_stripes[j])%disk_capacity)*block_size;
			aio_list[io_index].aio_reqprio=0;
			ret = posix_memalign((void**)&aio_list[io_index].aio_buf, pagesize, block_size);
			io_index++;
		}
  	  }
  	}

  // perform the read operations
  for(i=0;i<io_index;i++){

		ret=aio_read(&aio_list[i]);
		if(ret<0)
		  perror("aio_read");
		
  	}

  for(i=0;i<io_index;i++){

  	while(aio_error(&aio_list[i]) == EINPROGRESS);
	if((ret = aio_return(&aio_list[i]))==-1){ 
		printf("io_index=%d, io_amount=%d\n", io_index, io_amount);
		printf("%d-th aio_read_offset=%.2lfMB\n", i, aio_list[i].aio_offset*1.0/1024/1024);
		printf("aio_read_return error, ret=%d, io_no=%d\n",ret,i);
		}

  	}


/* free the aio structure */

 free(aio_list);

 for(i=0;i<matrix_width;i++)
   close(fd_disk[i]);


}


void dr_time_caso(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map, int *chunk_to_stripe_chunk_map, 
	int *num_extra_io, double *time, int start_evlat_num){

	printf("=============> dr_time_caso:\n");

    //read the data from csv file
    FILE *fp;
	if((fp=fopen(trace_name,"r"))==NULL){
	   //printf("open file failed\n");
	   exit(0);
	   }

     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';

	 int i,j;

	 int failed_disk;

	 char pre_timestamp[100];

	 int access_start_block, access_end_block;
	 int cur_index;
	 int count;
	 int temp_count;
	 int flag;
	 int io_count;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;


	 int temp_stripe_id, temp_chunk_id;
	 int chunk_count;

	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // suppose the maximum accessed stripes is 100

	 int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);
	 int *io_request=malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp

     // initialize the io_request
     // if a chunk is requested in the s_i-th stripe, then the corresponding cell is marked with 1
	 for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
	 	io_request[i]=0;

	 int stripe_count;
	 int read_count, write_count;
	 int rotation;
	 int if_dr;
	 int sort_index, real_index;
	 
	 int *total_caso_io;
	 int c=0;
	 total_caso_io=&c;

	 stripe_count=0;
	 read_count=0;
	 write_count=0;

	 struct timeval begin_time, end_time;

    for(failed_disk=0; failed_disk< erasure_k+erasure_m; failed_disk++){

     count=0;
	 fseek(fp, 0, SEEK_SET);
	 flag=0;

	 // re-initialize the io_request array
	 for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
	  io_request[i]=0;

	 printf("cur_rcd_idx=%d\n", cur_rcd_idx);
	 
     while(fgets(operation, sizeof(operation), fp)) {

		count++;

		new_strtok(operation, divider, timestamp);
		new_strtok(operation, divider, op_type);
		new_strtok(operation, divider, offset);
		new_strtok(operation, divider, size);
		

        // if it has not reached the given_timestamp then continue
		if(count<start_evlat_num)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Read")!=0){ 
			write_count++;
			continue;
			}

		read_count++;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;		

		//printf("timestamp=%s, access_start_block=%d, access_end_block=%d\n",timestamp, access_start_block,access_end_block);

        // if a new timestamp comes
		if(strcmp(pre_timestamp,timestamp)!=0){
/*
printf("----caso, before degraded reads:\n");
for(i=0; i<stripe_count; i++){
		   
  for(j=0; j< erasure_k+erasure_m; j++)
	printf("%d ", io_request[i*(erasure_k+erasure_m)+j]);
		   
  printf("\n");
}
*/
           // perform degraded reads

           if_dr=degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 0);


/*
printf("----caso, failed_disk=%d, after degraded reads:\n", failed_disk);
for(i=0; i<stripe_count; i++){

	for(j=0; j< erasure_k+erasure_m; j++)
		printf("%d ", io_request[i*(erasure_k+erasure_m)+j]);

	printf("\n");
}
*/
		   // update the total number of io requests
		   io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

		   if(if_dr==1){
			  
			 gettimeofday(&begin_time, NULL);
			 
			 for(i=0; i<50; i++)
			   system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
			  
			 gettimeofday(&end_time, NULL);
			 *time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;
			 
			  }



		   // re-initialize the io_request array
		   for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
		   	io_request[i]=0;

           // re-initiate the stripe_count
		   stripe_count=0;
		   chunk_count=0;

		   strcpy(pre_timestamp,timestamp); 

			}
		
		for(i=access_start_block; i<=access_end_block; i++){

			sort_index=binary_search(sort_trace_pattern, cur_rcd_idx, i);
			real_index=sort_pattern_index[sort_index];
			temp_stripe_id=chunk_to_stripe_map[real_index];
			rotation=temp_stripe_id%(erasure_k+erasure_m);
			temp_chunk_id=chunk_to_stripe_chunk_map[real_index];
		
			for(j=0; j<stripe_count; j++){
		
				if(stripes_per_timestamp[j]==temp_stripe_id)
					break;
		
				}
		
			if(j>=stripe_count){
				stripes_per_timestamp[stripe_count]=temp_stripe_id;
				stripe_count++;
				//printf("----stripe_count=%d\n",stripe_count);
				}

			// update the io_request_matrix

			// if the data is on the failed disk 
			if((temp_chunk_id+rotation)%(erasure_k+erasure_m)==failed_disk)
				io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=-1;

			else 
				io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=1;
				

			//printf("temp_chunk_id=%d, temp_stripe_id=%d\n", temp_chunk_id, temp_stripe_id);
		
		
			}

     	}

	 //for the last operation, add the degraded read io

	 // perform degraded reads
	 if_dr=degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 0);

     if(if_dr==1){
	 	
	   gettimeofday(&begin_time, NULL);
	   
	   for(i=0; i<50; i++)
	     system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
	 	
	   gettimeofday(&end_time, NULL);
	   *time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;
	   
     	}

	 io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

     }

	 printf("caso_dr_io=%d\n", io_count);
	 printf("aver_read_count=%d, aver_write_count=%d\n", read_count/(erasure_k+erasure_m), write_count/(erasure_k+erasure_m));
	 fclose(fp);	 
	 free(stripes_per_timestamp);
	 free(io_request);

	 printf("<============= dr_time_caso:\n");

}


void dr_time_striping(char *trace_name, char given_timestamp[], int *num_extra_io, double *time, int start_evlat_num){

	printf("=============> dr_time_striping:\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }


     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';

	 int i,j;

	 int failed_disk;
	 
	 char pre_timestamp[100];
	 int access_start_block, access_end_block;
	 int cur_index;

	 int count;
	 int temp_count;
	 int flag;
	 int io_count;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;


	 int temp_stripe_id, temp_chunk_id;
	 int chunk_count;
	 int if_dr;

	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // suppose the maximum accessed stripes is 100

	 int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);
	 int *io_request=malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp

     // initialize the io_request
     // if a chunk is requested in the s_i-th stripe, then the corresponding cell is marked with 1
	 for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
	 	io_request[i]=0;

	 int stripe_count;
	 int rotation;
	 int *total_caso_io;
	 int c=0;
	 total_caso_io=&c;

	 stripe_count=0;

	 struct timeval begin_time, end_time;

	 for(failed_disk=0; failed_disk< erasure_k+erasure_m; failed_disk++){

      flag=0;
	  count=0;
	  
	  fseek(fp, 0, SEEK_SET);

	  for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
		 io_request[i]=0;
	 
      while(fgets(operation, sizeof(operation), fp)) {

		count++;

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);
		

        // if it has not reached the given_timestamp then continue
		if(count<start_evlat_num)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Read")!=0) 
			continue;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;		

		//printf("timestamp=%s, access_start_block=%d, access_end_block=%d\n",timestamp, access_start_block,access_end_block);

        // if a new timestamp comes
		if(strcmp(pre_timestamp,timestamp)!=0){
/*
printf("striping---, before degraded reads:\n");
for(i=0; i<stripe_count; i++){
		   
  for(j=0; j< erasure_k+erasure_m; j++)
	printf("%d ", io_request[i*(erasure_k+erasure_m)+j]);
		   
  printf("\n");
}
*/
           // perform degraded reads
           if_dr=degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 0);

/*
printf("striping---, failed_disk=%d, after degraded reads:\n", failed_disk);
for(i=0; i<stripe_count; i++){

	for(j=0; j< erasure_k+erasure_m; j++)
		printf("%d ", io_request[i*(erasure_k+erasure_m)+j]);

	printf("\n");
}
*/
	   // update the total number of io requests
		   io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

		   // perform system write
		   if(if_dr==1){
			  
			 gettimeofday(&begin_time, NULL);
			 
			 for(i=0; i<50; i++)
			   system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
			  
			 gettimeofday(&end_time, NULL);
			 *time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;
			 
			  }


		   // re-initialize the io_request array
		   for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
		   	io_request[i]=0;

           // re-initiate the stripe_count
		   stripe_count=0;
		   chunk_count=0;

		   strcpy(pre_timestamp,timestamp); 

		   //printf("parity_update_io=%d\n",io_count);

		   //printf("cur_timestamp=%s\n", timestamp);

			}

		// determine the stripes they belong 
		// we compare each chunk in chunk_to_stripe_map 
		
		for(i=access_start_block; i<=access_end_block; i++){
			
			temp_stripe_id=i/erasure_k;
			rotation=temp_stripe_id%(erasure_k+erasure_m);
			temp_chunk_id=i%erasure_k;
			
			for(j=0; j<stripe_count; j++){
		
				if(stripes_per_timestamp[j]==temp_stripe_id)
					break;
		
				}
		
			if(j>=stripe_count){
				stripes_per_timestamp[stripe_count]=temp_stripe_id;
				//printf("i=%d, temp_stripe_id=%d, temp_chunk_id=%d, stripe_count=%d\n", i, temp_stripe_id, temp_chunk_id, stripe_count);
				stripe_count++;
				//printf("----stripe_count=%d\n",stripe_count);
				}


			// update the io_request_matrix

			// if the data is on the failed disk 
			if((temp_chunk_id+rotation)%(erasure_k+erasure_m)==failed_disk)
				io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=-1;

			else 
				io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=1;
				

			//printf("temp_chunk_id=%d, temp_stripe_id=%d\n", temp_chunk_id, temp_stripe_id);
		
		
			}

     	}

	 //for the last operation, add the parity update io

	 // perform degraded reads
	 
	 if_dr=degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 0);

     if(if_dr==1){
	 	
	   gettimeofday(&begin_time, NULL);
	   
	   for(i=0; i<50; i++)
	     system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
	 	
	   gettimeofday(&end_time, NULL);
	   *time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;
	   
     	}

	 // update the total number of io requests
	 io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

     }

	 printf("striping_dr_io=%d\n", io_count);
	 
	 fclose(fp);
	 free(stripes_per_timestamp);
	 free(io_request);

	 printf("<============= dr_time_striping:\n");

}




void dr_time_continugous(char *trace_name, char given_timestamp[], int *num_extra_io, double *time, int start_evlat_num){

	printf("=============> dr_time_continugous:\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  //printf("open file failed\n");
	  exit(0);
  	  }


     char operation[100];
	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';
	 char pre_timestamp[100];

	 int i,j;
	 int failed_disk;
	 int access_start_block, access_end_block;
	 int cur_index;
	 int count;
	 int temp_count;
	 int flag;
	 int io_count;

	 int temp_group_id, temp_stripe, temp_group_block_id;
	 int temp_row_id;
	 int group_num_blocks=contiguous_block*erasure_k;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;
	 temp_count=0;

	 int chunk_count;
     int stripe_count;
	 int temp_contiguous_block;
	 int temp_chunk_id;

	 flag=0;
	 io_count=0;

	 int max_accessed_stripes=max_access_chunks_per_timestamp; // suppose the maximum accessed stripes is 100

	 int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);
	 int *io_request=malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp

     // initialize the io_request
     // if a chunk is requested in the s_i-th stripe, then the corresponding cell is marked with 1
	 for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
	 	io_request[i]=0;

	 int *total_caso_io;
	 int c=0;
	 total_caso_io=&c;

	 stripe_count=0;

	 struct timeval begin_time, end_time;

	 for(failed_disk=0; failed_disk< erasure_k+erasure_m; failed_disk++){

	  count=0;

	  flag=0;

	  fseek(fp, 0, SEEK_SET);

	  for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
		 io_request[i]=0;

      while(fgets(operation, sizeof(operation), fp)) {

		count++;

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);
		

        // if it has not reached the given_timestamp then continue
		if(count<start_evlat_num)
			continue;

        // if it reaches the given timestamp
		flag=1;

		// get the operation 
		if(strcmp(op_type, "Read")!=0) 
			continue;

		// get the access chunks 
		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;		


        // if a new timestamp comes
		if(strcmp(pre_timestamp,timestamp)!=0){
/*
printf("continugous---, before degraded reads:\n");
for(i=0; i<stripe_count; i++){
		   
  for(j=0; j< erasure_k+erasure_m; j++)
	printf("%d ", io_request[i*(erasure_k+erasure_m)+j]);
		   
  printf("\n");
}
*/

           // perform degraded reads
           degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 1);

/*
printf("continugous---, failed_disk=%d, after degraded reads:\n", failed_disk);
for(i=0; i<stripe_count; i++){

	for(j=0; j< erasure_k+erasure_m; j++)
		printf("%d ", io_request[i*(erasure_k+erasure_m)+j]);

	printf("\n");
}
*/
		   // update the total number of io requests
		   io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);
		   
		   // perform system write
		   gettimeofday(&begin_time, NULL);
		   for(i=0; i<50; i++){
		   system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
			  }
		   gettimeofday(&end_time, NULL);
		   *time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;


		   // re-initialize the io_request array
		   for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
		   	io_request[i]=0;

           // re-initiate the stripe_count
		   stripe_count=0;
		   chunk_count=0;

		   strcpy(pre_timestamp,timestamp); 


			}

		// determine the stripes they belong 
		// we compare each chunk in chunk_to_stripe_map 

		//printf("access_start_block=%d, access_end_block=%d\n",access_start_block,access_end_block);
		
		for(i=access_start_block; i<=access_end_block; i++){
			
			// locate the corresponding group and block_id in that group
			temp_group_id=i/group_num_blocks;
			temp_group_block_id=i%group_num_blocks;

			temp_contiguous_block=contiguous_block;
			
			// locate the column in that group
			temp_row_id=temp_group_block_id%temp_contiguous_block;
			
			// locate the stripe_id 
			temp_stripe=temp_group_id*contiguous_block+temp_row_id; 
			
			// locate the column_id
			temp_chunk_id=temp_group_block_id/temp_contiguous_block;

			// printf("block_id=%d, group_num_blocks=%d, temp_group_block_id=%d, contiguous_block=%d, temp_row_id=%d, temp_column_id=%d\n", i, group_num_blocks, temp_group_block_id, contiguous_block, temp_row_id, temp_chunk_id);
			
			for(j=0; j<stripe_count; j++)
				if(stripes_per_timestamp[j]==temp_stripe)
					break;
			
			
			if(j>=stripe_count){
			
				stripes_per_timestamp[stripe_count]=temp_stripe;
				stripe_count++;
			
				}

			// update the io_request_matrix
			// if the data is on the failed disk 
			if((temp_chunk_id)%(erasure_k+erasure_m)==failed_disk)
				io_request[j*(erasure_k+erasure_m)+(temp_chunk_id)%(erasure_k+erasure_m)]=-1;

			else 
				io_request[j*(erasure_k+erasure_m)+(temp_chunk_id)%(erasure_k+erasure_m)]=1;
				

			//printf("temp_chunk_id=%d, temp_stripe_id=%d\n", temp_chunk_id, temp_stripe_id);
		
		
			}

     	}

	 //for the last operation, add the parity update io

	 // perform degraded reads
	 degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 1);

	 gettimeofday(&begin_time, NULL);
	 for(i=0; i<50; i++){
	 system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
	 	}
	 gettimeofday(&end_time, NULL);
	 *time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;


	 // update the total number of io requests
	 io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

     }

	 printf("continugous_dr_io=%d\n", io_count);
	 
	 fclose(fp);
	 free(stripes_per_timestamp);
	 free(io_request);

	 printf("<============= dr_time_continugous:\n");

}


void sorting_trace_access_pattern(){

	int i;

	for(i=0; i<cur_rcd_idx; i++){

		sort_trace_pattern[i]=trace_access_pattern[i];
		sort_pattern_index[i]=i;

		}

	QuickSort_index(sort_trace_pattern, sort_pattern_index, 0, cur_rcd_idx-1);

}

//find a value in a sorted array
void binary_search(int array[], int num, int value){

	int i; 
	int mid;
	int start;
	int end;

	start=0;
	end=num-1;

	while(1){

		mid=start+(end-start)/2;

		if(array[mid]<value)
			end=mid-1;

		else if(array[mid]>value)
			start=mid+1;

		else if(array[mid]==value)
			break;
		}

	return mid;
	
}
