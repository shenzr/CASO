// this program reads read patterns from workloads and measure the ratio 
// (#the number of blocks requested with other blocks in more than 2 I/O operations)/(# the number of accessed blocks in a workload)

#define _GNU_SOURCE 
#define __USE_LARGEFILE64

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
#include <assert.h>

#include "jerasure/jerasure.h"
#include "jerasure/galois.h"
#include "common.h"

#define inf 999999999
#define talloc(type, num) (type *) malloc(sizeof(type)*(num))
#define max_offset (2*1000*1000*1000-1) //KB

int num_distinct_chunks_timestamp[num_assume_timestamp];
int poten_crrltd_cnt;

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

//find a value in a sorted array
int binary_search(int array[], int num, int value){

	int mid;
	int start;
	int end;

	start=0;
	end=num-1;

	if(array[start]<value || array[end]>value){

		printf("ERR: range error!\n");
		exit(1);

	}

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
		printf("ERR: open file error!\n");
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

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);

		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;

		total_access_chunk_num += access_end_block - access_start_block + 1;

		//if it is the begining of the first pattern of a timestamp, then collect the info of the last timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

			//printf("\n%s\n",pre_timestamp);
			strcpy(pre_timestamp,timestamp);

			//if it is not the first timestamp, then initialize parameters
			if(if_begin==0){

				num_distinct_chunks_timestamp[num_timestamp]=num_distict_chunks_per_timestamp;
				num_distict_chunks_per_timestamp=0;

				//record the maximum number of accessed chunks in a timestamp
				if(temp_count>max_access_chunks_per_timestamp)
					max_access_chunks_per_timestamp=temp_count;

				num_timestamp++;

				if(num_timestamp>=num_assume_timestamp){
					printf("ERR: num_timestamp>=num_assume_timestamp!\n");
					exit(1);
				}
			}

			else
				if_begin=0;

			temp_count=access_end_block-access_start_block+1;

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

				if(cur_rcd_idx>=max_aces_blk_num){

					printf("ERR: cur_rcd_idx>=max_aces_blk_num\n");
					exit(0);

					}
			}
		}


	}

	// for the access pattern in the last timestamp
	if(temp_count>max_access_chunks_per_timestamp)
		max_access_chunks_per_timestamp=temp_count;

	num_distinct_chunks_timestamp[num_timestamp]=num_distict_chunks_per_timestamp;
	num_timestamp++;

	fclose(fp);

    printf("cur_rcd_idx=%d\n", cur_rcd_idx);
    printf("trace_access_pattern:\n");
	print_matrix(trace_access_pattern, cur_rcd_idx, 1);

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

	//printf("test_caso_rcd_idx=%d\n", caso_rcd_idx);

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
	int count;
	int if_begin;

	count=0;
	if_begin=1;

	caso_rcd_idx=0;

	while(fgets(operation, sizeof(operation), fp)) {

		//printf("count=%d\n",count);

        //if it is the beginning of the trace
		if(if_begin==1){

			if_begin=0;
			strcpy(pre_timestamp, timestamp);

			}

		new_strtok(operation,divider, timestamp);

		if(strcmp(pre_timestamp,timestamp)!=0){

			// calculate the numeber of timestamp
			if(strcmp(begin_timestamp,timestamp)!=0)
				caso_rcd_idx+=num_distinct_chunks_timestamp[count];

			else break;

			strcpy(pre_timestamp, timestamp);

			count++;

		}

	}

	//printf("======caso_rcd_idx=%d\n", caso_rcd_idx);

	fclose(fp);

}


void insert_chunk_into_bucket(int* bucket, int bucket_num, int chunk_id){

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

	if(i>=bucket_depth){

		printf("bucket-%d is full!\n", bucket_id);
		exit(1);

	}

}


void replace_old_peer_chunk(int* freq_peer_chks, int* rcd_peer_chks, int* caso_poten_crrltd_chks, int* num_peer_chks, 
		int given_chunk_idx, int given_chunk, int peer_chunk, int* start_search_posi){

	int k;
	int rplcd_chk_id;
	int sort_rplcd_chk_idx;
	int start_posi;

#if debug	
	printf("rcd_peer_chks:\n");
	print_matrix(rcd_peer_chks, max_num_peer_chunks, poten_crrltd_cnt);
#endif

	start_posi=start_search_posi[given_chunk_idx];

	printf("replace the old and non-correlated chunk\n");
	
	//we replace the chunk according to the access time 
	//we assume that an old access chunk that are not correlated will not be a correlated chunk any more
	for(k=start_posi; k<max_num_peer_chunks; k++){

		if(freq_peer_chks[given_chunk_idx*max_num_peer_chunks+k]<2){

			rplcd_chk_id=rcd_peer_chks[given_chunk_idx*max_num_peer_chunks+k];
			rcd_peer_chks[given_chunk_idx*max_num_peer_chunks+k]=peer_chunk;
			freq_peer_chks[given_chunk_idx*max_num_peer_chunks+k]=1;

			break;
		}
	}

	//update the start_search_posi
	start_search_posi[given_chunk_idx]=k+1;

	if(k>=max_num_peer_chunks){

		printf("ERR: cannot find a replaced chunk!\n");
		exit(1);

	}

	//remove the access record at the row of rplcd_chk_id
	sort_rplcd_chk_idx=binary_search(caso_poten_crrltd_chks, poten_crrltd_cnt, rplcd_chk_id);

	for(k=1; k<max_num_peer_chunks; k++){

		if(rcd_peer_chks[sort_rplcd_chk_idx*max_num_peer_chunks+k]==given_chunk){

			rcd_peer_chks[sort_rplcd_chk_idx*max_num_peer_chunks+k]=-1;
			freq_peer_chks[sort_rplcd_chk_idx*max_num_peer_chunks+k]=0;
			//update the number of peer chunks of rplcd_chk_id
			num_peer_chks[sort_rplcd_chk_idx]--;

			if(k<start_search_posi[sort_rplcd_chk_idx])
				start_search_posi[sort_rplcd_chk_idx]=k;

			break;

		}
	}

	if(k>=max_num_peer_chunks){

		printf("ERR: cannot find the given chunk!\n");
		exit(1);

	}

}


void record_access_freq(int bgn_tmstmp_num, int* analyze_chunks_time_slots, int* caso_poten_crrltd_chks, 
		int* num_chunk_per_timestamp, int* rcd_peer_chks, int* freq_peer_chks, int* num_peer_chks){

	int i,j; 
	int chk_idx1, chk_idx2;
	int chunk_id1, chunk_id2;
	int sort_chk_idx1, sort_chk_idx2;	

	int* start_search_posi=(int*)malloc(sizeof(int)*poten_crrltd_cnt); //it records the position to start search for replacement, in order to save search time

	memset(start_search_posi, 0, sizeof(int)*poten_crrltd_cnt);
	memset(rcd_peer_chks, -1, sizeof(int)*poten_crrltd_cnt*max_num_peer_chunks);
	memset(freq_peer_chks, 0, sizeof(int)*poten_crrltd_cnt*max_num_peer_chunks);
	memset(num_peer_chks, 0, sizeof(int)*poten_crrltd_cnt);

	for(i=0; i<poten_crrltd_cnt; i++)
		rcd_peer_chks[i*max_num_peer_chunks]=caso_poten_crrltd_chks[i];

	//record the chunks that are accessed within a timestamp and update the frequency when they are simultaneously accessed
	for(i=0; i<bgn_tmstmp_num;i++){

		if(num_chunk_per_timestamp[i]==1)
			continue;

		for(chk_idx1=0; chk_idx1<num_chunk_per_timestamp[i]-1; chk_idx1++){

			for(chk_idx2=chk_idx1+1; chk_idx2<num_chunk_per_timestamp[i]; chk_idx2++){

				chunk_id1=analyze_chunks_time_slots[i*max_access_chunks_per_timestamp+chk_idx1];
				chunk_id2=analyze_chunks_time_slots[i*max_access_chunks_per_timestamp+chk_idx2];

				sort_chk_idx1=binary_search(caso_poten_crrltd_chks, poten_crrltd_cnt, chunk_id1);
				sort_chk_idx2=binary_search(caso_poten_crrltd_chks, poten_crrltd_cnt, chunk_id2);

				//update rcd_peer_chks and freq_peer_chks of chunk_id1
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
						//increase the peer_chunk count
						num_peer_chks[sort_chk_idx1]++;

						break;

					}
				}

				//if the row of sort_chk_idx1 is full, then replace an old and uncorrelated chunk
				if(j>=max_num_peer_chunks)
					replace_old_peer_chunk(freq_peer_chks, rcd_peer_chks, caso_poten_crrltd_chks, num_peer_chks, sort_chk_idx1, chunk_id1, chunk_id2, start_search_posi);

				//update rcd_peer_chks and freq_peer_chks of chunk_id2
				for(j=1; j<max_num_peer_chunks; j++){

					//if the chunk has been recorded, then update frequency
					if(rcd_peer_chks[sort_chk_idx2*max_num_peer_chunks+j]==chunk_id1){

						freq_peer_chks[sort_chk_idx2*max_num_peer_chunks+j]++;
						break;

					}

					//otherwise, record the chunk and update frequency
					if(rcd_peer_chks[sort_chk_idx2*max_num_peer_chunks+j]==-1){

						rcd_peer_chks[sort_chk_idx2*max_num_peer_chunks+j]=chunk_id1;
						freq_peer_chks[sort_chk_idx2*max_num_peer_chunks+j]++;
						//update the number of peer chunks of chunk_id2
						num_peer_chks[sort_chk_idx2]++;

						break;

					}
				}

				//if the row of sort_chk_idx2 is full, then replace an old and uncorrelated chunk
				if(j>=max_num_peer_chunks)
					replace_old_peer_chunk(freq_peer_chks, rcd_peer_chks, caso_poten_crrltd_chks, num_peer_chks, sort_chk_idx2, chunk_id2, chunk_id1, start_search_posi);

			}
		}
	} 

#if debug
	printf("rcd_peer_chks:\n");
	print_matrix(rcd_peer_chks, max_num_peer_chunks, poten_crrltd_cnt);
#endif

	free(start_search_posi);

}

/* This function is to extract the correlated data chunks and their degrees, for serving the next stripe organization
 *@rcd_peer_chks: record the chunks that are simultaneously accessed within a timestamp. 
 */
void extract_caso_crltd_chnk_dgr(int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix, int* rcd_peer_chks, int* freq_peer_chks, 
		int num_correlate_chunk, int* num_peer_chunk){

	int i,j;
	int crltd_chunk_cnt;
	int its_crltd_chunk_cnt;
	int if_crrltd_exist;
	int peer_count;

#if debug
	printf("rcd_peer_chks:\n");
	print_matrix(rcd_peer_chks, max_num_peer_chunks, poten_crrltd_cnt);

	printf("freq_peer_chks:\n");
	print_matrix(freq_peer_chks, max_num_peer_chunks, poten_crrltd_cnt);

	printf("num_peer_chunk:\n");
	print_matrix(num_peer_chunk, poten_crrltd_cnt, 1);
#endif

	//printf("num_correlate_chunk=%d\n",num_correlate_chunk);
	memset(caso_crltd_mtrx, -1, sizeof(int)*num_correlate_chunk*max_num_relevent_chunks_per_chunk);
	memset(caso_crltd_dgr_mtrix, -1, sizeof(int)*num_correlate_chunk*max_num_relevent_chunks_per_chunk);

	crltd_chunk_cnt=0;
	peer_count=0;

#if debug
	printf("freq_peer_chks:\n");
	print_matrix(freq_peer_chks, max_num_peer_chunks, poten_crrltd_cnt);

	printf("num_peer_chunk:\n");
	print_matrix(num_peer_chunk, poten_crrltd_cnt, 1);
#endif

	for(i=0; i<poten_crrltd_cnt; i++){

		its_crltd_chunk_cnt=1;
		if_crrltd_exist=0;
		peer_count=0;

		for(j=1; j<max_num_peer_chunks; j++){

			//if the chunk with index i and that with index j are correlated, then record them
			if(freq_peer_chks[i*max_num_peer_chunks+j]>=2){

				//record its correlated chunks and its degree
				caso_crltd_mtrx[crltd_chunk_cnt*max_num_relevent_chunks_per_chunk+its_crltd_chunk_cnt]=rcd_peer_chks[i*max_num_peer_chunks+j];
				caso_crltd_dgr_mtrix[crltd_chunk_cnt*max_num_relevent_chunks_per_chunk+its_crltd_chunk_cnt]=freq_peer_chks[i*max_num_peer_chunks+j];

			    its_crltd_chunk_cnt++;
			    if_crrltd_exist=1;

			}

			if(freq_peer_chks[i*max_num_peer_chunks+j]>=1)
				peer_count++;

			if(peer_count==num_peer_chunk[i]){

				//we mark the correlated chunk finally
				if(if_crrltd_exist==1){

					caso_crltd_mtrx[crltd_chunk_cnt*max_num_relevent_chunks_per_chunk]=rcd_peer_chks[i*max_num_peer_chunks];
					
					crltd_chunk_cnt++;

				}

				break;
			}

		}
		
	}

	printf("caso_crltd_mtrx:\n");
	print_matrix(caso_crltd_mtrx, max_num_relevent_chunks_per_chunk, num_correlate_chunk);

	printf("caso_crltd_dgr_mtrix:\n");
	print_matrix(caso_crltd_dgr_mtrix, max_num_relevent_chunks_per_chunk, num_correlate_chunk);

}

int find_max_crrltd_chnk_udr_bundry(int num_correlated_chunk, int* crrltd_chnk_set, int given_chunk){

	int start,end,mid;

	start=0;
	end=num_correlated_chunk-1;
	mid=start+(end-start)/2;

	if(crrltd_chnk_set[0] < given_chunk)
		return num_correlated_chunk;

	if(crrltd_chnk_set[num_correlated_chunk-1] > given_chunk)
		return 0;

	while(1){

		if(crrltd_chnk_set[mid] > given_chunk)
			start=mid;

		else if(crrltd_chnk_set[mid] < given_chunk)
			end=mid;

		mid=start+(end-start)/2;

		//printf("start_chunk=%d, mid_chunk=%d, end_chunk=%d\n", crrltd_chnk_set[start], crrltd_chnk_set[mid], crrltd_chnk_set[end]);

		if(start==mid) 
			break;

	}

	return (num_correlated_chunk-mid-1);

}


// this function is to calculate the priority of the relevant chunks in stripe organization 
int calculate_priority(int *stripe_map, int cur_chunk_num, int *temp_stripe_idx_in_rele_chunk_table, int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix,  
		int candidate_chunk_index){

	int i;
	int count;
	int slct_chnk;

	int k;

	count=0;

	for(i=0;i<cur_chunk_num;i++){

		slct_chnk=caso_crltd_mtrx[temp_stripe_idx_in_rele_chunk_table[i]*max_num_relevent_chunks_per_chunk];

		// accumulate the priority
		for(k=1; k<max_num_relevent_chunks_per_chunk; k++){

			if(caso_crltd_mtrx[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k]==slct_chnk)
				break;

			if(caso_crltd_mtrx[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k]==-1)
				break;

		}

		if(caso_crltd_mtrx[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k]==slct_chnk)
			count+=caso_crltd_dgr_mtrix[candidate_chunk_index*max_num_relevent_chunks_per_chunk+k];

	}

	return count;

}



// this function is to update the correlation degree among the chunks organized in the same stripe
void updt_stripe_chnk_crrltn_dgr(int *stripe_map, int cur_chunk_num, int *temp_stripe_idx_in_rele_chunk_table, int* caso_crltd_mtrx, 
		int* caso_crltd_dgr_mtrix,  int slct_chunk_index, int* stripe_chnk_crrltn_dgr){

	int i;
	int slct_chnk;
	int k;

	for(i=0;i<cur_chunk_num;i++){

		slct_chnk=caso_crltd_mtrx[temp_stripe_idx_in_rele_chunk_table[i]*max_num_relevent_chunks_per_chunk];

		// record the priority
		for(k=1; k<max_num_relevent_chunks_per_chunk; k++){

			if(caso_crltd_mtrx[slct_chunk_index*max_num_relevent_chunks_per_chunk+k]==slct_chnk)
				break;

			if(caso_crltd_mtrx[slct_chunk_index*max_num_relevent_chunks_per_chunk+k]==-1)
				break;

		}

		if(caso_crltd_mtrx[slct_chunk_index*max_num_relevent_chunks_per_chunk+k]==slct_chnk)
			stripe_chnk_crrltn_dgr[cur_chunk_num*(erasure_k+1)+1+i]=caso_crltd_dgr_mtrix[slct_chunk_index*max_num_relevent_chunks_per_chunk+k];

	}

}


void lrc_local_group_orgnzt(int* stripe_chnk_crrltn_dgr, int* chunk_to_local_group, int* stripe_chnk_idx_in_crrltn_set, int* crrltd_chnk_pttn_idx){

	if((lrc_lg==0) || (erasure_k%lrc_lg!=0)){

		printf("ERR: LRC config\n");
		exit(1);

	}

	int i;
	int j,k;
	int fir_idx, sec_idx;
	int max_dgr=-1;
	int num_chunk_per_lg;
	int chunk_id;
	int count;
	int priority;
	int slct_chnk_idx;
	int chose_chnk_idx;
	int stripe_chunk_idx;
	int crrltd_chunk_idx;

	fir_idx=-1;
	sec_idx=-1;

	num_chunk_per_lg=erasure_k/lrc_lg;

	int* if_select=(int*)malloc(sizeof(int)*erasure_k);
	int* slct_lg_chnk_idx=(int*)malloc(sizeof(int)*num_chunk_per_lg);

	memset(if_select, 0, sizeof(int)*erasure_k);

	for(i=0; i<lrc_lg; i++){

		//select the first two chunks with maximum correlation degree among the remaining chunks in cur_stripe
		max_dgr=-1;
		count=0;

		memset(slct_lg_chnk_idx, -1, sizeof(int)*num_chunk_per_lg);

		for(j=0; j<erasure_k; j++){

			if(if_select[j]==1)
				continue;

			for(k=0; k<j; k++){

				if(if_select[k]==1)
					continue;

				if(stripe_chnk_crrltn_dgr[j*(erasure_k+1)+k+1]>=max_dgr){

					fir_idx=j;
					sec_idx=k;
					max_dgr=stripe_chnk_crrltn_dgr[j*(erasure_k+1)+k+1];

				}
			}
		}

		// after finding the first two chunks 
		if_select[fir_idx]=1;
		if_select[sec_idx]=1;

		// insert the two chunks 
		slct_lg_chnk_idx[count++]=fir_idx;
		slct_lg_chnk_idx[count++]=sec_idx;

		// establish the local group of the last chunks 
		for(count=2; count < num_chunk_per_lg; count++){

			max_dgr=-1;

			for(chunk_id=0; chunk_id < erasure_k; chunk_id++){

				if(if_select[chunk_id]==1)
					continue;

				//calculate the priority of chunk_id
				priority=0;

				for(j=0; j<count; j++){

					slct_chnk_idx=slct_lg_chnk_idx[j];

					if(stripe_chnk_crrltn_dgr[slct_chnk_idx*(erasure_k+1)+1+chunk_id]>0)
						priority+=stripe_chnk_crrltn_dgr[slct_chnk_idx*(erasure_k+1)+1+chunk_id];

					if(stripe_chnk_crrltn_dgr[chunk_id*(erasure_k+1)+1+slct_chnk_idx]>0)
						priority+=stripe_chnk_crrltn_dgr[chunk_id*(erasure_k+1)+1+slct_chnk_idx];

				}

				// select the one with the maximum priority
				if(priority >= max_dgr){

					max_dgr=priority;
					chose_chnk_idx=chunk_id;

				}

			}


			// update the slct_lg_chnk_idx
			if_select[chose_chnk_idx]=1;
			slct_lg_chnk_idx[count]=chose_chnk_idx;
			
		}
		
		// update chunk_to_local_group_map
		for(j=0; j<num_chunk_per_lg; j++){

			stripe_chunk_idx=slct_lg_chnk_idx[j];
			crrltd_chunk_idx=stripe_chnk_idx_in_crrltn_set[stripe_chunk_idx];
			// update the chunk_to_local_group
			chunk_to_local_group[crrltd_chnk_pttn_idx[crrltd_chunk_idx]]=i;
		}

	}

	free(if_select);
	free(slct_lg_chnk_idx);

}



// this function takes the accessed stripes as input and outputs an array that records the stripe organization
void stripe_orgnzt(int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix, int num_correlated_chunk, int* chunk_to_stripe_map, 
						int* chunk_to_stripe_chunk_map, int* chunk_to_local_group_map, int* crrltd_chnk_pttn_idx){

	printf("===========> stripe_organization:\n");

	int i,j;
	int first_chunk_index, second_chunk_index;
	int first_chunk, second_chunk;
	int temp_max;

	int* temp_stripe=(int*)malloc(sizeof(int)*erasure_k); // it records the chunks in a temp stripe
	int* temp_stripe_idx_to_rele_chunk_table=(int*)malloc(sizeof(int)*erasure_k);
	int* if_select_correlated_chunks=(int*)malloc(sizeof(int)*num_correlated_chunk);//mark if a correlated data chunk is selected in stripe organization
	int* crrltd_chnk_set=(int*)malloc(sizeof(int)*num_correlated_chunk);
	int* stripe_chnk_crrltn_dgr=(int*)malloc(sizeof(int)*(erasure_k+1)*erasure_k); //record the correlation degree among the chunks within a stripe
	int* stripe_chnk_idx_in_crrltn_set=(int*)malloc(sizeof(int)*erasure_k); //record the index of the stripe chunks in correlated chunks
	
	memset(if_select_correlated_chunks, 0, sizeof(int)*num_correlated_chunk);
	memset(stripe_chnk_crrltn_dgr, 0, sizeof(int)*(erasure_k+1)*erasure_k);

	first_chunk_index=-1;
	second_chunk_index=-1;

	//init crrltd_chnk_set
	for(i=0; i<num_correlated_chunk; i++)
		crrltd_chnk_set[i]=caso_crltd_mtrx[i*max_num_relevent_chunks_per_chunk];

#if debug
	printf("crrltd_chnk_set:\n");
	print_matrix(caso_crltd_mtrx, max_num_relevent_chunks_per_chunk, num_correlated_chunk);
#endif

	// initiate the unrelevant chunks and their indices
	int stripe_count;
	int temp_count;
	int priority;
	int select_chunk_index;
	int flag;
	int max_crrltd_stripe;
	int candidate_chunk;
	int temp_index;
	int select_chunk;
	int num_chunk_per_lg;
	int ognz_crrltd_cnt;
	int if_exist_clltn;

	num_chunk_per_lg=erasure_k/lrc_lg;
	stripe_count=0;
	ognz_crrltd_cnt=0;

    // the maximum number of correlated stripes 
	max_crrltd_stripe=num_correlated_chunk/erasure_k;

	printf("max_crrltd_stripe=%d\n", max_crrltd_stripe);

	int* ognzd_crrltd_chnk=(int*)malloc(sizeof(int)*erasure_k*max_crrltd_stripe);
	memset(ognzd_crrltd_chnk, -1, sizeof(int)*erasure_k*max_crrltd_stripe);

	// we need to arrange the chunk_to_stripe_map in ascending order
	for(i=0;i<cur_rcd_idx;i++)
		chunk_to_stripe_map[i]=-1;

    // organize the correlated stripes first 
	for(stripe_count=0; stripe_count<max_crrltd_stripe; stripe_count++){

		memset(stripe_chnk_idx_in_crrltn_set, -1, sizeof(int)*erasure_k);

		// initialize temp_stripe_idx_to_rele_chunk_table
		for(i=0; i<erasure_k; i++)
			temp_stripe_idx_to_rele_chunk_table[i]=-1;

		temp_count=0;
		if_exist_clltn=0;

		// find out the first two most relevant data chunks 
		temp_max=-1;

		for(i=0;i<num_correlated_chunk;i++){

			//if a correlated chunk is selected, we mark all the values of its row to be -1
			if(if_select_correlated_chunks[i]==1) 
				continue;

			for(j=1;j<max_num_relevent_chunks_per_chunk;j++){

				if(caso_crltd_mtrx[i*max_num_relevent_chunks_per_chunk+j]==-1)
					break;

				if(caso_crltd_dgr_mtrix[i*max_num_relevent_chunks_per_chunk+j]>temp_max){

					candidate_chunk=caso_crltd_mtrx[i*max_num_relevent_chunks_per_chunk+j];
					temp_index=binary_search(crrltd_chnk_set, num_correlated_chunk, candidate_chunk);

					if(if_select_correlated_chunks[temp_index]==1)
						continue;

					first_chunk_index=i;
					second_chunk_index=temp_index;

					//printf("first_chunk_index=%d, second_chunk_index=%d\n", first_chunk_index, second_chunk_index);
					temp_max=caso_crltd_dgr_mtrix[i*max_num_relevent_chunks_per_chunk+j];
					if_exist_clltn=1;

				}
			}

		}

		// if there is no correlations among the remaining correlated data chunks, then break
		if(if_exist_clltn==0)
			break;

		printf("the %d-th correlated stripe: first_chunk_index=%d, second_chunk_index=%d, init_degree=%d\n", stripe_count, caso_crltd_mtrx[first_chunk_index*max_num_relevent_chunks_per_chunk], caso_crltd_mtrx[second_chunk_index*max_num_relevent_chunks_per_chunk], temp_max);

		// mark the cell to indicate that these two chunks have been selected 
		if_select_correlated_chunks[first_chunk_index]=1;
		if_select_correlated_chunks[second_chunk_index]=1;

		/* insert the two chunks into the stripe */
		// for the first chunk
		chunk_to_stripe_map[crrltd_chnk_pttn_idx[first_chunk_index]]=stripe_count;
		chunk_to_stripe_chunk_map[crrltd_chnk_pttn_idx[first_chunk_index]]=0;

		// record the index 
		stripe_chnk_idx_in_crrltn_set[temp_count]=first_chunk_index;

		// update stripe_chnk_crrltn_dgr
		first_chunk=trace_access_pattern[crrltd_chnk_pttn_idx[first_chunk_index]];
		stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)]=first_chunk;

		// update ognzd_crrltd_chnk
		ognzd_crrltd_chnk[ognz_crrltd_cnt]=first_chunk;
		ognz_crrltd_cnt++;

		temp_stripe[temp_count]=first_chunk;
		temp_stripe_idx_to_rele_chunk_table[temp_count]=first_chunk_index;
		temp_count++;

		// for the second chunk
		chunk_to_stripe_map[crrltd_chnk_pttn_idx[second_chunk_index]]=stripe_count;
		chunk_to_stripe_chunk_map[crrltd_chnk_pttn_idx[second_chunk_index]]=1;

		// record the index 
		stripe_chnk_idx_in_crrltn_set[temp_count]=second_chunk_index;

		// update stripe_chnk_crrltn_dgr
		second_chunk=trace_access_pattern[crrltd_chnk_pttn_idx[second_chunk_index]];
		stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)]=second_chunk;
		stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)+1]=temp_max; 

		// update ognzd_crrltd_chnk
		ognzd_crrltd_chnk[ognz_crrltd_cnt]=second_chunk;
		ognz_crrltd_cnt++;

		temp_stripe[temp_count]=second_chunk;
		temp_stripe_idx_to_rele_chunk_table[temp_count]=second_chunk_index;
		temp_count++;

		// after that, we perform greedy selection
		for(temp_count=2; temp_count<erasure_k; temp_count++){

			flag=0; 
			temp_max=-1;

			for(i=0;i<num_correlated_chunk;i++){

				if(if_select_correlated_chunks[i]==1) 
					continue;

				// calculate the priority 
				priority=calculate_priority(temp_stripe, temp_count, temp_stripe_idx_to_rele_chunk_table, caso_crltd_mtrx, caso_crltd_dgr_mtrix, i);

				// we have to consider the case if there is no correlated data chunk with non-zero priority
				if(priority>=temp_max){

					temp_max=priority;
					select_chunk_index=i;
					flag=1;

				}
			}	

			// if there still exists relevant chunks, then organize the chunks that owns largest priority
			if(flag==1){

				chunk_to_stripe_map[crrltd_chnk_pttn_idx[select_chunk_index]]=stripe_count;
				chunk_to_stripe_chunk_map[crrltd_chnk_pttn_idx[select_chunk_index]]=temp_count;

				//record the index 
				stripe_chnk_idx_in_crrltn_set[temp_count]=select_chunk_index;

				// update stripe_chnk_crrltn_dgr
				select_chunk=trace_access_pattern[crrltd_chnk_pttn_idx[select_chunk_index]];
				stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)]=select_chunk;

				// update ognzd_crrltd_chnk
				ognzd_crrltd_chnk[ognz_crrltd_cnt]=select_chunk;
				ognz_crrltd_cnt++;

                if(strcmp(code_type, "lrc")==0)
					updt_stripe_chnk_crrltn_dgr(temp_stripe, temp_count, temp_stripe_idx_to_rele_chunk_table, caso_crltd_mtrx, 
												caso_crltd_dgr_mtrix, select_chunk_index, stripe_chnk_crrltn_dgr);

				temp_stripe[temp_count]=select_chunk;
				temp_stripe_idx_to_rele_chunk_table[temp_count]=select_chunk_index;

				if_select_correlated_chunks[select_chunk_index]=1;

			}

			printf("flag=%d, selected_chunk=%d, priority=%d\n", flag, caso_crltd_mtrx[select_chunk_index*max_num_relevent_chunks_per_chunk], temp_max);
			
		}

#if debug
		// print the stripe set and the correlationd degrees
		printf("%d-th stripe:\n", stripe_count);
		print_matrix(temp_stripe, erasure_k, 1);

		printf("correlation degree:\n");
		print_matrix(stripe_chnk_crrltn_dgr, (erasure_k+1), erasure_k);
#endif		

		//if it is LRC, then we have to organize the chunks of a stripe into local groups
		if(strcmp(code_type, "lrc")==0) 
			lrc_local_group_orgnzt(stripe_chnk_crrltn_dgr, chunk_to_local_group_map, stripe_chnk_idx_in_crrltn_set, crrltd_chnk_pttn_idx);

	}

	printf("correlated stripes are completely organized!\n");
	printf("ognz_crrltd_cnt=%d\n",ognz_crrltd_cnt);

	int crrltd_stripe=stripe_count;

	/* organize the remaining chunks after being erasure coded into stripes */
	int temp_chunk;
	int temp_stripe_id, temp_stripe_chunk_id;
	int num_smler_chnk;

	for(i=0; i<cur_rcd_idx; i++){

		temp_chunk=trace_access_pattern[i];

		// printf("temp_chunk=%d\n",temp_chunk);
		//find the maximum number of correlated data chunks that is smaller than temp_chunk
		//if it is a correlated data, then continue
		if(chunk_to_stripe_map[i]!=-1)
			continue;

		else {
			num_smler_chnk=find_max_crrltd_chnk_udr_bundry(erasure_k*crrltd_stripe, ognzd_crrltd_chnk, temp_chunk);
			//printf("temp_chunk=%d, num_crrltd_chnk_bfr=%d\n", temp_chunk, num_smler_chnk);
		}

		// then there are num_smler_chnk-1 chunks whose offsets are smaller than temp_chunk
		// note that the stripe_id should be larger than rele_chunk_stripe+extra_stripe_count;
		temp_stripe_id=(temp_chunk-num_smler_chnk)/(erasure_k)+crrltd_stripe+1;
		temp_stripe_chunk_id=(temp_chunk-num_smler_chnk)%erasure_k;

		// printf("chunk_id=%d: num_smler_chnk=%d, stripe_id=%d, stripe_chunk_id=%d\n", temp_chunk, num_smler_chnk, temp_stripe_id, temp_stripe_chunk_id);
		// record this stripe_id
		chunk_to_stripe_map[i]=temp_stripe_id;
		chunk_to_stripe_chunk_map[i]=temp_stripe_chunk_id;

		if(strcmp(code_type, "lrc")==0)
			chunk_to_local_group_map[i]=temp_stripe_chunk_id/num_chunk_per_lg;

	}



	//print the chunk_stripe_map 
	printf("chunk_to_stripe_map:\n");
	for(i=0; i<cur_rcd_idx; i++)
		printf("chunk_id=%d, chunk_to_stripe_map[%d]=%d\n", trace_access_pattern[i],  i, chunk_to_stripe_map[i]);
	printf("\n");

	printf("chunk_to_local_group_map:\n");
	for(i=0; i<cur_rcd_idx; i++)
		printf("chunk_id=%d, chunk_to_local_group_map[%d]=%d\n", trace_access_pattern[i], i, chunk_to_local_group_map[i]);
	printf("\n");


	free(if_select_correlated_chunks);
	free(temp_stripe);
	free(temp_stripe_idx_to_rele_chunk_table);
	free(crrltd_chnk_set);
	free(stripe_chnk_crrltn_dgr);
	free(stripe_chnk_idx_in_crrltn_set);
	free(ognzd_crrltd_chnk);

	printf("<=========== stripe_organization:\n");

}

void caso_stripe_ognztn(char *trace_name,  int *analyze_chunks_time_slots, int *num_chunk_per_timestamp, int bgn_tmstmp_num, int* sort_caso_rcd_pattern, 
		int* sort_caso_rcd_index, int* sort_caso_rcd_freq, int* chunk_to_stripe_map, int* chunk_to_stripe_chunk_map, int* chunk_to_local_group_map){

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
	int k;
	int p;
	int count_timestamp;
	int access_start_block, access_end_block;
	int cur_index;
	int if_begin;	 
	int bucket_num;
	int cell_num;
	int num_correlated_chunk;
	int flag, if_head_insert;
	int temp_chunk;
	int peer_count;
	int sort_index;
	int chunk_id;
	
	long long *size_int;
	long long *offset_int;
	long long a,b;
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;

	bucket_num=caso_rcd_idx/bucket_depth+1;
	cell_num=bucket_depth*bucket_num;
	//use a bucket to store the correlated chunks
	int* correlate_chunk_bucket=(int*)malloc(sizeof(int)*cell_num); //the number of correlated data chunks should be no larger than caso_rcd_idx
	memset(correlate_chunk_bucket, -1, sizeof(int)*cell_num);

	cur_index=0;
	count_timestamp=0;
	if_begin=1;

	for(i=0;i<bgn_tmstmp_num*max_access_chunks_per_timestamp;i++)
		analyze_chunks_time_slots[i]=0;

	for(i=0;i<bgn_tmstmp_num;i++)
		num_chunk_per_timestamp[i]=0;

	/* record the access chunks per timestamp in a table */
	while(fgets(operation, sizeof(operation), fp)){

		new_strtok(operation, divider, timestamp);
		new_strtok(operation, divider, op_type);
		new_strtok(operation, divider, offset);
		new_strtok(operation, divider, size);

		trnsfm_char_to_int(offset, offset_int);
		access_start_block=(*offset_int)/block_size;

		trnsfm_char_to_int(size, size_int);
		access_end_block=(*offset_int+*size_int-1)/block_size;

		// analyze the access pattern 
		// if it is accessed in the same timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

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

		//printf("timestamp=%s: access_start_block=%d, access_end_block=%d\n", timestamp, access_start_block, access_end_block);

		for(k=access_start_block; k<=access_end_block; k++){

			for(p=0; p<cur_index; p++)
				if(analyze_chunks_time_slots[count_timestamp*max_access_chunks_per_timestamp+p]==k)
					break;

			if(p<cur_index)
				continue;

			// check the freq of this chunk, if this chunk is accessed only once, then it must not be a correlated data chunk 
			sort_index=binary_search(sort_caso_rcd_pattern, caso_rcd_idx, k);

			if(sort_caso_rcd_freq[sort_index]<2)
				continue;

            // record the potential correlated data chunk
			analyze_chunks_time_slots[count_timestamp*max_access_chunks_per_timestamp+cur_index]=k;
			cur_index++;

			if(cur_index>=max_access_chunks_per_timestamp){

				printf("analyze_chunks_time_slots record error!\n");
				exit(1);

			}
		}
	}

#if debug
	printf("analyze_chunks_time_slots:\n");
	print_matrix(analyze_chunks_time_slots, max_access_chunks_per_timestamp, bgn_tmstmp_num);

	printf("analyze trace finishes\n");
	printf("caso_rcd_idx=%d, max_num_peer_chunks=%d\n", caso_rcd_idx, max_num_peer_chunks);
#endif

	// calculate the number of data chunks that are accessed more than twice in the caso analysis period
	int* caso_poten_crrltd_chks=(int*)malloc(sizeof(int)*caso_rcd_idx);
	
	poten_crrltd_cnt=0;
	for(i=0; i<caso_rcd_idx; i++){
		
		if(sort_caso_rcd_freq[i]>=2){
			caso_poten_crrltd_chks[poten_crrltd_cnt]=sort_caso_rcd_pattern[i];
			poten_crrltd_cnt++;
			}
		}

	printf("poten_crrltd_cnt=%d\n",poten_crrltd_cnt);

	int* rcd_peer_chks=(int*)malloc(poten_crrltd_cnt*sizeof(int)*max_num_peer_chunks);  // record the peer chunks with each potential correlated chunk in caso analysis
	int* freq_peer_chks=(int*)malloc(poten_crrltd_cnt*sizeof(int)*max_num_peer_chunks); // record the number of times that are accessed together 
	int* num_peer_chks=(int*)malloc(sizeof(int)*poten_crrltd_cnt); //it records the number of peer chunks per chunk

	memset(num_peer_chks, 0, sizeof(int)*poten_crrltd_cnt);

	record_access_freq(bgn_tmstmp_num, analyze_chunks_time_slots, caso_poten_crrltd_chks, 
			num_chunk_per_timestamp, rcd_peer_chks, freq_peer_chks, num_peer_chks);

	//calculate correlated chunk num
	num_correlated_chunk=0;
	peer_count=0;

	for(i=0; i<poten_crrltd_cnt; i++){

		flag=0;
		if_head_insert=0;

		for(j=1; j<max_num_peer_chunks; j++){

			if(freq_peer_chks[i*max_num_peer_chunks+j]>=2){

				//insert the chunk into the bucket
				temp_chunk=rcd_peer_chks[i*max_num_peer_chunks+j];
				insert_chunk_into_bucket(correlate_chunk_bucket, bucket_num, temp_chunk);
				flag=1;

			}

			if(freq_peer_chks[i*max_num_peer_chunks+j]>=1)
				peer_count++;

			//if all the peer chunks have been considered, then jump out of the loop
			if(peer_count==num_peer_chks[i])
				break;

		}

		if(flag==1){
			//if the first data chunk is not interted, then insert it 
			if(if_head_insert==0){
				insert_chunk_into_bucket(correlate_chunk_bucket, bucket_num, rcd_peer_chks[i*max_num_peer_chunks]);
				if_head_insert=1;
			}
		}

	}

	//count the correlated data chunks
	num_correlated_chunk=0;
	cell_num=bucket_depth*bucket_num;
	for(i=0; i<cell_num; i++)
		if(correlate_chunk_bucket[i]!=-1)
			num_correlated_chunk++;

	printf("----num_correlated_chunk=%d\n", num_correlated_chunk);

	//construct caso_crltd_mtrx, caso_crltd_dgr_mtrx
	int* caso_crltd_mtrx=(int*)malloc(sizeof(int)*num_correlated_chunk*max_num_relevent_chunks_per_chunk);
	int* caso_crltd_dgr_mtrix=(int*)malloc(sizeof(int)*num_correlated_chunk*max_num_relevent_chunks_per_chunk);
	int* crrltd_chnk_pttn_idx=(int*)malloc(sizeof(int)*num_correlated_chunk); //it records the index of the correlated chunk in the analyzed access patterns
	int num_crrltd_stripe;

	num_crrltd_stripe=num_correlated_chunk/erasure_k;
	int* ognzd_stripe_idx=(int*)malloc(sizeof(int)*num_crrltd_stripe*erasure_k); // it records the index of correlated chunks of every stripe in the correlated set 

	//extract the correlated data chunks and their degrees
	extract_caso_crltd_chnk_dgr(caso_crltd_mtrx, caso_crltd_dgr_mtrix, rcd_peer_chks, freq_peer_chks, num_correlated_chunk, num_peer_chks);

	// generate crrltd_chnk_pttn_idx 
	for(i=0; i<num_correlated_chunk; i++){

		chunk_id=caso_crltd_mtrx[i*max_num_relevent_chunks_per_chunk];
		sort_index=binary_search(sort_caso_rcd_pattern, caso_rcd_idx, chunk_id);
		crrltd_chnk_pttn_idx[i]=sort_caso_rcd_index[sort_index];

		}

	//stripe organization
	stripe_orgnzt(caso_crltd_mtrx, caso_crltd_dgr_mtrix, num_correlated_chunk, chunk_to_stripe_map, chunk_to_stripe_chunk_map, 
				  chunk_to_local_group_map, crrltd_chnk_pttn_idx);

	free(correlate_chunk_bucket);
	free(caso_crltd_mtrx);
	free(caso_crltd_dgr_mtrix);
	free(rcd_peer_chks);
	free(freq_peer_chks);
	free(crrltd_chnk_pttn_idx);
	free(num_peer_chks);
	free(ognzd_stripe_idx);
	free(caso_poten_crrltd_chks);

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

/*
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

int calculate_psw_io_caso_stripe(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map, int* chunk_to_local_group_map){

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
	int temp_stripe_id;
	int chunk_count;

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
	int stripe_count;

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
	stripe_count=0;

	int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe
	int *stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);

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
*/

// calculate the ones in the matrix
int calculate_chunk_num_io_matrix(int *io_matrix, int len, int width){

	int res=0;
	int i;


	for(i=0;i<len*width;i++)
		if(io_matrix[i]==1)
			res++;


	return res;

}

// @accessed_stripes: records the involved stripes in a timestamp
// @stripe_count: records the number of involved stripes in a timestamp
// @io_matrix: if a chunk is accessed, then the corresponding cell is marked as 1
/*
   void system_partial_stripe_writes(int *io_matrix, int *accessed_stripes, int stripe_count, int *total_write_block_num){

   int i,j;
   int io_amount;
   int io_index;
   int ret;
   int matrix_width;
   int pagesize;
   int fd_disk[15];
   int max_stripes;

   max_stripes=max_offset/(block_size*erasure_k);
//printf("max_stripes=%d\n", max_stripes);

pagesize=getpagesize();
matrix_width=erasure_k+erasure_m;

//count the io amount
io_amount=calculate_chunk_num_io_matrix(io_matrix, stripe_count, erasure_k+erasure_m);

 *total_write_block_num+=io_amount;

//record the name of disk array
char *disk_array[15]={"/dev/sde","/dev/sdf","/dev/sdg","/dev/sdh","/dev/sdi","/dev/sdj","/dev/sdk","/dev/sdl","/dev/sdm","/dev/sdn",
"/dev/sdo","/dev/sdp","/dev/sdq","/dev/sdr","/dev/sds"};


//record the start point and end point for each i/o
struct aiocb* aio_list = (struct aiocb*)malloc(sizeof(struct aiocb)*io_amount);

bzero(aio_list, sizeof (struct aiocb)*io_amount);

io_index=0;
srand((int)time(0)); 

for(i=0;i<matrix_width;i++){

fd_disk[i]=open64(disk_array[i], O_RDWR | O_DIRECT | O_SYNC);

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
aio_list[io_index].aio_offset=(accessed_stripes[j]%max_stripes*block_size);
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
	printf("aio_offset=%lld\n", aio_list[i].aio_offset);
	printf("%d-th aio_read_offset=%.2lfMB\n", i, aio_list[i].aio_offset*1.0/1024/1024);
	printf("aio_read_return error, ret=%d, io_no=%d\n",ret,i);
	printf("accessed_stripes:\n");
	print_matrix(accessed_stripes, stripe_count, 1);
}

}

int *encoding_matrix;
char **data, **coding;

encoding_matrix = talloc(int, erasure_m*erasure_k);
for (i = 0; i < erasure_m; i++) {
	for (j = 0; j < erasure_k; j++) {
		encoding_matrix[i*erasure_k+j] = galois_single_divide(1, i ^ (erasure_m + j), erasure_w);
	}
}

jerasure_matrix_encode(erasure_k, erasure_m, erasure_w, encoding_matrix, data, coding, block_size);


// perform the write operations 

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

//free the aio structure 
free(aio_list);

for(i=0;i<matrix_width;i++)
close(fd_disk[i]);


}
*/

int psw_time_caso(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map, int *chunk_to_stripe_chunk_map, 
						double *time, int* chunk_to_local_group_map){

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
	char pre_timestamp[100];

	int i,j;
	int k;	
	int access_start_block, access_end_block;
	int count;
	int flag;
	int io_count;
	int temp_stripe_id, temp_chunk_id;
	
	long long *size_int;
	long long *offset_int;
	long long a,b;
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;

	count=0;
	flag=0;
	io_count=0;

	int max_accessed_stripes=max_access_chunks_per_timestamp; // suppose the maximum accessed stripes is 100
	int* stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);
	int* lg_per_tmstmp=(int*)malloc(sizeof(int)*max_accessed_stripes*lrc_lg); 
	int* io_request=(int*)malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp

	// initialize the io_request
	memset(io_request, 0, sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m));

	int stripe_count;
	int write_count;
	int rotation;
	int sort_index, real_index;
	int lg_id, lg_count=0;

	int *total_caso_io;
	int c=0;
	total_caso_io=&c;

	stripe_count=0;
	write_count=0;

	//init the pre_timestamp as the given timestamp
	strcpy(pre_timestamp, given_timestamp);

	struct timeval begin_time, end_time;

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

			if(strcmp(code_type, "lrc")==0)
				io_count+=lg_count*lg_prty_num;

			// perform system write
			gettimeofday(&begin_time, NULL);
			//system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
			gettimeofday(&end_time, NULL);
			*time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

			// re-initialize the io_request array
			memset(io_request, 0, sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m));
			memset(stripes_per_timestamp, -1, sizeof(int)*max_accessed_stripes);
			memset(lg_per_tmstmp, 0, sizeof(int)*max_accessed_stripes*lrc_lg);

			if(stripe_count>=max_accessed_stripes){
				printf("ERR: max_accessed_stripes error!\n");
				exit(1);
			}

			// re-initiate the stripe_count
			stripe_count=0;
			lg_count=0;

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
			}

			//check the local group
			if(strcmp(code_type, "lrc")==0){

				lg_id=chunk_to_local_group_map[real_index];
				
				if(lg_per_tmstmp[j*lrc_lg+lg_id]==0){
				
					lg_per_tmstmp[j*lrc_lg+lg_id]=1;
					lg_count++;
				
					}
				}

            // mark the updated data chunk and the corresponding parity chunks
			io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=1;

			for(k=erasure_k; k<(erasure_k+erasure_m); k++)
				io_request[j*(erasure_k+erasure_m)+(k+rotation)%(erasure_k+erasure_m)]=1;
	

		}

	}

	//for the last operation, add the parity update io
	gettimeofday(&begin_time, NULL);
	//system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
	gettimeofday(&end_time, NULL);
	*time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

	write_count++;

	io_count+=stripe_count*erasure_m; 
	io_count+=lg_count*lg_prty_num;

	printf("write_count=%d\n", write_count);
	printf("caso_parity_io=%d, total_io_num=%d\n", io_count, *total_caso_io);


	fclose(fp);
	free(stripes_per_timestamp);
	free(io_request);
	free(lg_per_tmstmp);

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
	char pre_timestamp[100];


	int i,j;
	int k;
	int access_start_block, access_end_block;
	int count;
	int flag;
	int io_count;
	int start_stripe, end_stripe;
	int temp_stripe_id, temp_chunk_id;
	int stripe_count;
	int rotation;
	int *total_write_block_num; 
	int c=0;

	long long *size_int;
	long long *offset_int;
	long long a,b;
	
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;
	count=0;
	flag=0;
	io_count=0;

	int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe
	int *stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);
	int *io_request=(int*)malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp
	int* lg_per_tmstmp=(int*)malloc(sizeof(int)*max_accessed_stripes*lrc_lg); 

	int write_count;
	int lg_count=0, lg_id;
	int num_chnk_per_lg; 

	// initialize the io_request
	// if a chunk is requested in the s_i-th stripe, then the corresponding cell is marked with 1
	for(i=0;i<max_accessed_stripes*(erasure_k+erasure_m);i++)
		io_request[i]=0;

	stripe_count=0;
	total_write_block_num=&c;
	write_count=0;
	num_chnk_per_lg=erasure_k/lrc_lg;
	
	//init the pre_timestamp as the given timestamp
	strcpy(pre_timestamp, given_timestamp);

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

		// calculate the numeber of timestamp
		if(strcmp(pre_timestamp,timestamp)!=0){

			// calculate the partial stripe writes in the last timestamp
			io_count+=stripe_count*erasure_m;

			if(strcmp(code_type, "lrc")==0)
				io_count+=lg_count*lg_prty_num;

			// perform the system write
			gettimeofday(&begin_time, NULL);
			//system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
			gettimeofday(&end_time, NULL);
			*time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

			// summarize the access in the last timestamp
			//if(count>=1)
			//printf("timestamp=%s, stripe_count=%d\n\n",pre_timestamp,stripe_count);

			// list the new access in current timestamp
			//printf("timestamp=%s, start_stripe=%d, end_stripe=%d\n", timestamp, start_stripe, end_stripe);

			// re-initiate the stripes_per_timestamp
			memset(io_request, 0, max_accessed_stripes*(erasure_k+erasure_m));
			memset(stripes_per_timestamp, -1, sizeof(int)*max_accessed_stripes);
			memset(lg_per_tmstmp, 0, sizeof(int)*max_accessed_stripes*lrc_lg);

			// record the current involved stripes in this operation
			stripe_count=0;
			lg_count=0; 

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

			// record the updated info of local group

            if(strcmp(code_type, "lrc")==0){

				lg_id=temp_chunk_id/num_chnk_per_lg;

				if(lg_per_tmstmp[j*lrc_lg+lg_id]==0){
				
								lg_per_tmstmp[j*lrc_lg+lg_id]=1;
								lg_count++;
				
								}

            	}

			// update the io_request_matrix
			io_request[j*(erasure_k+erasure_m)+(temp_chunk_id+rotation)%(erasure_k+erasure_m)]=1;

			for(k=erasure_k; k< (erasure_k+erasure_m); k++)
				io_request[j*(erasure_k+erasure_m)+(k+rotation)%(erasure_k+erasure_m)]=1;


		}


	}

	gettimeofday(&begin_time, NULL);
	//system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
	gettimeofday(&end_time, NULL);
	*time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

	write_count++;
	// for the last operation
	io_count+=stripe_count*erasure_m; 
	io_count+=lg_count*lg_prty_num;
	
	printf("num_of_write_op=%d, total_write_block_num=%d\n", write_count, *total_write_block_num);
	printf("psw_striping_io_count=%d\n", io_count);
	
	//printf("timestamp=%s, stripe_count=%d\n\n",pre_timestamp,stripe_count);

	fclose(fp);
	free(stripes_per_timestamp);
	free(io_request);
	free(lg_per_tmstmp); 

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
	char pre_timestamp[100];

	int i,j;
	int k;
	int access_start_block, access_end_block;
	int count;
	int flag;
	int temp_stripe;
	int io_count;
	int temp_group_id, temp_group_block_id;
	int temp_row_id;
	int stripe_count;
	int temp_chunk_id;
	int lg_id, lg_count=0;
	int num_chnk_per_lg;

	num_chnk_per_lg=erasure_k/lrc_lg;

	long long *size_int;
	long long *offset_int;
	long long a,b;
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;

	count=0;
	flag=0;
	io_count=0;

	int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe

	int *stripes_per_timestamp=malloc(sizeof(int)*max_accessed_stripes);
	int* lg_per_tmstmp=(int*)malloc(sizeof(int)*max_accessed_stripes*lrc_lg); 
	int *io_request=malloc(sizeof(int)*max_accessed_stripes*(erasure_k+erasure_m)); // it records the io request in a timestamp
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

			if(strcmp(code_type, "lrc")==0)
				io_count+=lg_count*lg_prty_num;

			//printf("timestamp=%s, stripe_count=%d\n",pre_timestamp,stripe_count);

			gettimeofday(&begin_time, NULL);
			//system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
			gettimeofday(&end_time, NULL);
			*time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

			// record the current involved stripes in this operation
			memset(io_request, 0, max_accessed_stripes*(erasure_k+erasure_m));
			memset(stripes_per_timestamp, -1, sizeof(int)*max_accessed_stripes);
			memset(lg_per_tmstmp, 0, sizeof(int)*max_accessed_stripes*lrc_lg);

			// record the current involved stripes in this operation
			stripe_count=0;
			lg_count=0;

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

            if(strcmp(code_type, "lrc")==0){

				lg_id=temp_chunk_id/num_chnk_per_lg;

				if(lg_per_tmstmp[j*lrc_lg+lg_id]==0){
				
								lg_per_tmstmp[j*lrc_lg+lg_id]=1;
								lg_count++;
				
								}

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
	//system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count, total_write_block_num);
	gettimeofday(&end_time, NULL);
	*time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;	 
	io_count+=stripe_count*erasure_m;

	printf("psw_time_continugous_io_count=%d\n", io_count);


	fclose(fp);
	free(stripes_per_timestamp);
	free(io_request);
	free(lg_per_tmstmp);

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
/*
   void system_parallel_reads(int *io_matrix, int *accessed_stripes, int stripe_count, int *total_write_block_num){

   int i,j;
   int io_amount;
   int io_index;
   int ret;
   int matrix_width;
   int pagesize;
   int fd_disk[15];
   int max_stripes;

   max_stripes=max_offset/(block_size*erasure_k);
//printf("max_stripes=%d\n", max_stripes);

pagesize=getpagesize();
matrix_width=erasure_k+erasure_m;

//count the io amount
io_amount=calculate_chunk_num_io_matrix(io_matrix, stripe_count, erasure_k+erasure_m);

 *total_write_block_num+=io_amount;

//record the name of disk array
char *disk_array[15]={"/dev/sde","/dev/sdf","/dev/sdg","/dev/sdh","/dev/sdi","/dev/sdj","/dev/sdk","/dev/sdl","/dev/sdm","/dev/sdn",
"/dev/sdo","/dev/sdp","/dev/sdq","/dev/sdr","/dev/sds"};

//record the start point and end point for each i/o
struct aiocb *aio_list = (struct aiocb *)malloc(sizeof(struct aiocb)*io_amount);

bzero(aio_list, sizeof (struct aiocb)*io_amount);

io_index=0;

for(i=0;i<matrix_width;i++){

fd_disk[i]=open64(disk_array[i], O_RDWR | O_DIRECT);

if(fd_disk[i]<0)
printf("openfile failed, i=%d\n",i);

for(j=0; j<stripe_count; j++){

// if there is an io request, then initiate the aio_structure
if(io_matrix[j*matrix_width+i]==1){

aio_list[io_index].aio_fildes=fd_disk[i];
aio_list[io_index].aio_nbytes=block_size;
// make sure that the offset should not exceed the disk capacity. 
aio_list[io_index].aio_offset=accessed_stripes[j]%max_stripes*block_size;
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


free(aio_list);

for(i=0;i<matrix_width;i++)
close(fd_disk[i]);


}
*/

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
	char pre_timestamp[100];

	int i,j;
	int failed_disk;
	int access_start_block, access_end_block;
	int count;
	int io_count;
	int temp_stripe_id, temp_chunk_id;

	long long *size_int;
	long long *offset_int;
	long long a,b;
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;
	count=0;
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

				if_dr=degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 0);

				// update the total number of io requests
				io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

				if(if_dr==1){

					gettimeofday(&begin_time, NULL);

					//for(i=0; i<50; i++)
					//system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);

					gettimeofday(&end_time, NULL);
					*time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;

				}

				// re-initialize the io_request array
				for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
					io_request[i]=0;

				// re-initiate the stripe_count
				stripe_count=0;

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

			//for(i=0; i<50; i++)
			//system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);

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
	int count;
	int io_count;
	int temp_stripe_id, temp_chunk_id;
	int if_dr;

	long long *size_int;
	long long *offset_int;
	long long a,b;
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;

	count=0;
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

				// perform degraded reads
				if_dr=degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 0);

				//update the total number of io requests
				io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

				// perform system write
				if(if_dr==1){

					gettimeofday(&begin_time, NULL);

					//for(i=0; i<50; i++)
					//system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);

					gettimeofday(&end_time, NULL);
					*time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;

				}


				// re-initialize the io_request array
				for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
					io_request[i]=0;

				// re-initiate the stripe_count
				stripe_count=0;

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

			//for(i=0; i<50; i++)
			//system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);

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
	int count;
	int io_count;
	int temp_group_id, temp_stripe, temp_group_block_id;
	int temp_row_id;
	int group_num_blocks=contiguous_block*erasure_k;
	int stripe_count;
	int temp_contiguous_block;
	int temp_chunk_id;

	long long *size_int;
	long long *offset_int;
	long long a,b;
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;
	count=0;
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

				// perform degraded reads
				degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_extra_io, 1);

				// update the total number of io requests
				io_count+=calculate_num_io(io_request, stripe_count, erasure_k+erasure_m);

				// perform system write
				gettimeofday(&begin_time, NULL);
				//for(i=0; i<50; i++)
				//system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
				gettimeofday(&end_time, NULL);
				*time+=(end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000)/50;


				// re-initialize the io_request array
				for(i=0; i<max_accessed_stripes*(erasure_k+erasure_m); i++)
					io_request[i]=0;

				// re-initiate the stripe_count
				stripe_count=0;

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
		//for(i=0; i<50; i++){
		//system_parallel_reads(io_request, stripes_per_timestamp, stripe_count, total_caso_io);
		//}
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

