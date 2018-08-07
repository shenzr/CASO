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
#include <assert.h>
#include <stdint.h>

#include "Jerasure/jerasure.h"
#include "Jerasure/galois.h"
#include "Jerasure/cauchy.h"
#include "Jerasure/reed_sol.h"
#include "config.h"




/* ====== Fill the information of the files on different disks for testbed evaluations ======*/
char *disk_array[num_disks]={"/dev/sde","/dev/sdf","/dev/sdg","/dev/sdh","/dev/sdi",
							 "/dev/sdj","/dev/sdk","/dev/sdl","/dev/sdm","/dev/sdn",
							 "/dev/sdo","/dev/sdp","/dev/sdq","/dev/sdr", "/dev/sds"};
/* ==========================================================================================*/





#define talloc(type, num) (type *) malloc(sizeof(type)*(num))
#define disk_capacity 299 // GB
#define clean_cache_blk_num 1024*25 // the number of blocks read for cleaning cache in memory

int num_distinct_chunks_timestamp[num_assume_timestamp];
int poten_crrltd_cnt;


/* This function prints all the elements of a matrix */
void print_matrix(int* matrix, int len, int width){

    int i,j; 
    for(i=0; i<width; i++){

        for(j=0; j<len; j++)
            printf("%d ", matrix[i*len+j]);

        printf("\n");

    }
}

void print_chunk_info(CHUNK_INFO* chunk_info){

    printf("chunk_info:\n");
    printf("chunk_info->logical_chunk_id = %d\n", chunk_info->logical_chunk_id);
    printf("chunk_info->stripe_id = %d\n", chunk_info->stripe_id);
    printf("chunk_info->chunk_id_in_stripe = %d\n", chunk_info->chunk_id_in_stripe);
    printf("chunk_info->lg_id = %d\n", chunk_info->lg_id);
    printf("\n");

}


/* generate a random string with the input size */
void gene_radm_buff(char* buff, int len){

    int i;

    char alphanum[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    for(i=0; i<len; i++)
        buff[i]=alphanum[i%(sizeof(alphanum)-1)];

}

/* encode the data by calling the arthmetic of the galois field */
void encode_data(char* data, char* pse_coded_data, int ecd_cef){

    galois_w08_region_multiply(data, ecd_cef, block_size, pse_coded_data, 0);

}


/* XOR two regions into one region */
void cal_delta(char* result, char* srcA, char* srcB, int length) {

    int i;
    int XorCount = length / sizeof(long);

    uint64_t* srcA64 = (uint64_t*) srcA;
    uint64_t* srcB64 = (uint64_t*) srcB;
    uint64_t* result64 = (uint64_t*) result;

    // finish all the word-by-word XOR
    for (i = 0; i < XorCount; i++) {
        result64[i] = srcA64[i] ^ srcB64[i];
    }

}


/* agggregate multiple data/delta chunks into one data/delta chunk */
void aggregate_data(char** coding_delta, int updt_chnk_cnt, char* final_result){

    char* mid_result=NULL;
    char* temp;
    int i;

    for(i=0; i<updt_chnk_cnt; i++){

        if(i==0){
            mid_result=coding_delta[i];
            continue;
        }

        cal_delta(final_result, mid_result, coding_delta[i], block_size);

        temp=mid_result;
        mid_result=final_result;
        final_result=temp;

    }

    // the final data is stored in mid_result
    final_result=mid_result;

}

/* process the timestamp based on the given time distance */
void process_timestamp(char* input_timestamp, char* output_timestamp){

    int i;
    int len;

    len=0;
    while(input_timestamp[len]!='\0')
        len++;

    for(i=0; i<len-tm_dstnc_odr; i++)
        output_timestamp[i]=input_timestamp[i];

    for(i=len-tm_dstnc_odr; i<len; i++)
        output_timestamp[i]='0';

    output_timestamp[len]='\0';

}

/* transform a char type to a int type (usually for determining the number of operated chunks and the stripe information) */
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

/* add all the requests of the io matrix */
int calculate_num_io(int *io_request, int col, int width){

    int num=0;
    int i;

    for(i=0; i<col*width; i++)
        if(io_request[i]>0)
            num++;

    return num;

}

/* calculate the number of additional io requests in degraded reads */
int calculate_extra_io(int *io_request, int col, int width){

    int num=0;
    int i;

    for(i=0; i<col*width; i++)
        if(io_request[i]==2)
            num++;

    return num;

}



/* check if a chunk is recorded, else insert the chunk */
int check_if_in_access_bucket(int chunk_id){

    int bucket_width;
    int bucket_id;

    bucket_width=max_aces_blk_num/bucket_depth;

    bucket_id=chunk_id%bucket_width;

    // check if the chunk_id is in the bucket 
    int i;
    int addr;
    for(i=0; i<bucket_depth; i++){

        addr=bucket_id*bucket_depth+i;

        // if the chunk is recorded
        if(access_bucket[addr]==chunk_id)
            break;

        // if the chunk is not recorded
        if(access_bucket[addr]==-1)
            break;
    }

    if(i>=bucket_depth){

        printf("ERR: bucket_depth is exhausted\n");
        exit(1);

    }

    if(access_bucket[addr]==chunk_id)
        return order_access_bucket[addr];

    else{

        //record the chunk and its access order 
        access_bucket[addr]=chunk_id;
        order_access_bucket[addr]=cur_rcd_idx;

        return -1;

    }

}

/* find a value in an array sorted in descending order */
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

/* sort the array and move the corresponding index to the new position */
void QuickSort_index(int data[], int index[],int left, int right){
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

    if(p - left > 1)
        QuickSort_index(data, index, left, p-1);
    if(right - p > 1)
        QuickSort_index(data, index, p+1, right);

}

/* truncate a component from a string according to a given divider */
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


/* calculate the number of chunks written in a trace and record some global variables */
void calculate_chunk_num(char *trace_name){

    total_num_req=0;

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("ERR: open file error!\n");
        exit(1);
    }

    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char pre_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
    char usetime[10];
    char divider=',';

    int i;
    int access_start_block, access_end_block;
    int num_distict_chunks_per_timestamp;
    int count;
    int temp_count;
    int if_begin;
    int ret;
	int write_flag;

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
	write_flag=0;

    // the max_access_chunks_per_timestamp is not the exact value, it did not consider the multiple accesses 
    // to a chunk within a timestamp
    max_access_chunks_per_timestamp=-1;
    cur_rcd_idx=0;

    memset(num_distinct_chunks_timestamp, 0, sizeof(int)*num_assume_timestamp);

    while(fgets(operation, sizeof(operation), fp)) {

        count++;
        total_num_req++;

        // break the operation
        new_strtok(operation,divider,orig_timestamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);
        new_strtok(operation,divider,offset);
        new_strtok(operation,divider,size);
        new_strtok(operation,divider,usetime);

        if(strcmp(op_type, "Read")==0){

            // transform the offset and size
            trnsfm_char_to_int(offset, offset_int);
            trnsfm_char_to_int(size, size_int);

            // update the max_access_chunks_per_timestamp to avoid segmentation fault in next degraded read test 
            access_start_block=(*offset_int)/block_size;
            access_end_block=(*offset_int+*size_int-1)/block_size; 

            if(access_end_block - access_start_block +1 > max_access_chunks_per_timestamp)
                max_access_chunks_per_timestamp=access_end_block-access_start_block+1;

            continue;

        }

        // process timestamp, offset, and operated size
        process_timestamp(orig_timestamp, round_timestamp);
        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

        access_start_block=(*offset_int)/block_size;
        access_end_block=(*offset_int+*size_int-1)/block_size;
        total_access_chunk_num += access_end_block - access_start_block + 1;

		// update the flag indicating that the time distance has write requests
		write_flag=1;

        //if it is the begining of the first pattern of a time distance, then collect the info of the last timestamp
        if(strcmp(pre_timestamp, round_timestamp)!=0){

            //if it is a new timestamp, then copy it to the pre_timestamp
            strcpy(pre_timestamp, round_timestamp);

            //if it is not the first timestamp, then initialize parameters
            if(if_begin==0){

                num_distinct_chunks_timestamp[num_timestamp]=num_distict_chunks_per_timestamp;
                num_distict_chunks_per_timestamp=0;

                //record the maximum number of accessed chunks in a timestamp
                if(temp_count>max_access_chunks_per_timestamp)
                    max_access_chunks_per_timestamp=temp_count;

                num_timestamp++;
				write_flag=0;

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

        // scan the written blocks in the request
        for(i=access_start_block; i<=access_end_block; i++){

            ret=check_if_in_access_bucket(i);
            // if the chunk is not recorded before
            if(ret==-1){

                //increase the access freq
                freq_access_chunk[cur_rcd_idx]++;
                trace_access_pattern[cur_rcd_idx]=i;
                cur_rcd_idx++;
                num_distict_chunks_per_timestamp++;

            }

            // if the chunk is recorded before, then only increase its access frequency
            else if(ret!=-1)
                freq_access_chunk[ret]++;

            if(cur_rcd_idx>=max_aces_blk_num){

                printf("ERR: cur_rcd_idx>=max_aces_blk_num\n");
                exit(1);

            }

        }
    }

    // for the access pattern in the last timestamp, record the information 
    if(temp_count>max_access_chunks_per_timestamp)
        max_access_chunks_per_timestamp=temp_count;

    num_distinct_chunks_timestamp[num_timestamp]=num_distict_chunks_per_timestamp;

	// if the last time distance has write update operations 
	if(write_flag==1)
		num_timestamp++;

    fclose(fp);

}

/* determine the begin timestamp value for replaying the trace */
void determine_begin_timestamp(char *trace_name, char begin_timestamp[], int begin_timestamp_num){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(1);
    }

    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char pre_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char divider=',';

    int temp_count=0;
    int if_begin=1;
	int write_flag;

    caso_rcd_idx=0;
	write_flag=1;

    // read the trace 
    while(fgets(operation, sizeof(operation), fp)) {

        new_strtok(operation,divider,orig_timestamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);

        if(strcmp(op_type, "Read")==0)
            continue;

        process_timestamp(orig_timestamp, round_timestamp);
		write_flag=1;

        // calculate the numeber of timestamp
        if(strcmp(pre_timestamp, round_timestamp)!=0){

            if(if_begin==0){

                caso_rcd_idx+=num_distinct_chunks_timestamp[temp_count];
                temp_count++;
				write_flag=0;
				
            }

            else
				if_begin=0;

            strcpy(pre_timestamp, round_timestamp);

            // check if it reaches the number of analyzed time distance
		    if(temp_count==begin_timestamp_num){
		
			    strcpy(begin_timestamp, round_timestamp);
			    break;
		
		    }
        }
    }

	// for the timestamp in the last access pattern 
	if(write_flag==1){
		
		temp_count++;

		if(temp_count==begin_timestamp_num)
			strcpy(begin_timestamp, round_timestamp);
			
		}

    fclose(fp);

}

/* calculates the number of accessed chunks analyzed in CASO */
void calculate_caso_chunk_num(char *trace_name, char begin_timestamp[]){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(1);
    }

    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char pre_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char divider=',';

    int count;
    int if_begin;

    count=0;
    if_begin=1;

    caso_rcd_idx=0;

    // read the trace
    while(fgets(operation, sizeof(operation), fp)) {

        new_strtok(operation,divider,orig_timestamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);

        // we concern the optimization of write operations 
        if(strcmp(op_type, "Read")==0)
            continue;

        process_timestamp(orig_timestamp, round_timestamp);

        //if it is the beginning of the trace
        if(if_begin==1){

            if_begin=0;
            strcpy(pre_timestamp, round_timestamp);

        }

        if(strcmp(pre_timestamp, round_timestamp)!=0){

            // update caso_rcd_idx by adding the number of distinct chunks written in the timestamp value 
            if(strcmp(begin_timestamp, round_timestamp)!=0)
                caso_rcd_idx+=num_distinct_chunks_timestamp[count];

            else break;

            strcpy(pre_timestamp, round_timestamp);
            count++;

        }
    }

    fclose(fp);

}


/* insert the metadata information of a chunk into a bucket for next searches */
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
#if debug
    if(i>=bucket_depth){
        printf("bucket-%d is full!\n", bucket_id);
        exit(1);
    }
#endif

}


/* due to the limits of memory size, we cannot record all the peer chunks for every chunk. We propose to replace the old peer chunks by the new peer chunks once the 
   number of peer chunks of a chunk reaches the maximum value allowed in the analysis */
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

    //we replace the chunk according to the access time 
    //we assume that an old access chunk that are not correlated will not be a correlated chunk any more
    for(k=start_posi; k<max_num_peer_chunks; k++){

        if(freq_peer_chks[given_chunk_idx*max_num_peer_chunks+k]<3){

            rplcd_chk_id=rcd_peer_chks[given_chunk_idx*max_num_peer_chunks+k];
            rcd_peer_chks[given_chunk_idx*max_num_peer_chunks+k]=peer_chunk;
            freq_peer_chks[given_chunk_idx*max_num_peer_chunks+k]=1;

            break;
        }
    }

    //update the start_search_posi
    start_search_posi[given_chunk_idx]=k;

    if(k>=max_num_peer_chunks){

        //printf("ERR: cannot find a replaced chunk!\n");
        return; 
        //exit(1);

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

/* record the access frequency of a chunk */
void record_access_freq(int bgn_tmstmp_num, int* analyze_chunks_time_slots, int* caso_poten_crrltd_chks, 
        int* num_chunk_per_timestamp, int* rcd_peer_chks, int* freq_peer_chks, int* num_peer_chks){

    int i,j; 
    int chk_idx1, chk_idx2;
    int chunk_id1, chunk_id2;
    int sort_chk_idx1, sort_chk_idx2;   

    int* start_search_posi=(int*)malloc(sizeof(int)*poten_crrltd_cnt); //it records the position to start search for replacement, in order to save search time

    memset(start_search_posi, 1, sizeof(int)*poten_crrltd_cnt);
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

                if(chunk_id1==chunk_id2)
                    continue;

                sort_chk_idx1=binary_search(caso_poten_crrltd_chks, poten_crrltd_cnt, chunk_id1);
                sort_chk_idx2=binary_search(caso_poten_crrltd_chks, poten_crrltd_cnt, chunk_id2);

                if(sort_chk_idx1>=poten_crrltd_cnt || sort_chk_idx2>=poten_crrltd_cnt){

                    printf("ERR: sort_chk_idx1=%d, sort_chk_idx2=%d, poten_crrltd_cnt=%d\n", sort_chk_idx1, sort_chk_idx2, poten_crrltd_cnt);
                    exit(1);

                }

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

/* extract the correlated data chunks and their degrees of correlations, for serving the next stripe organization */
void extract_caso_crltd_chnk_dgr(int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix, int* rcd_peer_chks, int* freq_peer_chks, 
                                             int num_correlate_chunk, int* num_peer_chunk){

    int i,j;
    int crltd_chunk_cnt;
    int its_crltd_chunk_cnt;
    int if_crrltd_exist;
    int peer_count;

    memset(caso_crltd_mtrx, -1, sizeof(int)*num_correlate_chunk*max_num_correlated_chunks_per_chunk);
    memset(caso_crltd_dgr_mtrix, -1, sizeof(int)*num_correlate_chunk*max_num_correlated_chunks_per_chunk);

    crltd_chunk_cnt=0;
    peer_count=0;

    for(i=0; i<poten_crrltd_cnt; i++){

        its_crltd_chunk_cnt=1;
        if_crrltd_exist=0;
        peer_count=0;

        for(j=1; j<max_num_peer_chunks; j++){

            //if the chunk with index i and that with index j are correlated, then record them
            if(freq_peer_chks[i*max_num_peer_chunks+j]>=2){

                //record its correlated chunks and its degree
                caso_crltd_mtrx[crltd_chunk_cnt*max_num_correlated_chunks_per_chunk+its_crltd_chunk_cnt]=rcd_peer_chks[i*max_num_peer_chunks+j];
                caso_crltd_dgr_mtrix[crltd_chunk_cnt*max_num_correlated_chunks_per_chunk+its_crltd_chunk_cnt]=freq_peer_chks[i*max_num_peer_chunks+j];

                its_crltd_chunk_cnt++;
                if_crrltd_exist=1;

                if(its_crltd_chunk_cnt>=max_num_correlated_chunks_per_chunk){

                    printf("ERR: its_crltd_chunk_cnt>=max_num_correlated_chunks_per_chunk\n");
                    exit(1);

                }

            }

            if(freq_peer_chks[i*max_num_peer_chunks+j]>=1)
                peer_count++;

            if(peer_count==num_peer_chunk[i]){

                //we mark the correlated chunk finally
                if(if_crrltd_exist==1){

                    caso_crltd_mtrx[crltd_chunk_cnt*max_num_correlated_chunks_per_chunk]=rcd_peer_chks[i*max_num_peer_chunks];
                    crltd_chunk_cnt++;

                }

                break;
            }
        }
    }
}

/* find the maximum number of correlated chunks which are smaller than the given chunk */ 
void find_max_crrltd_chnk_udr_bundry(int chunk_id, SEARCH_INFO* si){

    int start,end,mid;

    start=0;
    end=ognz_crrltd_cnt-1;
    mid=start+(end-start)/2;

    // if there is no correlated chunks
	if(start > end){

		si->search_flag=0;
		si->num_crrltd_chnk_bf_chnk_id=0;

		return;
		
		}

    // if the smallest correlated data chunk is larger than the chunk_id
    if(sort_ognzd_crrltd_chnk[ognz_crrltd_cnt-1] > chunk_id){

        si->search_flag=0; 
        si->num_crrltd_chnk_bf_chnk_id=0;

        return;

    }

    if(sort_ognzd_crrltd_chnk[0] < chunk_id){

        si->search_flag=0;
        si->num_crrltd_chnk_bf_chnk_id=ognz_crrltd_cnt-1;

        return;

    }

    if(sort_ognzd_crrltd_chnk[start] == chunk_id || sort_ognzd_crrltd_chnk[end] == chunk_id){

        si->search_flag=1;
        si->num_crrltd_chnk_bf_chnk_id=-1;

        return;

    }

    // search the chunk by using binary search
    while(1){

        if(sort_ognzd_crrltd_chnk[mid] > chunk_id)
            start=mid;

        else if(sort_ognzd_crrltd_chnk[mid] < chunk_id)
            end=mid;

        else if(sort_ognzd_crrltd_chnk[mid] == chunk_id)
            break;

        mid=start+(end-start)/2;

        if(start==mid) 
            break;

    }

    if(sort_ognzd_crrltd_chnk[mid] == chunk_id){

        si->search_flag=1;
        si->num_crrltd_chnk_bf_chnk_id=-1;

    }

    else if(sort_ognzd_crrltd_chnk[start] > chunk_id){

        si->search_flag=0;
        si->num_crrltd_chnk_bf_chnk_id=ognz_crrltd_cnt-1-start;

    }

    else {

        printf("ERR: search info\n");
        exit(1);

    }
}


/* calculate the priority of the correlated  chunks in stripe organization */
int calculate_priority(int *stripe_map, int cur_chunk_num, int *temp_stripe_idx_in_rele_chunk_table, int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix,  
                              int candidate_chunk_index){

    int i;
    int count;
    int slct_chnk;
    int k;

    count=0;

    for(i=0;i<cur_chunk_num;i++){

        slct_chnk=caso_crltd_mtrx[temp_stripe_idx_in_rele_chunk_table[i]*max_num_correlated_chunks_per_chunk];

        // accumulate the priority
        for(k=1; k<max_num_correlated_chunks_per_chunk; k++){

            if(caso_crltd_mtrx[candidate_chunk_index*max_num_correlated_chunks_per_chunk+k]==slct_chnk)
                break;

            if(caso_crltd_mtrx[candidate_chunk_index*max_num_correlated_chunks_per_chunk+k]==-1)
                break;

        }

        if(caso_crltd_mtrx[candidate_chunk_index*max_num_correlated_chunks_per_chunk+k]==slct_chnk)
            count+=caso_crltd_dgr_mtrix[candidate_chunk_index*max_num_correlated_chunks_per_chunk+k];

    }

    return count;

}



/* update the correlation degree among the chunks organized in the same stripe */
void updt_stripe_chnk_crrltn_dgr(int *stripe_map, int cur_chunk_num, int *temp_stripe_idx_in_rele_chunk_table, int* caso_crltd_mtrx, 
        int* caso_crltd_dgr_mtrix,  int slct_chunk_index, int* stripe_chnk_crrltn_dgr){

    int i;
    int slct_chnk;
    int k;

    for(i=0;i<cur_chunk_num;i++){

        slct_chnk=caso_crltd_mtrx[temp_stripe_idx_in_rele_chunk_table[i]*max_num_correlated_chunks_per_chunk];

        // record the priority
        for(k=1; k<max_num_correlated_chunks_per_chunk; k++){

            if(caso_crltd_mtrx[slct_chunk_index*max_num_correlated_chunks_per_chunk+k]==slct_chnk)
                break;

            if(caso_crltd_mtrx[slct_chunk_index*max_num_correlated_chunks_per_chunk+k]==-1)
                break;

        }

        if(caso_crltd_mtrx[slct_chunk_index*max_num_correlated_chunks_per_chunk+k]==slct_chnk)
            stripe_chnk_crrltn_dgr[cur_chunk_num*(erasure_k+1)+1+i]=caso_crltd_dgr_mtrix[slct_chunk_index*max_num_correlated_chunks_per_chunk+k];

    }

}

/* organize the local groups */
void lrc_local_group_orgnzt(int* stripe_chnk_crrltn_dgr, int* stripe_chnk_idx_in_crrltn_set, int* crrltd_chnk_pttn_idx, int stripe_id){

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

    fir_idx=-1;
    sec_idx=-1;
    num_chunk_per_lg=erasure_k/num_lg;

    int* if_select=(int*)malloc(sizeof(int)*erasure_k);
    int* slct_lg_chnk_idx=(int*)malloc(sizeof(int)*num_chunk_per_lg);

    memset(if_select, 0, sizeof(int)*erasure_k);

    for(i=0; i<num_lg; i++){

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

            // update the local group 
            ognzd_crrltd_chnk_lg[stripe_id*erasure_k+stripe_chunk_idx]=i;

            // update the chunk id in lrc stripe
            ognzd_crrltd_chnk_id_stripe[stripe_id*erasure_k+stripe_chunk_idx]=i*num_chunk_per_lg+j;

        }

#if debug
        printf("%d-th local group:\n", i);

        printf("ognzd_crrltd_chnk_id_stripe:\n");
        for(j=0; j<num_chunk_per_lg; j++){
            stripe_chunk_idx=slct_lg_chnk_idx[j];
            printf("%d ", stripe_chnk_crrltn_dgr[stripe_chunk_idx*(erasure_k+1)]);
        }
        printf("\n");
#endif

    }

    free(if_select);
    free(slct_lg_chnk_idx);

}



/* organize the stripes */
void stripe_orgnzt(int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix, int num_correlated_chunk, int* crrltd_chnk_pttn_idx){

    int i,j;
    int first_chunk_index, second_chunk_index;
    int first_chunk, second_chunk;
    int temp_max;

    int* temp_stripe=(int*)malloc(sizeof(int)*erasure_k);                             // it records the chunks in a temp stripe
    int* temp_stripe_idx_to_rele_chunk_table=(int*)malloc(sizeof(int)*erasure_k);     // record the index of the data chunk in the access stream
    int* if_select_correlated_chunks=(int*)malloc(sizeof(int)*num_correlated_chunk);  // mark if a correlated data chunk has been selected in stripe organization
    int* crrltd_chnk_set=(int*)malloc(sizeof(int)*num_correlated_chunk);              // store the correlated data chunks that are identified
    int* stripe_chnk_crrltn_dgr=(int*)malloc(sizeof(int)*(erasure_k+1)*erasure_k);    // record the correlation degree among the chunks within a stripe
    int* stripe_chnk_idx_in_crrltn_set=(int*)malloc(sizeof(int)*erasure_k);           // record the index of the stripe chunks in correlated chunks

    memset(if_select_correlated_chunks, 0, sizeof(int)*num_correlated_chunk);
    memset(stripe_chnk_crrltn_dgr, 0, sizeof(int)*(erasure_k+1)*erasure_k);

    first_chunk_index=-1;
    second_chunk_index=-1;

    //initialize crrltd_chnk_set
    for(i=0; i<num_correlated_chunk; i++)
        crrltd_chnk_set[i]=caso_crltd_mtrx[i*max_num_correlated_chunks_per_chunk];

#if debug
    printf("caso_crltd_mtrx:\n");
    print_matrix(caso_crltd_mtrx, max_num_correlated_chunks_per_chunk, num_correlated_chunk);

    printf("caso_crltd_dgr_mtrix:\n");
    print_matrix(caso_crltd_dgr_mtrix, max_num_correlated_chunks_per_chunk, num_correlated_chunk);
#endif

    // initialize the uncorrelated chunks and their indices
    int stripe_count;
    int temp_count;
    int priority;
    int select_chunk_index;
    int flag;
    int max_crrltd_stripe;
    int candidate_chunk;
    int temp_index;
    int select_chunk;
    int if_exist_clltn;

    stripe_count=0;
    ognz_crrltd_cnt=0;
	select_chunk_index=-1;

    // the maximum number of correlated stripes 
    max_crrltd_stripe=num_correlated_chunk/erasure_k;

    sort_ognzd_crrltd_chnk=(int*)malloc(sizeof(int)*erasure_k*max_crrltd_stripe);
    ognzd_crrltd_chnk_lg=(int*)malloc(sizeof(int)*erasure_k*max_crrltd_stripe);
    ognzd_crrltd_chnk_id_stripe=(int*)malloc(sizeof(int)*erasure_k*max_crrltd_stripe);

    memset(sort_ognzd_crrltd_chnk, -1, sizeof(int)*erasure_k*max_crrltd_stripe);
    memset(ognzd_crrltd_chnk_lg, -1, sizeof(int)*erasure_k*max_crrltd_stripe);
    memset(ognzd_crrltd_chnk_id_stripe, -1, sizeof(int)*erasure_k*max_crrltd_stripe);

    // organize the correlated stripes first 
    for(stripe_count=0; stripe_count<max_crrltd_stripe; stripe_count++){

        memset(stripe_chnk_idx_in_crrltn_set, -1, sizeof(int)*erasure_k);
        memset(stripe_chnk_crrltn_dgr, 0, sizeof(int)*(erasure_k+1)*erasure_k);

        // initialize temp_stripe_idx_to_rele_chunk_table
        for(i=0; i<erasure_k; i++)
            temp_stripe_idx_to_rele_chunk_table[i]=-1;

        temp_count=0;
        if_exist_clltn=0;

        // find out the first two most correlated data chunks 
        temp_max=-1;

        for(i=0;i<num_correlated_chunk;i++){

            //if a correlated chunk is selected, we mark all the values of its row to be -1
            if(if_select_correlated_chunks[i]==1) 
                continue;

            for(j=1;j<max_num_correlated_chunks_per_chunk;j++){

                if(caso_crltd_mtrx[i*max_num_correlated_chunks_per_chunk+j]==-1)
                    break;

                if(caso_crltd_dgr_mtrix[i*max_num_correlated_chunks_per_chunk+j]>temp_max){

                    candidate_chunk=caso_crltd_mtrx[i*max_num_correlated_chunks_per_chunk+j];
                    temp_index=binary_search(crrltd_chnk_set, num_correlated_chunk, candidate_chunk);

                    if(if_select_correlated_chunks[temp_index]==1)
                        continue;

                    first_chunk_index=i;
                    second_chunk_index=temp_index;

                    temp_max=caso_crltd_dgr_mtrix[i*max_num_correlated_chunks_per_chunk+j];
                    if_exist_clltn=1;

                }
            }

        }

        // if there is no correlations among the remaining correlated data chunks, then break
        if(if_exist_clltn==0)
            break;

        // mark the cell to indicate that these two chunks have been selected 
        if_select_correlated_chunks[first_chunk_index]=1;
        if_select_correlated_chunks[second_chunk_index]=1;

        // record the index 
        stripe_chnk_idx_in_crrltn_set[temp_count]=first_chunk_index;

        // update stripe_chnk_crrltn_dgr
        first_chunk=trace_access_pattern[crrltd_chnk_pttn_idx[first_chunk_index]];
        stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)]=first_chunk;

        // update ognzd_crrltd_chnk
        sort_ognzd_crrltd_chnk[ognz_crrltd_cnt]=first_chunk;
        ognz_crrltd_cnt++;

        temp_stripe[temp_count]=first_chunk;
        temp_stripe_idx_to_rele_chunk_table[temp_count]=first_chunk_index;
        temp_count++;

        // record the index 
        stripe_chnk_idx_in_crrltn_set[temp_count]=second_chunk_index;

        // update stripe_chnk_crrltn_dgr
        second_chunk=trace_access_pattern[crrltd_chnk_pttn_idx[second_chunk_index]];
        stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)]=second_chunk;
        stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)+1]=temp_max; 

        // update ognzd_crrltd_chnk
        sort_ognzd_crrltd_chnk[ognz_crrltd_cnt]=second_chunk;
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

            // if there still exists correlated chunks, then organize the chunks that owns largest priority
            if(flag==1){

                //record the index 
                stripe_chnk_idx_in_crrltn_set[temp_count]=select_chunk_index;

                // update stripe_chnk_crrltn_dgr
                select_chunk=trace_access_pattern[crrltd_chnk_pttn_idx[select_chunk_index]];
                stripe_chnk_crrltn_dgr[temp_count*(erasure_k+1)]=select_chunk;

                // update ognzd_crrltd_chnk
                sort_ognzd_crrltd_chnk[ognz_crrltd_cnt]=select_chunk;
                ognz_crrltd_cnt++;

                if(strcmp(code_type, "lrc")==0)
                    updt_stripe_chnk_crrltn_dgr(temp_stripe, temp_count, temp_stripe_idx_to_rele_chunk_table, caso_crltd_mtrx, 
                            caso_crltd_dgr_mtrix, select_chunk_index, stripe_chnk_crrltn_dgr);

                temp_stripe[temp_count]=select_chunk;
                temp_stripe_idx_to_rele_chunk_table[temp_count]=select_chunk_index;

                if_select_correlated_chunks[select_chunk_index]=1;

            }
        }
		

        //if it is LRC, then we have to organize the chunks of a stripe into local groups
        if(strcmp(code_type, "lrc")==0) 
            lrc_local_group_orgnzt(stripe_chnk_crrltn_dgr, stripe_chnk_idx_in_crrltn_set, crrltd_chnk_pttn_idx, stripe_count);

    }

#if debug
    printf("correlated stripes are completely organized!\n");
    printf("ognz_crrltd_cnt=%d\n",ognz_crrltd_cnt);

    printf("before_sort: ognzd_crrltd_chnk\n");
    print_matrix(sort_ognzd_crrltd_chnk, ognz_crrltd_cnt, 1);
#endif

    // initialize the index 
    sort_ognzd_crrltd_chnk_index=(int*)malloc(sizeof(int)*ognz_crrltd_cnt);

    for(i=0; i<ognz_crrltd_cnt; i++)
        sort_ognzd_crrltd_chnk_index[i]=i;

    // sort the ognzd_crrltd_chnk 
    QuickSort_index(sort_ognzd_crrltd_chnk, sort_ognzd_crrltd_chnk_index, 0, ognz_crrltd_cnt-1);

#if debug
    printf("sort_ognzd_crrltd_chnk\n");
    print_matrix(sort_ognzd_crrltd_chnk, ognz_crrltd_cnt, 1);

    printf("sort_ognzd_crrltd_chnk_index\n");
    print_matrix(sort_ognzd_crrltd_chnk_index, ognz_crrltd_cnt, 1);

    printf("ognzd_crrltd_chnk_lg\n");
    for(i=0; i<ognz_crrltd_cnt; i++)
        printf("%d ",ognzd_crrltd_chnk_lg[i]);
    printf("\n");

    printf("ognzd_crrltd_chnk_id_stripe\n");
    for(i=0; i<ognz_crrltd_cnt; i++)
        printf("%d ",ognzd_crrltd_chnk_id_stripe[i]);
    printf("\n");
#endif

    free(if_select_correlated_chunks);
    free(temp_stripe);
    free(temp_stripe_idx_to_rele_chunk_table);
    free(crrltd_chnk_set);
    free(stripe_chnk_crrltn_dgr);
    free(stripe_chnk_idx_in_crrltn_set);

}


/* find the correlated data chunks and organize them into stripes */
void caso_stripe_ognztn(char *trace_name,  int *analyze_chunks_time_slots, int *num_chunk_per_timestamp, int bgn_tmstmp_num, int* sort_caso_rcd_pattern, 
        int* sort_caso_rcd_index, int* sort_caso_rcd_freq){

	printf("\n+++++++++ Correlation Analysis and Stripe Organization +++++++++\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(1);
    }

    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char pre_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
    char usetime[10];
    char divider=',';

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

    memset(analyze_chunks_time_slots, 0, sizeof(int)*bgn_tmstmp_num*max_access_chunks_per_timestamp);
    memset(num_chunk_per_timestamp, 0, sizeof(int)*bgn_tmstmp_num);

    struct timeval begin_time, end_time; 
    gettimeofday(&begin_time, NULL);

    /* record the access chunks per timestamp in a table */
    while(fgets(operation, sizeof(operation), fp)){

        // break the operation
        new_strtok(operation,divider,orig_timestamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);
        new_strtok(operation,divider,offset);
        new_strtok(operation,divider,size);
        new_strtok(operation,divider,usetime);

        if(strcmp(op_type, "Read")==0)
            continue;

        // process timestamp, offset, and operated size
        process_timestamp(orig_timestamp, round_timestamp);
        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

        access_start_block=(*offset_int)/block_size;
        access_end_block=(*offset_int+*size_int-1)/block_size;

        // analyze the access pattern 
        // if it is accessed in the same timestamp
        if(strcmp(pre_timestamp, round_timestamp)!=0){

            if(if_begin==0){

                num_chunk_per_timestamp[count_timestamp]=cur_index;
                cur_index=0; 
                count_timestamp++;
				
                // the trace replay will start at the timestamp with the order of bgn_tmstamp_num
                // so the correlation analysis ends at the timestamp with the order of bgn_tmstamp_num-1
                if(count_timestamp==bgn_tmstmp_num-1) 
                    break;
            }

            else if_begin=0;

            strcpy(pre_timestamp, round_timestamp);

        }

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

            // pre-validate the range of cur_index which should not be larger than max_access_chunks_per_timestamp
            if(cur_index>=max_access_chunks_per_timestamp){

                printf("analyze_chunks_time_slots record error! cur_index=%d, max_access_chunks_per_timestamp=%d\n", cur_index, max_access_chunks_per_timestamp);
                exit(1);

            }

            // record the potential correlated data chunk
            analyze_chunks_time_slots[count_timestamp*max_access_chunks_per_timestamp+cur_index]=k;
            cur_index++;

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

    int* rcd_peer_chks=(int*)malloc(poten_crrltd_cnt*sizeof(int)*max_num_peer_chunks);  // record the peer chunks with each potential correlated chunk in caso analysis
    int* freq_peer_chks=(int*)malloc(poten_crrltd_cnt*sizeof(int)*max_num_peer_chunks); // record the number of times that are accessed together 
    int* num_peer_chks=(int*)malloc(sizeof(int)*poten_crrltd_cnt); //it records the number of peer chunks per chunk

    memset(num_peer_chks, 0, sizeof(int)*poten_crrltd_cnt);

    record_access_freq(bgn_tmstmp_num, analyze_chunks_time_slots, caso_poten_crrltd_chks, num_chunk_per_timestamp, rcd_peer_chks, freq_peer_chks, num_peer_chks);

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

    //construct caso_crltd_mtrx, caso_crltd_dgr_mtrx
    int* caso_crltd_mtrx=(int*)malloc(sizeof(int)*num_correlated_chunk*max_num_correlated_chunks_per_chunk); // it records the correlated chunks of each chunk 
    int* caso_crltd_dgr_mtrix=(int*)malloc(sizeof(int)*num_correlated_chunk*max_num_correlated_chunks_per_chunk); // it records the correlation degree of any two correlated chunks
    int* crrltd_chnk_pttn_idx=(int*)malloc(sizeof(int)*num_correlated_chunk); //it records the index of the correlated chunk in the analyzed access patterns

    //extract the correlated data chunks and their degrees
    extract_caso_crltd_chnk_dgr(caso_crltd_mtrx, caso_crltd_dgr_mtrix, rcd_peer_chks, freq_peer_chks, num_correlated_chunk, num_peer_chks);

    // generate crrltd_chnk_pttn_idx 
    for(i=0; i<num_correlated_chunk; i++){

        chunk_id=caso_crltd_mtrx[i*max_num_correlated_chunks_per_chunk];
        sort_index=binary_search(sort_caso_rcd_pattern, caso_rcd_idx, chunk_id);
        crrltd_chnk_pttn_idx[i]=sort_caso_rcd_index[sort_index];

    }

    gettimeofday(&end_time, NULL);
    printf("Correlation Identification Time = %lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

    gettimeofday(&begin_time, NULL);

    //stripe organization
    stripe_orgnzt(caso_crltd_mtrx, caso_crltd_dgr_mtrix, num_correlated_chunk, crrltd_chnk_pttn_idx);

    gettimeofday(&end_time, NULL);
    printf("Correlation Graph Partition Time = %lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

    free(freq_peer_chks);
    free(correlate_chunk_bucket);
    free(caso_crltd_mtrx);
    free(caso_crltd_dgr_mtrix);
    free(rcd_peer_chks);
    free(crrltd_chnk_pttn_idx);
    free(num_peer_chks);
    free(caso_poten_crrltd_chks);

}

/* quick sort for the value only */
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

    if(p - left > 1)
        quick_sort_value(data,left, p-1);
    if(right - p > 1)
        quick_sort_value(data,p+1, right);

}


// calculate the ones in the matrix
int calculate_chunk_num_io_matrix(int *io_matrix, int len, int width){

    int res=0;
    int i;


    for(i=0;i<len*width;i++)
        if(io_matrix[i]>0)
            res++;


    return res;

}

/* perform the write operations to a batch of disks */
// @accessed_stripes: records the involved stripes in a timestamp
// @stripe_count: records the number of involved stripes in a timestamp
// @io_matrix: if a chunk is accessed, then the corresponding cell is marked as 1
void system_partial_stripe_writes(int *io_matrix, int *accessed_stripes, int stripe_count){

    int i,j;
    int io_amount;
    int io_index;
    int ret;
    int pagesize;
    int fd_disk[15];
    int stripe_id;
    int rotation;
    int num_disk_stripe;
    int disk_id;

    // get the value of num_disk_stripe
    if(strcmp(code_type, "rs")==0)
        num_disk_stripe=erasure_k+erasure_m;

    else if (strcmp(code_type, "lrc")==0)
        num_disk_stripe=erasure_k+erasure_m+num_lg;

    else {

        printf("ERR: num_disk_stripe\n");
        exit(1);

    }

    pagesize=getpagesize();

    // count the io amount
    io_amount=calculate_chunk_num_io_matrix(io_matrix, stripe_count, num_disk_stripe);

#if debug
    printf("\nio_matrix:\n");
    print_matrix(io_matrix, num_disk_stripe, stripe_count);
#endif


    // record the start point and end point for each i/o
    struct aiocb* aio_list = (struct aiocb*)malloc(sizeof(struct aiocb)*io_amount);
    int* updt_chnk_io_map=(int*)malloc(sizeof(int)*stripe_count*num_disk_stripe);

    bzero(aio_list, sizeof (struct aiocb)*io_amount);
    memset(updt_chnk_io_map, -1, sizeof(int)*stripe_count*num_disk_stripe);

    // open the disks 
    for(i=0; i<num_disk_stripe; i++){

        fd_disk[i]=open64(disk_array[i], O_RDWR | O_DIRECT | O_SYNC);

        if(fd_disk[i]<0){
            printf("ERR: opening file %s fails\n", disk_array[i]);
            exit(1);
        }
    }

    // initialize the io requests
    io_index=0;
    for(j=0; j<stripe_count; j++){

        for(disk_id=0; disk_id<num_disk_stripe; disk_id++){

            // if there is an io request, then initialize the aio_structure
            if(io_matrix[j*num_disk_stripe+disk_id]>0){

                // initialize the aio info
                aio_list[io_index].aio_fildes=fd_disk[disk_id];
                aio_list[io_index].aio_nbytes=block_size;

                // make sure that the offset should not exceed the disk capacity. 
                aio_list[io_index].aio_offset=1LL;
                aio_list[io_index].aio_offset=aio_list[io_index].aio_offset*accessed_stripes[j]*block_size; 

                aio_list[io_index].aio_reqprio=0;
                ret = posix_memalign((void**)&aio_list[io_index].aio_buf, pagesize, block_size);

                updt_chnk_io_map[j*num_disk_stripe+disk_id]=io_index;
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
            printf("aio_offset=%lld\n", (long long)aio_list[i].aio_offset);
            printf("%d-th aio_read_offset=%.2lfMB\n", i, aio_list[i].aio_offset*1.0/1024/1024);
            printf("aio_read_return error, ret=%d, io_no=%d\n",ret,i);
            printf("accessed_stripes:\n");
            print_matrix(accessed_stripes, stripe_count, 1);
        }
    }

#if debug
    printf("updt_chnk_io_map:\n");
    print_matrix(updt_chnk_io_map, num_disk_stripe, stripe_count);
#endif

    // perform encoding operation 
    int* rs_encoding_matrix;
    char** data_delta;
    char** coding_delta;

    data_delta=(char**)malloc(sizeof(char*)*erasure_k);
    coding_delta=(char**)malloc(sizeof(char*)*erasure_k);

    for(i=0; i<erasure_k; i++)
        data_delta[i]=(char*)malloc(sizeof(char)*block_size);

    for(i=0; i<erasure_k; i++)
        coding_delta[i]=(char*)malloc(sizeof(char)*block_size);


    // get extention matrix of rs codes
    rs_encoding_matrix = talloc(int, erasure_m*erasure_k);
    for (i = 0; i < erasure_m; i++) {
        for (j = 0; j < erasure_k; j++) 
            rs_encoding_matrix[i*erasure_k+j] = galois_single_divide(1, i ^ (erasure_m + j), erasure_w);
    }

    // update the parity chunks in memory
    int* updt_chunk_stripe=(int*)malloc(sizeof(int)*num_disk_stripe);
    char* new_data=(char*)malloc(sizeof(char)*block_size);
    char* new_parity=(char*)malloc(sizeof(char)*block_size);
    char* final_coding_delta=(char*)malloc(sizeof(char)*block_size);
    char** temp_data_delta=(char**)malloc(sizeof(char*)*erasure_k);

    int  updt_mrk_stripe[erasure_k];
    int chunk_id_stripe;
    int updt_chnk_cnt;
    int k;
    int global_prty_id, local_prty_id;
    int num_data_chunk_lg;
    int io_id;
    int temp_cnt;

    num_data_chunk_lg=erasure_k/num_lg;

    for(i=0; i<stripe_count; i++){

        memset(updt_chunk_stripe, 0, sizeof(int)*num_disk_stripe);
        memset(updt_mrk_stripe, -1, sizeof(int)*erasure_k);

        stripe_id=accessed_stripes[i];
        rotation=stripe_id%num_disk_stripe;

        updt_chnk_cnt=0;

        // update the data chunks in the memory
        for(j=0; j<num_disk_stripe; j++){

            if(io_matrix[i*num_disk_stripe+j]==0)
                continue;

            // get the id of the updated chunk in the stripe
            chunk_id_stripe=j-rotation;
            if(chunk_id_stripe<0)
                chunk_id_stripe+=num_disk_stripe;

            // if it is a data chunk
            if(chunk_id_stripe<erasure_k){

                // record the id of the updated data chunk in the stripe. It is used in the following encoding
                updt_mrk_stripe[updt_chnk_cnt]=chunk_id_stripe;

                // generate random buff to act as the new incoming data chunk 
                gene_radm_buff(new_data, block_size);

                // calculate the data delta chunk
                io_id=updt_chnk_io_map[i*num_disk_stripe+j];
                cal_delta(data_delta[updt_chnk_cnt], (char*)aio_list[io_id].aio_buf, new_data, block_size);

                // update the data 
                memcpy((char*)aio_list[io_id].aio_buf, new_data, sizeof(char)*block_size);

                // increase the record
                updt_chnk_cnt++;

            }
        }

#if debug
        printf("updt_chnk_cnt=%d, updt_mrk_stripe:\n", updt_chnk_cnt);
        print_matrix(updt_mrk_stripe, updt_chnk_cnt, 1);
#endif

        // calculate the parity delta chunks in the memory  
        for(j=0; j<num_disk_stripe; j++){

            if(io_matrix[i*num_disk_stripe+j]==0)
                continue;

            chunk_id_stripe=j-rotation;
            if(chunk_id_stripe<0)
                chunk_id_stripe+=num_disk_stripe;

            // if it is a parity chunk 
            if(chunk_id_stripe>=erasure_k){

                io_id=updt_chnk_io_map[i*num_disk_stripe+j];

                // if it is a parity chunk for reed-solomon codes or a global parity chunk for local reconstruction codes
                if((strcmp(code_type, "rs")==0) || (strcmp(code_type, "lrc")==0 && chunk_id_stripe>=erasure_k+num_lg)){

                    // determine global parity id
                    if(strcmp(code_type, "rs")==0)
                        global_prty_id=chunk_id_stripe-erasure_k;

                    else
                        global_prty_id=chunk_id_stripe-erasure_k-num_lg;

                    // generate the parity delta chunks
                    for(k=0; k<updt_chnk_cnt; k++)
                        encode_data(data_delta[k], coding_delta[k], rs_encoding_matrix[global_prty_id*erasure_k+updt_mrk_stripe[k]]);

                    // aggregate the parity delta chunks 
                    aggregate_data(coding_delta, updt_chnk_cnt, final_coding_delta);

                    // calculate the new parity chunk by xoring 
                    cal_delta(new_parity, final_coding_delta, (char*)aio_list[io_id].aio_buf, block_size); 

                    // update aio buff
                    memcpy((char*)aio_list[io_id].aio_buf, new_parity, sizeof(char)*block_size);

                }

                // if it is a local parity chunk for local reconstruction codes
                else if ((strcmp(code_type, "lrc")==0) && (chunk_id_stripe<erasure_k+num_lg)){

                    // calculate the local parity id 
                    local_prty_id=chunk_id_stripe-erasure_k;

#if debug
                    printf("local_prty_id=%d\n", local_prty_id);
                    printf("lp_addr=%p\n", lp_addr);
                    printf("num_data_chunk_lg=%d, index=%d\n", num_data_chunk_lg, j);
                    printf("updt_mrk_stripe:\n");
                    print_matrix(updt_mrk_stripe, updt_chnk_cnt, 1);
#endif

                    temp_cnt=0;
                    for(k=0; k<updt_chnk_cnt; k++){

                        if(updt_mrk_stripe[k]/num_data_chunk_lg==local_prty_id){

                            temp_data_delta[temp_cnt]=data_delta[k];
                            temp_cnt++;

                        }
                    }

                    // aggregate the data delta: the coefficients are all 1s for the data chunks to calculate the local parity chunks in Azure's LRC
                    aggregate_data(temp_data_delta, temp_cnt, new_data);

                    // generate the new local parity chunk 
                    cal_delta(new_parity, new_data, (char*)(aio_list[io_id].aio_buf), block_size);

                    // copy the new parity to the aio buff 
                    memcpy((char*)(aio_list[io_id].aio_buf), new_parity, block_size);

                }
            }
        }   
    }

    // perform the aio_write operations  
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

    // close the disk files
    for(i=0; i<num_disk_stripe; i++)
        close(fd_disk[i]);

    // free the used memory sizes 
    for(i=0; i<io_index; i++)
        free((char*)(aio_list[i].aio_buf));

    free(aio_list);
    free(updt_chunk_stripe);
    free(new_data);
    free(final_coding_delta);
    free(new_parity);

    for(i=0; i<erasure_k; i++){

        free(data_delta[i]);
        free(coding_delta[i]);

    }

    free(data_delta);
    free(coding_delta);
    free(rs_encoding_matrix);
    free(updt_chnk_io_map);
    free(temp_data_delta);


}

/* initialize the chunk information */
void get_chnk_info(int chunk_id, CHUNK_INFO* chunk_info){

    int crrltd_chnk_idx;
    int temp_index;
    int num_chunk_per_lg;

    num_chunk_per_lg=erasure_k/num_lg;

    // get logical_chunk_id
    chunk_info->logical_chunk_id=chunk_id;

    // get stripe_id 
    // 1) check if the chunk is a correlated chunk that is organized; 
    // 2) calculate the number of correlated data chunks whose chunk ids are smaller than the given chunk id

    SEARCH_INFO* si=(SEARCH_INFO*)malloc(sizeof(SEARCH_INFO));
    memset(si, 0, sizeof(SEARCH_INFO));

    // find the maximum number of correlated chunks whose chunk identities are smaller than the given chunk identity
    find_max_crrltd_chnk_udr_bundry(chunk_id, si);

    // if the chunk is a correlated data chunk, then record its stripe id (as well as the local group id for LRC) and the identity in the stripe
    if(si->search_flag==1){

        temp_index=binary_search(sort_ognzd_crrltd_chnk, ognz_crrltd_cnt, chunk_id);
        crrltd_chnk_idx=sort_ognzd_crrltd_chnk_index[temp_index];

        chunk_info->stripe_id=crrltd_chnk_idx/erasure_k;
        chunk_info->lg_id=ognzd_crrltd_chnk_lg[crrltd_chnk_idx];

        if(strcmp(code_type, "rs")==0)
            chunk_info->chunk_id_in_stripe=crrltd_chnk_idx%erasure_k;   
        else
            chunk_info->chunk_id_in_stripe=ognzd_crrltd_chnk_id_stripe[crrltd_chnk_idx];

    }

    // if the chunk is a uncorrelated data chunk, then we can quickly determine its stripe id and other metadata info
    else {

        chunk_info->stripe_id=(chunk_id - si->num_crrltd_chnk_bf_chnk_id)/erasure_k;
        chunk_info->chunk_id_in_stripe=(chunk_id - si->num_crrltd_chnk_bf_chnk_id)%erasure_k;
        chunk_info->lg_id=chunk_info->chunk_id_in_stripe/num_chunk_per_lg;

    }

#if debug
    if(si->search_flag==1)
        printf("Correlated chunk:\n");

    else 
        printf("Uncorrelated chunk:\n");

    printf("chunk_info->logical_chunk_id=%d\n", chunk_info->logical_chunk_id);
    printf("chunk_info->stripe_id=%d\n",        chunk_info->stripe_id);
    printf("chunk_info->chunk_id_in_stripe=%d\n", chunk_info->chunk_id_in_stripe);
    printf("chunk_info->lg_id=%d\n", chunk_info->lg_id);
    printf("\n");
#endif

    free(si);

}

/* perform the partial stripe writes in CASO: we will extract the write requests that are not used for correlation analysis for 
   validating the effectivenss of CASO */
int psw_time_caso(char *trace_name, char given_timestamp[], double *time){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(1);
    }

    // variables for recording the access information from the trace
    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char pre_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
    char usetime[10];
    char divider=',';

    int i,j;
    int k;  
    int access_start_block, access_end_block;
    int count;
    int flag;
    int io_count;
    int num_disk_stripe;

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

    // define the number of disks of a stripe for different codes
    if(strcmp(code_type, "rs")==0)
        num_disk_stripe=erasure_k+erasure_m;

    else if(strcmp(code_type, "lrc")==0)
        num_disk_stripe=erasure_k+lg_prty_num*num_lg+erasure_m;

    else {

        printf("ERR: input code_type\n");
        exit(1);

    }

    int max_accessed_stripes=max_access_chunks_per_timestamp;                  // suppose the maximum accessed stripes in a time distance is 100
    int* stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes); // the stripe id that are accessed in a time distance 
    int* lg_per_tmstmp=(int*)malloc(sizeof(int)*max_accessed_stripes*num_lg);  // record the accessed local groups in a time distance 
    int* io_request=(int*)malloc(sizeof(int)*max_accessed_stripes*num_disk_stripe); // matrix to record the io requests
    
    memset(io_request, 0, sizeof(int)*max_accessed_stripes*num_disk_stripe);

    int stripe_count;
    int write_count;
    int rotation;
    int lg_id, lg_count=0;
    int write_stripe_cnt;
    int updt_chnk_cnt;
	int if_begin;

    stripe_count=0;
    write_count=0;
    lg_id=-1;
    write_stripe_cnt=0;
    updt_chnk_cnt=0;
	if_begin=1;

    //initialize the pre_timestamp as the given timestamp
    strcpy(pre_timestamp, given_timestamp);

    struct timeval begin_time, end_time;

    CHUNK_INFO* chunk_info=(CHUNK_INFO*)malloc(sizeof(CHUNK_INFO));

    while(fgets(operation, sizeof(operation), fp)) {

        count++;

        // break the operation
        new_strtok(operation,divider,orig_timestamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);
        new_strtok(operation,divider,offset);
        new_strtok(operation,divider,size);
        new_strtok(operation,divider,usetime);

        // bypass the read operations
        if(strcmp(op_type, "Read")==0) 
            continue;

        // process the timestamp, the timestamp will be rounded up according to the given size of timedistance 
        // therefore, some write operations requested across several close timestamps may be in the same time distance
        process_timestamp(orig_timestamp, round_timestamp);

        // if the requests in the timestamp is used for correlation analysis, then bypass the requests in the timestamp 
        // we will use the requests that are not used for analysis for validating the performance improvement of CASO
        if(strcmp(given_timestamp, round_timestamp)!=0 && flag==0)
            continue;

        // if the requests are in the timestamp that are not used for correlation analysis
        flag=1;

        // get the access chunks 
        trnsfm_char_to_int(offset, offset_int);
        access_start_block=(*offset_int)/block_size;

        trnsfm_char_to_int(size, size_int);
        access_end_block=(*offset_int+*size_int-1)/block_size;

        // if it is a request in a new timedistance 
        if(strcmp(pre_timestamp, round_timestamp)!=0){

			if(if_begin==0){

                // calculate the partial stripe writes in the last time distance 
                io_count+=stripe_count*erasure_m;

                // for LRC, we should calculate the updates to local parity chunks 
                if(strcmp(code_type, "lrc")==0)
					io_count+=lg_count*lg_prty_num;

                // perform system write
                if(strcmp(test_type, "testbed")==0){
				
                    gettimeofday(&begin_time, NULL);
                    system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count);
                    gettimeofday(&end_time, NULL);
                    *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;
				
            	    }

                // re-initialize the io_request array
                memset(io_request, 0, sizeof(int)*max_accessed_stripes*num_disk_stripe);
                memset(stripes_per_timestamp, -1, sizeof(int)*max_accessed_stripes);
                memset(lg_per_tmstmp, 0, sizeof(int)*max_accessed_stripes*num_lg);

                if(stripe_count>=max_accessed_stripes){
                    printf("ERR: max_accessed_stripes error!\n");
                    exit(1);
                }

                write_stripe_cnt+=stripe_count;

                // re-initialize the stripe_count
                stripe_count=0;
                lg_count=0;
                updt_chnk_cnt=0;

                write_count++;
				}

			else 
				if_begin=0;

			strcpy(pre_timestamp, round_timestamp); 

        }

        updt_chnk_cnt+=access_end_block-access_start_block+1;

        // for each written chunk, we will determine its stripe id and record the update to the parity chunks 
        for(i=access_start_block; i<=access_end_block; i++){

            memset(chunk_info, 0, sizeof(CHUNK_INFO));
            get_chnk_info(i, chunk_info); 

            // we assume that data and parity chunks are rotated with the stripe identity
            rotation=chunk_info->stripe_id%num_disk_stripe; 

            for(j=0; j<stripe_count; j++){

                if(stripes_per_timestamp[j]==chunk_info->stripe_id)
                    break;

            }

            if(j>=stripe_count){
                stripes_per_timestamp[stripe_count]=chunk_info->stripe_id;
                stripe_count++;
            }

            // check the local group
            if(strcmp(code_type, "lrc")==0){

                lg_id=chunk_info->lg_id;

                if(lg_per_tmstmp[j*num_lg+lg_id]==0){

                    lg_per_tmstmp[j*num_lg+lg_id]=1;
                    lg_count++;

                }
            }

            // mark the updated data chunk
            io_request[j*num_disk_stripe+(chunk_info->chunk_id_in_stripe+rotation)%num_disk_stripe]=1;

            // mark the global parity chunks
            for(k=num_disk_stripe-erasure_m; k<num_disk_stripe; k++)
                io_request[j*num_disk_stripe+(k+rotation)%num_disk_stripe]=3;

            // if it is lrc, then mark the local parity chunks
            if(strcmp(code_type, "lrc")==0)
                io_request[j*num_disk_stripe+(erasure_k+lg_id+rotation)%num_disk_stripe]=2;

        }
    }

    //for the last operation, add the parity update io
    if(strcmp(test_type, "testbed")==0){
		
        gettimeofday(&begin_time, NULL);
        system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count);
        gettimeofday(&end_time, NULL);
        *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;
		
    	}

    write_count++;
    io_count+=stripe_count*erasure_m; 
    io_count+=lg_count*lg_prty_num;

    if(strcmp(test_type,"testbed")==0)
	    printf("CASO(Parity Update I/O) = %d, CASO(Write Time) = %.2lf\n", io_count, *time);

	else
		printf("CASO(Parity Update I/O) = %d\n", io_count);
		  

    fclose(fp);
    free(stripes_per_timestamp);
    free(io_request);
    free(lg_per_tmstmp);
    free(chunk_info);

    return io_count;

}


/* perform partial stripe writes to the data and parity chunks organized by baseline stripe organization. 
   For BSO, we should replay the same write operations as in CASO for fair comparison */
int psw_time_bso(char *trace_name, char given_timestamp[], double *time){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(1);
    }

    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char pre_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
    char usetime[10];
    char divider=',';

    int i,j;
    int k;
    int access_start_block, access_end_block;
    int count;
    int flag;
    int io_count;
    int temp_stripe_id, temp_chunk_id;
    int stripe_count;
    int rotation;
    int num_disk_stripe;

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

    // define the number of disks of a stripe for different codes
    if(strcmp(code_type, "rs")==0)
        num_disk_stripe=erasure_k+erasure_m;

    else if(strcmp(code_type, "lrc")==0)
        num_disk_stripe=erasure_k+lg_prty_num*num_lg+erasure_m;

    else {

        printf("ERR: input code_type\n");
        exit(1);

    }

    int max_accessed_stripes=max_access_chunks_per_timestamp; // the worse case is that every chunk belongs to a different stripe
    int *stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);
    int *io_request=(int*)malloc(sizeof(int)*max_accessed_stripes*num_disk_stripe); // it records the io request in a timestamp
    int* lg_per_tmstmp=(int*)malloc(sizeof(int)*max_accessed_stripes*num_lg); 

    memset(io_request, 0, sizeof(int)*max_accessed_stripes*num_disk_stripe);

    int write_count;
    int lg_count=0, lg_id;
    int num_chnk_per_lg; 
    int write_stripe_cnt;
    int updt_chnk_cnt;
	int if_begin;

    lg_id=-1;
    stripe_count=0;
    write_count=0;
    write_stripe_cnt=0;
    num_chnk_per_lg=erasure_k/num_lg;
    updt_chnk_cnt=0;
	if_begin=1;

    //initialize the pre_timestamp as the given timestamp
    strcpy(pre_timestamp, given_timestamp);

    struct timeval begin_time, end_time;
    while(fgets(operation, sizeof(operation), fp)) {

        // break the operation
        new_strtok(operation,divider,orig_timestamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);
        new_strtok(operation,divider,offset);
        new_strtok(operation,divider,size);
        new_strtok(operation,divider,usetime);

        // bypass the read operations
        if(strcmp(op_type, "Read")==0) 
            continue;

        // process the timestamp, the timestamp will be rounded up according to the given size of timedistance 
        process_timestamp(orig_timestamp, round_timestamp);

        // if the requests in the timestamp is used for correlation analysis, then bypass the requests in the timestamp 
        // for BSO, we should replay the same write operations as in CASO for fair comparison
        if(strcmp(given_timestamp, round_timestamp)!=0 && flag==0)
            continue;

        // if it reaches the given timestamp
        flag=1;

        // get the access chunks 
        trnsfm_char_to_int(offset, offset_int);
        access_start_block=(*offset_int)/block_size;

        trnsfm_char_to_int(size, size_int);
        access_end_block=(*offset_int+*size_int-1)/block_size;      

        // if it is a request in a new timedistance
        if(strcmp(pre_timestamp, round_timestamp)!=0){

            // if it is not the first time distance 
            if(if_begin==0){
				
                // calculate the partial stripe writes in the last timestamp
                io_count+=stripe_count*erasure_m;

                // record the update to local parity for LRC
                if(strcmp(code_type, "lrc")==0)
                    io_count+=lg_count*lg_prty_num;

                // perform the system write
                if(strcmp(test_type, "testbed")==0){
				
                    gettimeofday(&begin_time, NULL);
                    system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count);
                    gettimeofday(&end_time, NULL);
                    *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

            	    }

                // re-initialize the stripes_per_timestamp
                memset(io_request, 0, sizeof(int)*max_accessed_stripes*num_disk_stripe);
                memset(stripes_per_timestamp, -1, sizeof(int)*max_accessed_stripes);
                memset(lg_per_tmstmp, 0, sizeof(int)*max_accessed_stripes*num_lg);

                write_stripe_cnt+=stripe_count;

                // record the current involved stripes in this operation
                stripe_count=0;
                lg_count=0;
                updt_chnk_cnt=0;

                count++;
                write_count++;
            	}

			else 
				if_begin=0;

			strcpy(pre_timestamp, round_timestamp); 

        }

        updt_chnk_cnt += access_end_block-access_start_block+1;

        // scan each written data chunk, determine its stripe id
        for(i=access_start_block; i<=access_end_block; i++){

            temp_stripe_id=i/erasure_k;
            rotation=temp_stripe_id%num_disk_stripe;
            temp_chunk_id=i%erasure_k;

            for(j=0; j<stripe_count; j++){

                if(stripes_per_timestamp[j]==temp_stripe_id)
                    break;

            }

            if(j>=stripe_count){
                stripes_per_timestamp[stripe_count]=temp_stripe_id;
                stripe_count++;
            }

            // record the updated info of local group
            if(strcmp(code_type, "lrc")==0){

                lg_id=temp_chunk_id/num_chnk_per_lg;

                if(lg_per_tmstmp[j*num_lg+lg_id]==0){

                    lg_per_tmstmp[j*num_lg+lg_id]=1;
                    lg_count++;

                }
            }

            // mark the updated data chunk
            io_request[j*num_disk_stripe+(temp_chunk_id+rotation)%num_disk_stripe]=1;

            // mark the global parity chunks
            for(k=num_disk_stripe-erasure_m; k<num_disk_stripe; k++)
                io_request[j*num_disk_stripe+(k+rotation)%num_disk_stripe]=3;

            // if it is lrc, then mark the local parity chunks
            if(strcmp(code_type, "lrc")==0)
                io_request[j*num_disk_stripe+(erasure_k+lg_id+rotation)%num_disk_stripe]=2;

        }
    }

    // perform the write operation in the last time distance 
    if(strcmp(test_type, "testbed")==0){
		
        gettimeofday(&begin_time, NULL);
        system_partial_stripe_writes(io_request, stripes_per_timestamp, stripe_count);
        gettimeofday(&end_time, NULL);
        *time+=end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000;

    	}

    write_count++;
	
    // for the last operation
    io_count+=stripe_count*erasure_m; 
    io_count+=lg_count*lg_prty_num;

    if(strcmp(test_type, "testbed")==0)
		printf("BSO(Parity Update I/O) = %d, BSO(Write Time) = %.2lf\n", io_count, *time);

	else 
		printf("BSO(Parity Update I/O)  = %d\n", io_count);

    fclose(fp);
    free(stripes_per_timestamp);
    free(io_request);
    free(lg_per_tmstmp); 

    return io_count;

}

/* perform degraded reads when a single disk is temporal unavaialble. Spacifically, we will use the global parity chunks to repair 
   an unavailable chunk for reed-solomon codes. While for LRC, we will utilize its local parity chunk. */
void degraded_reads(int *io_request, int *stripe_id_array, int stripe_count, int num_disk_stripe){

    int i, j; 
    int k;
    int temp_stripe, temp_rotation;
    int num_cur_req_chunks;
    int num_extra_req_chunks, cur_extra_chunk;
    int lg_id;
    int orig_failed_chunk_id;
    int num_chnk_per_lg;

    lg_id=-1;
    num_chnk_per_lg=erasure_k/num_lg;

    for(i=0; i<stripe_count; i++){

        num_cur_req_chunks=0;

        // calculate the number of elements to be read currently
        for(j=0; j<num_disk_stripe; j++)
            if(io_request[i*num_disk_stripe+j]==1)
                num_cur_req_chunks++;

        // scan the io_request
        for(j=0; j<num_disk_stripe; j++){

            // if a requested chunk is lost
            if(io_request[i*num_disk_stripe+j]==-1){

                temp_stripe=stripe_id_array[i];
                temp_rotation=temp_stripe%num_disk_stripe;

                // calculate the number of elements to be read currently 
                if(strcmp(code_type, "rs")==0){

                    num_extra_req_chunks=erasure_k-num_cur_req_chunks;

                    // if it has read no less than k chunks, then continue
                    if(num_extra_req_chunks<=0) 
                        continue;

                    // we read all the k-1 data chunks
                    for(k=0; k<erasure_k; k++){

                        cur_extra_chunk=(k+temp_rotation)%num_disk_stripe;

                        if(io_request[i*num_disk_stripe+cur_extra_chunk]==0)
                            io_request[i*num_disk_stripe+cur_extra_chunk]=2;

                    }

                    // read a global parity 
                    io_request[i*num_disk_stripe+(erasure_k+temp_rotation)%num_disk_stripe]=3;

                }

                // if it is lrc, then we use local parity to repair the lost data chunk
                else if(strcmp(code_type, "lrc")==0){

                    // determine the local group that the lost chunk belongs to 
                    orig_failed_chunk_id=(j+num_disk_stripe-temp_rotation)%num_disk_stripe;
                    lg_id=orig_failed_chunk_id/num_chnk_per_lg;

                    // read the data chunks in the corresponding local group
                    for(k=lg_id*num_chnk_per_lg; k<(lg_id+1)*num_chnk_per_lg; k++){

                        cur_extra_chunk=(k+temp_rotation)%num_disk_stripe;

                        if(io_request[i*num_disk_stripe+cur_extra_chunk]==0)
                            io_request[i*num_disk_stripe+cur_extra_chunk]=2;

                    }

                    // read one local parity chunk in that local group
                    io_request[i*num_disk_stripe+(erasure_k+lg_id*lg_prty_num+temp_rotation)%num_disk_stripe]=3;

                }
            }
        }
    }
}

/* perform degraded reads to the data and parity chunks organized by CASO. In the evaluation, different from the partial stripe 
   writes, read operations usually have high timely requirement. Due to this reason, we peroform a degraded read 
   *immediately* once it has an unavailable data chunk */
void dr_io_caso(char *trace_name, char given_timestamp[], int *num_extra_io, double *time){

    //read the data from csv file
    FILE *fp;
    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(1);
    }

    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
    char usetime[10];
    char divider=',';

    int i,j;
    int failed_disk;
    int access_start_block, access_end_block;
    int count;
    int io_count;
    int extra_io_count;
    int num_disk_stripe;
    int total_access_stripes;
    int total_access_correlated_stripes;

    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
    count=0;
    io_count=0;
    extra_io_count=0;

    // define the number of disks of a stripe for different codes
    if(strcmp(code_type, "rs")==0)
        num_disk_stripe=erasure_k+erasure_m;

    else if(strcmp(code_type, "lrc")==0)
        num_disk_stripe=erasure_k+lg_prty_num*num_lg+erasure_m;

    else {

        printf("ERR: input code_type\n");
        exit(1);

    }

    int max_accessed_stripes=max_access_chunks_per_timestamp; 
    int *stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);
    int *io_request=(int*)malloc(sizeof(int)*max_accessed_stripes*num_disk_stripe); // it records the io request in a timestamp

    int stripe_count;
    int rotation;
    int flag;
    int dr_crrltd_stripe_cnt;

    stripe_count=0;
    total_access_stripes=0;
    total_access_correlated_stripes=0;
    dr_crrltd_stripe_cnt=0;

    CHUNK_INFO* chunk_info=(CHUNK_INFO*)malloc(sizeof(CHUNK_INFO));

    count=0;

    // we perform degraded reads of the trace under every member disk's failure 
    for(failed_disk=0; failed_disk<num_disk_stripe; failed_disk++){

        // reset the pointer for reading the trace 
        fseek(fp, 0, SEEK_SET);
        flag=0;

        // read the trace 
        while(fgets(operation, sizeof(operation), fp)) {

            // break the operation
            new_strtok(operation,divider,orig_timestamp);
            new_strtok(operation,divider,workload_name);
            new_strtok(operation,divider,volumn_id);
            new_strtok(operation,divider,op_type);
            new_strtok(operation,divider,offset);
            new_strtok(operation,divider,size);
            new_strtok(operation,divider,usetime);

            process_timestamp(orig_timestamp, round_timestamp);

            // if it has not reached the given_timestamp then continue
            // we should replay the read operations after correlation analysis to validate the degraded read performance of CASO 
            if((strcmp(round_timestamp, given_timestamp)!=0) && (flag==0))
                continue;

            // the evaluation starts
            flag=1;

            // bypass the write operations
            if(strcmp(op_type, "Read")!=0)
                continue;

            trnsfm_char_to_int(offset, offset_int);
            trnsfm_char_to_int(size, size_int);
            access_start_block=(*offset_int)/block_size;
            access_end_block=(*offset_int+*size_int-1)/block_size;

            // we issue each degraded read request immediately and initialize the io_request array
            memset(io_request, 0, sizeof(int)*max_accessed_stripes*num_disk_stripe);
            memset(stripes_per_timestamp, 0, sizeof(int)*max_accessed_stripes);

            // update io_request
            stripe_count=0;

            // scan each chunk and generate the io matrix 
            for(i=access_start_block; i<=access_end_block; i++){

                // get the stripe id of the read data 
                memset(chunk_info, 0, sizeof(CHUNK_INFO));
                get_chnk_info(i, chunk_info); 
                rotation=chunk_info->stripe_id%num_disk_stripe; 

                for(j=0; j<stripe_count; j++)
                    if(stripes_per_timestamp[j]==chunk_info->stripe_id)
                        break;

                if(j>=stripe_count){
                    stripes_per_timestamp[stripe_count]=chunk_info->stripe_id;
                    stripe_count++;
                }

                // if the data is on the failed disk 
                int temp_chunk_id=(chunk_info->chunk_id_in_stripe+rotation)%num_disk_stripe;

                if(temp_chunk_id==failed_disk)
                    io_request[j*num_disk_stripe+temp_chunk_id]=-1;

                else 
                    io_request[j*num_disk_stripe+temp_chunk_id]=1;

            }

            // check the access stripes
            for(i=0; i<stripe_count; i++)
                if(stripes_per_timestamp[i]<ognz_crrltd_cnt/erasure_k)
                    total_access_correlated_stripes++;

            total_access_stripes+=stripe_count;

            // check if the read is degraded read 
            degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_disk_stripe);            

            io_count+=calculate_num_io(io_request, stripe_count, num_disk_stripe);
            extra_io_count=calculate_extra_io(io_request, stripe_count, num_disk_stripe);

            // analyze the io_reques
            for(i=0; i<stripe_count; i++){

                if((stripes_per_timestamp[i] < ognz_crrltd_cnt/erasure_k) && (io_request[i*num_disk_stripe+failed_disk]==-1)){

                    dr_crrltd_stripe_cnt++;
                    aver_read_size++;

                    for(j=0; j<num_disk_stripe; j++)
                        if(io_request[i*num_disk_stripe+j]==1)
                            aver_read_size++;

                }
            }

            *num_extra_io+=extra_io_count;
            count++;

        }
    }

    printf("CASO(Addi. I/O of Degraded Reads) = %d\n", *num_extra_io);

    fclose(fp);  
    free(stripes_per_timestamp);
    free(io_request);
    free(chunk_info);

}

/* perform degraded reads to the data and parity chunks organized by BSO */
void dr_io_bso(char *trace_name, char given_timestamp[], int *num_extra_io, double *time){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(1);
    }

    char operation[100];
    char orig_timestamp[100];
    char round_timestamp[100];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
    char usetime[10];
    char divider=',';

    int i,j;
    int failed_disk;
    int access_start_block, access_end_block;
    int count;
    int io_count;
    int extra_io_count;
    int temp_stripe_id, temp_chunk_id;
    int num_disk_stripe;
    int total_access_stripes;

    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
    count=0;
    io_count=0;
    extra_io_count=0;

    // define the number of disks of a stripe for different codes
    if(strcmp(code_type, "rs")==0)
        num_disk_stripe=erasure_k+erasure_m;

    else if(strcmp(code_type, "lrc")==0)
        num_disk_stripe=erasure_k+lg_prty_num*num_lg+erasure_m;

    else {

        printf("ERR: input code_type\n");
        exit(1);

    }

    int max_accessed_stripes=max_access_chunks_per_timestamp; 
    int* stripes_per_timestamp=(int*)malloc(sizeof(int)*max_accessed_stripes);
    int* io_request=(int*)malloc(sizeof(int)*max_accessed_stripes*num_disk_stripe); // it records the io request in a timestamp
    int stripe_count;
    int rotation;
    int flag;
    int dr_cnt;

    count=0;
    total_access_stripes=0;

    aver_read_size=0;
    dr_cnt=0;

	// we perform degraded reads of the trace under every member disk's failure 
    for(failed_disk=0; failed_disk<num_disk_stripe; failed_disk++){

		// reset the pointer for reading the trace 
        flag=0;
        fseek(fp, 0, SEEK_SET);

        // read the trace
        while(fgets(operation, sizeof(operation), fp)) {

            // break the operation
            new_strtok(operation,divider,orig_timestamp);
            new_strtok(operation,divider,workload_name);
            new_strtok(operation,divider,volumn_id);
            new_strtok(operation,divider,op_type);
            new_strtok(operation,divider,offset);
            new_strtok(operation,divider,size);
            new_strtok(operation,divider,usetime);
			
            process_timestamp(orig_timestamp, round_timestamp);

            // if it has not reached the given_timestamp then continue
            // we should replay the same degraded read operations as in CASO for fair comparison
            if((strcmp(round_timestamp, given_timestamp)!=0) && (flag==0))
                continue;

            // update flag
            flag=1;

            // bypass the write operations
            if(strcmp(op_type, "Read")!=0) 
                continue;

            count++;

            trnsfm_char_to_int(offset, offset_int);
            trnsfm_char_to_int(size, size_int);
            access_start_block=(*offset_int)/block_size;
            access_end_block=(*offset_int+*size_int-1)/block_size;      

            // we issue each read request immediately and initialize the io_request array
            memset(io_request, 0, sizeof(int)*max_accessed_stripes*num_disk_stripe);
            memset(stripes_per_timestamp, 0, sizeof(int)*max_accessed_stripes);

            // update io_requst
            stripe_count=0;

            // for each read chunk, record its stripe id
            for(i=access_start_block; i<=access_end_block; i++){

                temp_stripe_id=i/erasure_k;
                rotation=temp_stripe_id%num_disk_stripe;
                temp_chunk_id=i%erasure_k;

                for(j=0; j<stripe_count; j++){

                    if(stripes_per_timestamp[j]==temp_stripe_id)
                        break;

                }

                if(j>=stripe_count){
                    stripes_per_timestamp[stripe_count]=temp_stripe_id;
                    stripe_count++;
                }

                // if the data is on the failed disk 
                if((temp_chunk_id+rotation)%num_disk_stripe==failed_disk)
                    io_request[j*num_disk_stripe+(temp_chunk_id+rotation)%num_disk_stripe]=-1;

                else 
                    io_request[j*num_disk_stripe+(temp_chunk_id+rotation)%num_disk_stripe]=1;

            }

#if debug
            printf("stripes_per_timestamp:\n");
            print_matrix(stripes_per_timestamp, stripe_count, 1);
            printf("\n");
            printf("before degraded read:\n");
            print_matrix(io_request, num_disk_stripe, stripe_count);
            printf("\n");
#endif

            // perform degraded reads
            degraded_reads(io_request, stripes_per_timestamp, stripe_count, num_disk_stripe);

            io_count+=calculate_num_io(io_request, stripe_count, num_disk_stripe);
            extra_io_count=calculate_extra_io(io_request, stripe_count, num_disk_stripe);
            *num_extra_io+=extra_io_count;
            total_access_stripes+=stripe_count;

            // calculate the average degraded read size
            for(i=0; i<stripe_count; i++){

                if(io_request[i*num_disk_stripe+failed_disk]==-1){

                    aver_read_size++;
                    dr_cnt++;

                    for(j=0; j<num_disk_stripe; j++)
                        if(io_request[i*num_disk_stripe+j]==1)
                            aver_read_size++;

                }
            }

#if debug
            printf("after degraded read:\n");
            print_matrix(io_request, num_disk_stripe, stripe_count);
            printf("*num_extra_io=%d\n", *num_extra_io);
#endif

        }
    }

	printf("BSO(Addi. I/O of Degraded Reads) = %d\n", *num_extra_io);

    fclose(fp);
    free(stripes_per_timestamp);
    free(io_request);

}

/* read the data from random addresses of disks to mitigate the performance impact of cached data */
void clean_cache(void){

    int disk_num;
    int pagesize;
    off_t offset;
    int i;
    int fd_disk;
    int ret;
    int *buffer;

    pagesize=getpagesize();

    //read the data to the main memory
    offset=1LL;
    offset=offset*100*1024*1024*1024;

    if(strcmp(code_type,"rs")==0)
        disk_num=erasure_k+erasure_m;

    else 
        disk_num=erasure_k+erasure_m+num_lg;

    for(i=0; i<disk_num; i++){

        fd_disk=open64(disk_array[i], O_RDWR | O_DIRECT);

        ret = posix_memalign((void**)&buffer, pagesize, clean_cache_blk_num*block_size);
        if(ret)
            printf("clear cache: posix_memalign error\n");

        if(pread(fd_disk, buffer, clean_cache_blk_num*block_size, offset)!=clean_cache_blk_num*block_size)
            printf("read error!\n");

        close(fd_disk);
        free(buffer);

    }   
}

