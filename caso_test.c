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

    if(argc!=5){
        printf("Please input: ./caso_test trace_name analysis_ratio code_type(rs or lrc) test_type(numeric or testbed)!\n");
        exit(1);
    }

    int i;
    int count;
	int ret;
    int begin_timestamp_num;
	double analysis_ratio; 

    /* ===== initialize the parameters ====== */
    analysis_ratio=atoi(argv[2])*1.0/100;
    strcpy(code_type, argv[3]);
	strcpy(test_type, argv[4]);

	if(strcmp(test_type, "testbed")==0){

		char input_info[5];
		printf("+++++++++ Confirmation before Testbed Evaluation +++++++++\n");
		printf("Have you filled the global disk info in the common.h file and the general.c file? Y or N?\n");
		ret=scanf("%s", input_info); 

		if(ret!=1){

			printf("Error: Scanf Funciton!\n");
			exit(1);
			
			}

		if(strcmp(input_info, "N")==0){

			printf("please fill the **disk array** info in the common.h and the general.c file first before running testbed experiment!\n");
			exit(1);

			}

		else if(strcmp(input_info, "Y")!=0){

			printf("Please input Y or N!\n");
			exit(1);

			}

		}
	
    total_access_chunk_num=0;
    num_timestamp=0;
    max_access_chunks_per_timestamp=-1;
    aver_read_size=0;

    // judge the input code: reed-solomon codes or local reconstruction code
    if(strcmp(code_type, "rs")!=0 && strcmp(code_type, "lrc")!=0){

        printf("ERR: input code_type should be 'rs' or 'lrc' \n");
        exit(1);

    }

    if(strcmp(code_type, "rs")==0)
        printf("\n======== trace=%s, RS(%d,%d), Analysis Ratio=%.2lf ========\n", argv[1], erasure_k, erasure_m, analysis_ratio);

    else if(strcmp(code_type, "lrc")==0)
        printf("\n======== trace=%s, LRC(%d,2,%d), Analysis Ratio=%.2lf ========\n", argv[1], erasure_k, erasure_m, analysis_ratio);

    // initialize the global variables
    memset(access_bucket, -1, sizeof(int)*max_aces_blk_num);
    memset(order_access_bucket, 0, sizeof(int)*max_aces_blk_num);
    memset(trace_access_pattern, -1, sizeof(int)*max_aces_blk_num);
    memset(freq_access_chunk, 0, sizeof(int)*max_aces_blk_num);

    // process the trace 
    calculate_chunk_num(argv[1]);

    // calculate the number of data chunks which are accessed no less than twice during the trace 
    int count_larger_2=0;
    count=0;
    for(i=0;i<cur_rcd_idx;i++){
        if(freq_access_chunk[i]>=2){
            count++;
            count_larger_2+=freq_access_chunk[i];
        }
    }

    // based on the analysis ratio, calculate the number of time distance for analysis
    begin_timestamp_num=analysis_ratio*num_timestamp;

    // determine the begin_timestamp 
    // The requests before the begin time window are used for correlation analysis, while those after the begin time window are used for performance validation 
    char begin_timestamp[100];
    determine_begin_timestamp(argv[1], begin_timestamp, begin_timestamp_num);

    // calculate the number of chunks which are used for analysis and are accessed no less than twice during the correlation analysis
    count=0;
    for(i=0; i<caso_rcd_idx; i++)
        if(freq_access_chunk[i]>=2)
            count++;

    int* analyze_chunks_time_slots=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp);   // it records  all the accessed blocks at every time windows for correlation analysis
    int* access_time_slots_index=(int*)malloc(sizeof(int)*begin_timestamp_num*max_access_chunks_per_timestamp);     // it records the index of each chunk of total_access in the trace_access_pattern
    int* num_chunk_per_timestamp=(int*)malloc(sizeof(int)*begin_timestamp_num);                                     // it records the number of accessed chunks in every time window before correlation analysis
    int* sort_caso_rcd_pattern=(int*)malloc(sizeof(int)*caso_rcd_idx);                                              // it records the sorted data chunks for correlation analysis in CASO
    int* sort_caso_rcd_index=(int*)malloc(sizeof(int)*caso_rcd_idx);                                                // it records the original index of the data chunks before being sorted 
    int* sort_caso_rcd_freq=(int*)malloc(sizeof(int)*caso_rcd_idx);                                                 // it records the access frequencies of the sorted data chunks 

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

    // organize the data chunks by using CASO only if there exist correlated data chunks
	if(caso_rcd_idx>0)
		caso_stripe_ognztn(argv[1], analyze_chunks_time_slots, num_chunk_per_timestamp, begin_timestamp_num, sort_caso_rcd_pattern, sort_caso_rcd_index, sort_caso_rcd_freq);


    double *caso_time, *striping_time;
    double f=0, g=0;

    caso_time=&f;
    striping_time=&g; 

    printf("\n+++++++++ Partial Stripe Writes Test +++++++++\n");
    psw_time_caso(argv[1],begin_timestamp, caso_time);

    // clean cache to mitigate the impact of cached items in next testbed experiments
	if(strcmp(test_type, "testbed")==0)
		clean_cache();
	
    psw_time_bso(argv[1], begin_timestamp, striping_time);

	if(strcmp(test_type, "testbed")==0)
		clean_cache();

    /* ========== Perform degraded reads ========= */
    int *caso_num_extra_io;
    int c=0;
    caso_num_extra_io=&c;

    int *striping_num_extra_io;
    int d=0;
    striping_num_extra_io=&d;

    printf("\n+++++++++ Degraded Read Test +++++++++\n");
    dr_io_caso(argv[1], begin_timestamp, caso_num_extra_io, caso_time); 
    dr_io_bso(argv[1], begin_timestamp, striping_num_extra_io, striping_time);
	printf("\n");

    free(num_chunk_per_timestamp);
    free(analyze_chunks_time_slots);
    free(sort_caso_rcd_pattern);
    free(sort_caso_rcd_index);
    free(access_time_slots_index);
    free(sort_caso_rcd_freq);
    free(sort_ognzd_crrltd_chnk);
    free(sort_ognzd_crrltd_chnk_index);
    free(ognzd_crrltd_chnk_lg);
    free(ognzd_crrltd_chnk_id_stripe);

    return 0;

}

