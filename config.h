#ifndef _COMMON_H
#define _COMMON_H

/* parameters of erasure codes */
#define erasure_k 4     // the number of data chunks of a stripe in erasure codes
#define erasure_m 2     // the number of parity chunks of a stripe in erasure codes
#define num_lg 2        // the number of local groups in a stripe, it should be divisible by erasure_m and erasure_k
#define lg_prty_num 1   // the number of local parity in a local group 
#define erasure_w 8     // the value of w to configure the arthmetic on the finite field 
#define block_size 4096 // assume that the block size is 4KB

/* parameters of testbed */ 
#define num_disks 15    // number of disks used in our testbed experiment. Make sure that num_disks >= erasure_k + erasure_m + num_lg

/* parameters for data correlation analysis */
/* Note: we may allocate large arraies based on these global variables, which will consume memory space based on the values 
         of the global variables. We can adjust the analysis granularity (i.e., max_num_peer_chunks and max_num_correlated_chunks_per_chunk) 
         based on the memory space on the machine */
#define inf 999999999
#define max_aces_blk_num 1000000                  // the strict accessed blocks in a trace (a block may appear more than twice)
#define max_num_peer_chunks 5000                  // we call the chunks that are accessed within a timestamp peer chunks
#define max_num_correlated_chunks_per_chunk 5000  // it defines the maximum number of correlated chunks to a chunk
#define num_assume_timestamp 1000000              // the maximum number of timestamps assumed in the analysis
#define tm_dstnc_odr 4                            // it defines the time distance (i.e., four orders of magnitude of the windows filetime) for correlation analysis

/* other macro definitions*/
#define bucket_depth 2000         
#define contiguous_block 1024*16       // it defines the number of blocks in a column of a stripe under contiguous data placement
#define clean_cache_read_block_num 100 // it defines the number of blocks read for cleaning cache in the main memory
#define debug 0

/* the global variables for correlation analysis */
int access_bucket[max_aces_blk_num];        // it records the accessed chunks 
int order_access_bucket[max_aces_blk_num];  // it records the access frequency of each chunk 
int trace_access_pattern[max_aces_blk_num]; // it records the accessed blocks by strictly following the trace
int freq_access_chunk[max_aces_blk_num];    // it counts the access frequency of each chunk

int max_access_chunks_per_timestamp;    // it records the maximum number of chunks requested in a timestamp value
int cur_rcd_idx;                        // it records the number of access blocks recorded in the trace
int caso_rcd_idx;                       // it records the number of accessed chunks in correlation analysis
int total_access_chunk_num;             // it records the number accessed in all the patterns
int num_timestamp;                      // it records the number of timestamp in a trace
int ognz_crrltd_cnt;                    // number of correlated chunks that are organized
int total_num_req;
double aver_read_size;

char code_type[5];                      // it records the code used (either reed-solomon codes or local construction codes)
int* sort_ognzd_crrltd_chnk_index;      // it denotes the index of correlated chunks in ognzd_crrltd_chnk
int* sort_ognzd_crrltd_chnk;            // it records the sorted correlated chunks that are organized into correlated stripes
int* ognzd_crrltd_chnk_lg;              // the local group of correlated chunks in lrc 
int* ognzd_crrltd_chnk_id_stripe;       // the chunk_id_stripe of correlated chunks

/* the structrue recoding the metadata information of a chunk */
typedef struct _chunk_info{

	int logical_chunk_id;
	int stripe_id;
	int chunk_id_in_stripe;
	int lg_id;

}CHUNK_INFO;

/* the structure recording the search information */
typedef struct _search_info{

	int search_flag; //record if the chunk is a correlated chunk
	int num_crrltd_chnk_bf_chnk_id;

}SEARCH_INFO;

// record the disk identities of a disk array
char *disk_array[num_disks]={"/dev/sde","/dev/sdf","/dev/sdg","/dev/sdh","/dev/sdi","/dev/sdj","/dev/sdk","/dev/sdl","/dev/sdm","/dev/sdn",
	"/dev/sdo","/dev/sdp","/dev/sdq","/dev/sdr", "/dev/sds"};


#endif
