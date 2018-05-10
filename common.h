#ifndef _COMMON_H
#define _COMMON_H

#define inf 999999999
#define max_aces_blk_num 500000 // the strict accessed blocks in a trace (a block may appear more than twice)
#define max_num_peer_chunks 1000  // we call the chunks that are accessed within a timestamp peer chunks
#define max_num_relevent_chunks_per_chunk 20 // it defines the maximum number of relevant chunks to a chunk
#define num_assume_timestamp 1000000
#define bucket_depth 20

#define IF_LRC 0

#define block_size 4096
#define erasure_k 4
#define erasure_m 2
#define erasure_w 8
#define lrc_lg 2 // the number of local groups in a stripe
#define lg_prty_num erasure_m/lrc_lg  // the number of local parity in a local group 

#define contiguous_block 1024*16 // it defines the number of blocks in a column of a stripe under contiguous data placement
#define clean_cache_read_block_num 100 // it defines the number of blocks read for cleaning cache in the main memory
#define debug 0

int max_access_chunks_per_timestamp;
int total_num_req;

int trace_access_pattern[max_aces_blk_num]; // it records the accessed blocks by strictly following the trace
int freq_access_chunk[max_aces_blk_num]; // it counts the access frequency of each chunk

int sort_trace_pattern[max_aces_blk_num]; //it is the sorted result of trace_access_pattern
int sort_pattern_index[max_aces_blk_num]; //it records the original index of the chunk in trace_access_pattern

int cur_rcd_idx; // it records the number of access blocks recorded in the trace
int caso_rcd_idx; // it records the number of accessed chunks in correlation analysis
int total_access_chunk_num; // it records the number accessed in all the patterns
int num_timestamp; // it records the number of timestamp in a trace
int num_rele_chunks; // it records the number of relevant chunks in the trace

char code_type[5];

#endif
