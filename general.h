extern void trnsfm_char_to_int(char *char_data, long long *data);

extern int calculate_num_io(int *io_request, int col, int width);

extern void new_strtok(char string[], char divider, char result[]);

extern void calculate_chunk_num(char *trace_name);

extern void determine_begin_timestamp(char *trace_name, char begin_timestamp[], int begin_timestamp_num);

extern void calculate_caso_chunk_num(char *trace_name, char begin_timestamp[]);

extern int calculate_priority(int *stripe_map, int cur_chunk_num, int *temp_stripe_idx_in_rele_chunk_table, int *caso_relevant_set, int *caso_relevant_degree,  
                                      int candidate_chunk_index, int *relevant_chunks_table, int caso_num_rele_chunks);

extern void quick_sort_value(int *data, int left, int right);

extern void QuickSort_index(int *data, int index[],int left, int right);

extern int calculate_chunk_num_io_matrix(int *io_matrix, int len, int width);

extern void system_partial_stripe_writes(int *io_matrix, int *accessed_stripes, int stripe_count);

extern int psw_time_caso(char *trace_name, char given_timestamp[], double *time);

extern int psw_time_bso(char *trace_name, char given_timestamp[], double *time);

extern void degraded_reads(int *io_request, int *stripe_id_array, int stripe_count, int num_disk_stripe);

extern void system_parallel_reads(int *io_matrix, int *accessed_stripes, int stripe_count, int *total_write_block_num);

extern void dr_io_caso(char *trace_name, char given_timestamp[], int *num_extra_io, double *time);

extern void dr_io_bso(char *trace_name, char given_timestamp[], int *num_extra_io, double *time);

extern void sorting_trace_access_pattern();

extern void binary_search(int array[], int num, int value);

extern void encode_data(char* data, char* pse_coded_data, int ecd_cef);

extern void clean_cache(void);

extern void caso_stripe_ognztn(char *trace_name,  int *analyze_chunks_time_slots, int *num_chunk_per_timestamp, int bgn_tmstmp_num, int* sort_caso_rcd_pattern, int* sort_caso_rcd_index, int* sort_caso_rcd_freq);

extern void print_chunk_info(CHUNK_INFO* chunk_info); 

extern void gene_radm_buff(char* buff, int len); 

extern void cal_delta(char* result, char* srcA, char* srcB, int length);

extern void aggregate_data(char** coding_delta, int updt_chnk_cnt, char* final_result);

extern void process_timestamp(char* input_timestamp, char* output_timestamp);

extern int calculate_extra_io(int *io_request, int col, int width);

extern int check_if_in_access_bucket(int chunk_id);

extern void insert_chunk_into_bucket(int* bucket, int bucket_num, int chunk_id);

extern void replace_old_peer_chunk(int* freq_peer_chks, int* rcd_peer_chks, int* caso_poten_crrltd_chks, int* num_peer_chks, 
        int given_chunk_idx, int given_chunk, int peer_chunk, int* start_search_posi);

extern void record_access_freq(int bgn_tmstmp_num, int* analyze_chunks_time_slots, int* caso_poten_crrltd_chks, 
        int* num_chunk_per_timestamp, int* rcd_peer_chks, int* freq_peer_chks, int* num_peer_chks);

extern void extract_caso_crltd_chnk_dgr(int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix, int* rcd_peer_chks, int* freq_peer_chks, 
                                             int num_correlate_chunk, int* num_peer_chunk);

extern void find_max_crrltd_chnk_udr_bundry(int chunk_id, SEARCH_INFO* si);

extern void updt_stripe_chnk_crrltn_dgr(int *stripe_map, int cur_chunk_num, int *temp_stripe_idx_in_rele_chunk_table, int* caso_crltd_mtrx, 
        int* caso_crltd_dgr_mtrix,  int slct_chunk_index, int* stripe_chnk_crrltn_dgr);

extern void lrc_local_group_orgnzt(int* stripe_chnk_crrltn_dgr, int* stripe_chnk_idx_in_crrltn_set, int* crrltd_chnk_pttn_idx, int stripe_id);

extern void stripe_orgnzt(int* caso_crltd_mtrx, int* caso_crltd_dgr_mtrix, int num_correlated_chunk, int* crrltd_chnk_pttn_idx);

extern void get_chnk_info(int chunk_id, CHUNK_INFO* chunk_info);

extern void nr_time(char *trace_name, char given_timestamp[], double *time, int flag);
