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

extern int calculate_psw_io_striping_stripe(char *trace_name, char given_timestamp[]);

extern int calculate_psw_io_caso_stripe(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map);

extern int calculate_psw_io_continugous_stripe(char *trace_name, char given_timestamp[], int max_access_block);

extern int calculate_chunk_num_io_matrix(int *io_matrix, int len, int width);

extern void stripe_organization(int *relevant_chunks_table, int *caso_relevant_set, int *caso_relevant_degree,  
               int *chunk_to_stripe_map, int *chunk_to_stripe_chunk_map, int num_stripe, int caso_num_rele_chunks, int *caso_rele_chunk_index);

extern void system_partial_stripe_writes(int *io_matrix, int *accessed_stripes, int stripe_count, int *total_write_block_num);

extern int psw_time_caso(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map, 
	int *chunk_to_stripe_chunk_map, double *time);

extern int psw_time_striping(char *trace_name, char given_timestamp[], double *time);

extern int psw_time_continugous(char *trace_name, char given_timestamp[], double *time);

extern int degraded_reads(int *io_request, int *stripe_id_array, int stripe_count, int *num_extra_io, int if_continugous);

extern void system_parallel_reads(int *io_matrix, int *accessed_stripes, int stripe_count, int *total_write_block_num);

extern void dr_time_caso(char *trace_name, char given_timestamp[], int *chunk_to_stripe_map, int *chunk_to_stripe_chunk_map, 
	int *num_extra_io, double *time, int start_evlat_num);

extern void dr_time_striping(char *trace_name, char given_timestamp[], int *num_extra_io, double *time, int start_evlat_num);

extern void dr_time_continugous(char *trace_name, char given_timestamp[], int *num_extra_io, double *time, int start_evlat_num);

extern void sorting_trace_access_pattern();

extern void binary_search(int array[], int num, int value);

