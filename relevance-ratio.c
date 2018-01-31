
// this program reads read patterns from workloads and measure the ratio 
// (#the number of blocks requested with other blocks in more than 2 I/O operations)/(# the number of accessed blocks in a workload)


#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>

#define inf 999999999
#define dft_aces_blk_num 20000
#define num_blk_per_aces 100
#define block_size 4096


int access_pattern[num_blk_per_aces]; 
int total_access_pattern[dft_aces_blk_num]; // it record the access blocks by strictly following the trace
int rlvc_mtrx[dft_aces_blk_num*dft_aces_blk_num]; // it record the relevance among all the accessed blocks
int cur_rcd_idx; // it record the current number of access blocks recorded

int total_access_freq[dft_aces_blk_num]; // it records the access frequency of accessed blocks



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


void read_trace_pattern(char *trace_name){

    //read the data from csv file
    FILE *fp;
	FILE *output_fp;

    if((fp=fopen(trace_name,"r"))==NULL){
  	  printf("open file failed\n");
	  exit(0);
  	  }

	/* generate the output file name */
	char output_file[100];
	char temp[10]="_result";
	strcpy(output_file,trace_name);
	strcat(output_file,temp);

	printf("output_file_name=%s\n",output_file);

	output_fp=fopen(output_file,"w");
	
     char operation[100];

	 char timestamp[100];
	 char op_type[10];
	 char offset[20];
	 char size[10];
	 char divider='\t';

	 int i,j;
	 int k,h;

	 
	 char pre_timestamp[100];
	 int num_timestamp;
	 int num_read_op, num_write_op;
	 int access_start_block, access_end_block;
	 int cur_index;

	 int count;

	 long long *size_int;
	 long long *offset_int;
	 long long a,b;
	 a=0LL;
	 b=0LL;
	 offset_int=&a;
	 size_int=&b;

     cur_index=0;
	 count=0;

	 int total_accessed_blocks_num=0;
	 
     while(fgets(operation, sizeof(operation), fp)) {

		count++;

		new_strtok(operation,divider, timestamp);
		new_strtok(operation,divider, op_type);
		new_strtok(operation,divider, offset);
		new_strtok(operation,divider, size);

	    printf("\n\n\ntimestamp=%s, op_type=%s, offset=%s, size=%s\n", timestamp, op_type, offset, size);

		// analyze the access pattern 
		// if it is accessed in the same timestamp

		if(strcmp(pre_timestamp,timestamp)==0){

			//printf("====same timestamp====\n");

			trnsfm_char_to_int(offset, offset_int);
			access_start_block=(*offset_int)/block_size;

			trnsfm_char_to_int(size, size_int);
			access_end_block=(*offset_int+*size_int)/block_size;

			printf("access_start_block=%d, access_end_block=%d\n", access_start_block, access_end_block); 

			for(i=access_start_block; i<=access_end_block; i++)
				access_pattern[cur_index++]=i;

			}


		else {

			//printf("+++++ different timestamp, cur_index=%d\n",cur_index);

			// update the total_access_pattern by adding new accessed blocks 
			for(i=0;i<cur_index;i++){

				for(j=0;j<cur_rcd_idx;j++){

					if(total_access_pattern[j]==access_pattern[i])
						break;

					}

				if(j>=cur_rcd_idx)
					total_access_pattern[cur_rcd_idx++]=access_pattern[i];

				}


            // record the relevance between any two access blocks in the same timestamp
	
            for(i=0;i<cur_index-1;i++){

				for(j=i+1;j<cur_index;j++){

					if(access_pattern[i]!=access_pattern[j]){ 

                        // locate the position of block access_pattern[i] and block access_pattern[j] in the array@total_access_pattern
                        
						for(k=0;k<cur_rcd_idx;k++)
							if(total_access_pattern[k]==access_pattern[i]) 
								break;

						for(h=0;h<cur_rcd_idx;h++)
							if(total_access_pattern[h]==access_pattern[j])
								break;


						// printf("k=%d, h=%d, cur_index=%d, cur_rcd_idx=%d\n",k,h, cur_index, cur_rcd_idx);

						rlvc_mtrx[k*dft_aces_blk_num+h]++;
						rlvc_mtrx[h*dft_aces_blk_num+k]++;


						}
					}
            	}


			// initialize the access_pattern array for the next timestamp

			for(i=0;i<num_blk_per_aces;i++)
				access_pattern[i]=0;

			total_accessed_blocks_num+=cur_index;

			cur_index=0;

            // record this patter at the new timestamp 
           
			trnsfm_char_to_int(offset, offset_int);
			access_start_block=(*offset_int)/block_size;

			trnsfm_char_to_int(size, size_int);
			access_end_block=(*offset_int+*size_int)/block_size;

			//printf("access_start_block=%d, access_end_block=%d\n", access_start_block, access_end_block); 

			for(i=access_start_block; i<=access_end_block; i++)
				access_pattern[cur_index++]=i;

			//printf("------>pre_timestamp=%s, timestamp=%s\n", pre_timestamp, timestamp);

			strcpy(pre_timestamp, timestamp);

			//printf("<------pre_timestamp=%s, timestamp=%s\n", pre_timestamp, timestamp);
			printf("cur_rcd_idx=%d, total_accessed_blocks_num=%d\n",cur_rcd_idx, total_accessed_blocks_num);

			}
	   
       }

	 close(fp);
	 close(output_fp);
	
}



int main(int argc, char *argv[]){

  if(argc!=2){
  	printf("./s trace_name!\n");
	exit(1);
  	}

  int total_block_num; 

  int *max_block_id;
  int *min_block_id;

  int a=0, b=inf;
  int access_blocks;

  int i,j;

  cur_rcd_idx=0;

  for(i=0;i<dft_aces_blk_num*dft_aces_blk_num;i++)
  	rlvc_mtrx[i]=0;

  read_trace_pattern(argv[1]);
 
}

