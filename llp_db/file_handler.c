#include "file_structs.h"
#include "file_handler.h"
#include "filters.h"
#include "format_translators.h"
#include "file_handler_inner_structs.h"

//#include "common_structs.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define MAX_RESULT_BUFFER_SIZE 4096


FILE* open_db_file(char* filename) {
	FILE* file = fopen(filename, "rb+");
	return file;
}

FILE* create_db_file(char* filename) {
	FILE* file = fopen(filename, "wb+");
	return file;
}

uint32_t read_from_db_file(struct File_Handle* f_handle, uint32_t offset, uint32_t size, void* buffer) {
	int fseek_res = fseek(f_handle->file, offset, SEEK_SET);
	if (fseek_res != 0) {
		return -1;
	}
	return fread(buffer, 1, size, f_handle->file);
}

uint32_t write_into_db_file(struct File_Handle* f_handle, uint32_t offset, uint32_t size, void* buffer) {
	int fseek_res = fseek(f_handle->file, offset, SEEK_SET);
	if (fseek_res != 0) {
		return -1;
	}
	size_t fwrite_res = fwrite(buffer, 1, size, f_handle->file);
	if (fwrite_res != size) {
		return -1;
	}
	return 0;
}

uint32_t find_file_end(struct File_Handle* f_handle) {
	//printf("find_file_end\n");
	fseek(f_handle->file, 0, SEEK_END);
	return ftell(f_handle->file);
}

uint32_t write_init_file_header(FILE* f) {
	struct File_Header f_header = (struct File_Header){
		.first_gap_offset = -1,
		.last_gap_offset = -1,
		.gap_sz = 0,
		.signature = DB_FILE_SIGNATURE,
		.tables_number = 0,
		.first_table_offset = -1,
		.last_table_offset = -1
	};
	size_t result = fwrite(&f_header, sizeof(struct File_Header), 1, f);
	if (result != 1) {
		return -1;
	}
	return 0;
}

void close_db_file(struct File_Handle* f_handle, uint8_t preserve_f_handle) {
	fclose(f_handle->file);
	if (preserve_f_handle == 0) {
		free(f_handle);
	}
	
}

uint32_t check_db_file_signature(struct File_Handle* f_handle) {
	struct File_Header file_header;
	uint32_t read_res = read_from_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);
	if (read_res < sizeof(struct File_Header)) {
		return -1;
	}
	if (file_header.signature != DB_FILE_SIGNATURE) {
		return -1;
	}
	return 0;
}

struct File_Handle* open_or_create_db_file(char* filename, float critical_gap_rate) {
	FILE* f = open_db_file(filename);
	uint8_t is_created = 0;
	if (f == NULL) {
		f = create_db_file(filename);
		if (f == NULL) {
			return NULL;
		}
		is_created = 1;

	}
	
	struct File_Handle* f_handle = malloc(sizeof(struct File_Handle));
	f_handle->file = f;
	f_handle->critical_gap_rate = critical_gap_rate;
	f_handle->filename = filename;

	if (is_created == 0) {
		
		uint32_t sig_check_res = check_db_file_signature(f_handle);
		if (sig_check_res == -1) {
			close_db_file(f_handle, 0);
			return NULL;
		}
	}
	else {
		uint32_t f_header_write_res = write_init_file_header(f);
		if (f_header_write_res == -1) {
			close_db_file(f_handle, 0);
			return NULL;
		}
	}
	
	return f_handle;
}


struct Table_Handle not_existing_table_handle() {
	return (struct Table_Handle) {
		.exists = 0,
			.table_metadata_offset = -1,
			.table_header = { 0 }
	};
}





void flush_buffer(struct File_Handle* f_handle, uint32_t buffer_left_offset, uint32_t buffer_right_offset, void* buffer) {
	//printf("flush buffer\n");
	if (buffer_right_offset - buffer_left_offset > 0) {
		write_into_db_file(f_handle, buffer_left_offset, buffer_right_offset - buffer_left_offset, buffer);
	}
}



void* fetch_row_into_buffer(struct File_Handle* f_handle,
	void* buffer,
	uint32_t current_row_offset,
	uint32_t* buff_sz,
	uint32_t* buffer_left_offset,
	uint32_t* buffer_right_offset,
	uint32_t* row_header_pos,
	uint32_t* row_sz, uint8_t read_only) {
	struct Row_Header* r_header;
	uint32_t right_bound_row_header_offset = current_row_offset + sizeof(struct Row_Header);
	//printf("current_row_offset: %x, buffer_left_offset: %x, buffer_right_offset: %x\n", current_row_offset, *buffer_left_offset, *buffer_right_offset);
	if ((*buffer_left_offset <= current_row_offset) && (right_bound_row_header_offset <= *buffer_right_offset)) {
		// ROW HEADER IN BUFFER
		*row_header_pos = current_row_offset - *buffer_left_offset;
		r_header = (struct Row_Header*)((uint8_t*)buffer + *row_header_pos);
		*row_sz = r_header->row_size;
		if ((current_row_offset + *row_sz) > *buffer_right_offset) {

			// not the whole row in array
			// should rewrite buffer data
			//printf("not the whole row in array\n");
			if (read_only != 1) {
				flush_buffer(f_handle, *buffer_left_offset, *buffer_right_offset, buffer);

			}

			if (*row_sz > *buff_sz) {
				*buff_sz = *row_sz;
				buffer = realloc(buffer, *buff_sz);
			}

			uint32_t read_res = read_from_db_file(f_handle, current_row_offset, *buff_sz, buffer);
			*buffer_left_offset = current_row_offset;
			*buffer_right_offset = current_row_offset + read_res;
			r_header = (struct Row_Header*)(buffer);
			*row_header_pos = 0;
		}

	}
	else {
		// ROW HEADER NOT IN BUFFER
		//printf("ROW HEADER NOT IN BUFFER\n");
		if (read_only != 1) {
			flush_buffer(f_handle, *buffer_left_offset, *buffer_right_offset, buffer);

		}
		uint32_t read_res = read_from_db_file(f_handle, current_row_offset, *buff_sz, buffer);

		r_header = (struct Row_Header*)(buffer);
		*row_sz = r_header->row_size;
		if (*row_sz > *buff_sz) {
			/*IF ROW IS BIGGER THAN BUFFER*/
			*buff_sz = *row_sz;
			buffer = realloc(buffer, *buff_sz);
			read_res = read_from_db_file(f_handle, current_row_offset, *buff_sz, buffer);
		}

		*buffer_left_offset = current_row_offset;
		*buffer_right_offset = current_row_offset + read_res;
		*row_header_pos = 0;
	}
	return buffer;
}


struct Table_Handle find_table(struct File_Handle* f_handle, struct String table_name) {
	//printf("find_table\n");
	struct File_Header file_header;
	uint32_t read_res = read_from_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);
	if (read_res < sizeof(struct File_Header)) {
		return not_existing_table_handle();
	}
	if (file_header.tables_number == 0) {
		return not_existing_table_handle();
	}
	uint32_t current_table_header_offset = file_header.first_table_offset;
	void* buffer = malloc(DB_MAX_TABLE_METADATA_SIZE);
	uint32_t buffer_left_offset = 0;
	uint32_t buffer_right_offset = 0;
	while (current_table_header_offset != -1) {

		/*ingnore result because there can be less than MAX T M SIZE bytes in file*/
		
		struct Table_Header* table_header;

		uint32_t right_bound_table_header_offset = current_table_header_offset + sizeof(struct Table_Header);

		if ((buffer_left_offset < current_table_header_offset) && (right_bound_table_header_offset < buffer_right_offset)) {
			
			/*current th already inside buffer => dont have to read data from files*/
			uint32_t new_table_header_buffer_position = current_table_header_offset - buffer_left_offset;
			table_header = (struct Table_Header*)((uint8_t*)buffer + new_table_header_buffer_position);

		}
		else {
			uint32_t read_result = read_from_db_file(f_handle, current_table_header_offset, DB_MAX_TABLE_METADATA_SIZE, buffer);
			buffer_left_offset = current_table_header_offset;
			buffer_right_offset = current_table_header_offset + read_result;
			table_header = (struct Table_Header*)buffer;
		}
		if (table_header->table_name_metadata.hash == table_name.hash) {
			char* current_table_name = (uint8_t*)table_header + sizeof(struct Table_Header);
			if (strcmp(current_table_name, table_name.value) == 0) {
				struct Table_Header th = *table_header;
				free(buffer);
				return (struct Table_Handle) {
					.exists = 1,
						.table_metadata_offset = current_table_header_offset,
						.table_header = th
				};
			}
		}

		current_table_header_offset = table_header->next_table_header_offset;
	}

	free(buffer);
	return not_existing_table_handle();
}




struct Copy_Op_Result {
	uint32_t dest_last_tab_offset;
	uint32_t src_next_tab_offset;
};


struct Copy_Op_Result copy_table_to_db_file(struct File_Handle* dest, struct File_Handle* src, uint32_t dest_last_tab_metadata_offset, uint32_t src_metadata_offset) {

	struct File_Header dest_file_header;
	uint32_t read_res = read_from_db_file(dest, 0, sizeof(struct File_Header), &dest_file_header);

	uint32_t pos_to_write_table = find_file_end(dest);

	struct Table_Header src_curr_tab_header;
	read_res = read_from_db_file(src, src_metadata_offset, sizeof(struct Table_Header), &src_curr_tab_header);

	void* tab_metadata_buffer = malloc(src_curr_tab_header.table_metadata_size);
	read_from_db_file(src, src_metadata_offset, src_curr_tab_header.table_metadata_size, tab_metadata_buffer);
	write_into_db_file(dest, pos_to_write_table, src_curr_tab_header.table_metadata_size, tab_metadata_buffer);

	uint32_t pos_to_write_table_rows = pos_to_write_table + src_curr_tab_header.table_metadata_size;

	struct Table_Header dest_curr_tab_header;
	memcpy(&dest_curr_tab_header, &src_curr_tab_header, sizeof(struct Table_Header));

	if (src_curr_tab_header.first_row_offset != -1) { // tab not empty

		dest_curr_tab_header.first_row_offset = pos_to_write_table_rows;


		void* src_buffer = malloc(DB_MAX_ROW_SIZE);
		uint32_t src_buff_sz = DB_MAX_ROW_SIZE;
		uint32_t src_buffer_left_offset = 0;
		uint32_t src_buffer_right_offset = 0;

		uint32_t src_first_gap_offset = -1;
		uint32_t src_last_gap_offset = -1;

		uint32_t src_current_row_offset = src_curr_tab_header.first_row_offset;



		void* dest_buffer = malloc(DB_MAX_ROW_SIZE);
		uint32_t dest_buff_sz = DB_MAX_ROW_SIZE;
		uint32_t dest_buff_pos = 0;

		uint32_t dest_last_row_offset_in_buffer = -1;
		uint32_t dest_last_row_offset_in_file = -1;

		while (src_current_row_offset != -1) {

			/*FETCH CURRENT ROW*/
			uint32_t src_row_sz;
			uint32_t src_row_header_pos;

			src_buffer = fetch_row_into_buffer(src,
				src_buffer,
				src_current_row_offset,
				&src_buff_sz,
				&src_buffer_left_offset,
				&src_buffer_right_offset,
				&src_row_header_pos,
				&src_row_sz, 1);



			struct Row_Header* src_r_header = (struct Row_Header*)((uint8_t*)src_buffer + src_row_header_pos);
			uint32_t src_next_row_offset = src_r_header->next_row_header_offset;
			/*row is in buffer on row_header_pos position*/

			if (dest_last_row_offset_in_buffer != -1) {
				struct Row_Header* dest_last_r_header = (struct Row_Header*)((uint8_t*)dest_buffer + dest_last_row_offset_in_buffer);
				dest_last_r_header->next_row_header_offset = pos_to_write_table_rows + dest_buff_pos;
			}

			if (src_r_header->row_size > (dest_buff_sz - dest_buff_pos)) {
				if (dest_buff_pos != 0) {

					write_into_db_file(dest, pos_to_write_table_rows, dest_buff_pos, dest_buffer);
					pos_to_write_table_rows += dest_buff_pos;
					dest_buff_pos = 0;
				}
				if (src_r_header->row_size > dest_buff_sz) {
					dest_buff_sz = src_r_header->row_size;
					dest_buffer = realloc(dest_buffer, dest_buff_sz);
				}
			}

			struct Row_Header* dest_r_header = (struct Row_Header*)((uint8_t*)dest_buffer + dest_buff_pos);
			memcpy(dest_r_header, src_r_header, src_r_header->row_size);
			dest_r_header->next_row_header_offset = -1;
			dest_r_header->prev_row_header_offset = dest_last_row_offset_in_file;



			dest_last_row_offset_in_buffer = dest_buff_pos;

			dest_last_row_offset_in_file = pos_to_write_table_rows + dest_buff_pos;

			dest_buff_pos += src_r_header->row_size;

			src_current_row_offset = src_next_row_offset;

		}
		write_into_db_file(dest, pos_to_write_table_rows, dest_buff_pos, dest_buffer);


		dest_curr_tab_header.last_row_offset = dest_last_row_offset_in_file;
		/*dest_curr_tab_header.prev_table_header_offset = dest_last_tab_metadata_offset;
		dest_curr_tab_header.next_table_header_offset = -1;*/

		free(src_buffer);
		free(dest_buffer);

	}
	//else {
	//	dest_curr_tab_header.first_row_offset = -1;
	//	dest_curr_tab_header.last_row_offset = -1;
	//	dest_curr_tab_header.prev_table_header_offset = dest_last_tab_metadata_offset;
	//	dest_curr_tab_header.next_table_header_offset = -1;
	//}
	dest_curr_tab_header.prev_table_header_offset = dest_last_tab_metadata_offset;
	dest_curr_tab_header.next_table_header_offset = -1;

	write_into_db_file(dest, pos_to_write_table, sizeof(struct Table_Header), &dest_curr_tab_header);

	struct Table_Header dest_last_tab_header;
	if (dest_last_tab_metadata_offset != -1) {
		read_res = read_from_db_file(dest, dest_last_tab_metadata_offset, sizeof(struct Table_Header), &dest_last_tab_header);
		dest_last_tab_header.next_table_header_offset = pos_to_write_table;
		write_into_db_file(dest, dest_last_tab_metadata_offset, sizeof(struct Table_Header), &dest_last_tab_header);
	}
	free(tab_metadata_buffer);

	dest_file_header.last_table_offset = pos_to_write_table;
	if (dest_file_header.first_table_offset == -1) {
		dest_file_header.first_table_offset = pos_to_write_table;
	}
	dest_file_header.tables_number++;
	write_into_db_file(dest, 0, sizeof(struct File_Header), &dest_file_header);


	return (struct Copy_Op_Result) {
		.dest_last_tab_offset = pos_to_write_table,
			.src_next_tab_offset = src_curr_tab_header.next_table_header_offset
	};
}


void normalize_db_file(struct File_Handle* f_handle, uint8_t preserve_f_handle) {
	printf("norm\n");
	struct File_Handle* new_f_handle = open_or_create_db_file("buffer_file", f_handle->critical_gap_rate);
	struct File_Header src_file_header;
	uint32_t read_res = read_from_db_file(f_handle, 0, sizeof(struct File_Header), &src_file_header);


	uint32_t src_current_tab_metadata_offset = src_file_header.first_table_offset;
	uint32_t dest_last_tab_metadata_offset = -1;

	while (src_current_tab_metadata_offset != -1) {
		struct Copy_Op_Result copy_res = copy_table_to_db_file(new_f_handle, f_handle, dest_last_tab_metadata_offset, src_current_tab_metadata_offset);
		dest_last_tab_metadata_offset = copy_res.dest_last_tab_offset;
		src_current_tab_metadata_offset = copy_res.src_next_tab_offset;
	}

	char* name = f_handle->filename;
	close_db_file(f_handle, preserve_f_handle);
	close_db_file(new_f_handle, 0);
	remove(name);
	rename("buffer_file", name);
	if (preserve_f_handle == 1) {
		f_handle->file = open_db_file(name);
	}
	

}




void close_or_normalize_db_file(struct File_Handle* f_handle, uint8_t normalize) {
	if (normalize == 1) {
		normalize_db_file(f_handle, 0);
	}
	else {
		close_db_file(f_handle, 0);
	}
}




float check_gap_rate(struct File_Handle* f_handle, uint32_t gap_sz) {
	return ((float)gap_sz) / find_file_end(f_handle);
}



void normalize_db_file_after_command(struct File_Handle* f_handle) {
	
	struct File_Header f_header;
	read_from_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);
	float gap_rate = check_gap_rate(f_handle, f_header.gap_sz);
	if (gap_rate >= f_handle->critical_gap_rate) {
		normalize_db_file(f_handle, 1); // only FILE* in f_handle changed beacause file reopened
	}

	
}





/*POTENTIAL BUG, NEEDS TESTING*/
uint32_t find_free_space(struct File_Handle* f_handle, uint32_t size) {
	//printf("find_free_space\n");
	struct File_Header f_header;
	uint32_t read_res = read_from_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);
	if (read_res < sizeof(struct File_Header)) {
		return -1;
	}

	uint32_t curr_gap_offset = f_header.first_gap_offset;
	uint32_t minimal_gap_size = sizeof(struct Row_Header) + sizeof(int32_t);


	void* buffer = malloc(DB_MAX_ROW_SIZE);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;
	uint32_t buffer_left_offset = 0;
	uint32_t buffer_right_offset = 0;

	struct Gap_Header* gap_header;
	uint8_t found = 0;
	while (curr_gap_offset != -1) {

		uint32_t right_bound_gap_header_offset = curr_gap_offset + sizeof(struct Gap_Header);

		if ((buffer_left_offset <= curr_gap_offset) && (right_bound_gap_header_offset <= buffer_right_offset)) {
			// gap header in buf
			uint32_t gh_buffer_pos = curr_gap_offset - buffer_left_offset;
			gap_header = (struct Gap_Header*)((uint8_t*)buffer + gh_buffer_pos);

		}
		else {
			// gap not in buffer
			read_res = read_from_db_file(f_handle, curr_gap_offset, DB_MAX_ROW_SIZE, buffer);
			buffer_left_offset = curr_gap_offset;
			buffer_right_offset = curr_gap_offset + read_res;
			gap_header = (struct Gap_Header*)buffer;
		}

		if ((gap_header->gap_size >= (size + minimal_gap_size)) || (gap_header->gap_size == size)) {
			found = 1;
			break;
		}

		curr_gap_offset = gap_header->next_gap_header_offset;
	}


	if (found == 1) {
		if (gap_header->gap_size >= (size + minimal_gap_size)) {
			// split
			gap_header->gap_size = gap_header->gap_size - size;
			write_into_db_file(f_handle, curr_gap_offset, sizeof(struct Gap_Header), &gap_header);
			/*error handle*/
			uint32_t free_space_offset = curr_gap_offset + gap_header->gap_size;
			free(buffer);
			return free_space_offset;
		}
		if (gap_header->gap_size == size) {
			// take whole gap
			
			if (gap_header->prev_gap_header_offset != -1) {

				uint32_t left_bound_gap_header_offset = gap_header->prev_gap_header_offset;
				uint32_t right_bound_gap_header_offset = gap_header->prev_gap_header_offset + sizeof(struct Gap_Header);
				
				struct Gap_Header* prev_gap_header;
				if ((buffer_left_offset <= left_bound_gap_header_offset) && (right_bound_gap_header_offset <= buffer_right_offset)) {
					uint32_t gh_buff_pos = left_bound_gap_header_offset - buffer_left_offset;
					prev_gap_header = (struct Gap_Header*)((uint8_t*)buffer + gh_buff_pos);
				}
				else {
					read_res = read_from_db_file(f_handle, gap_header->prev_gap_header_offset, DB_MAX_ROW_SIZE, buffer);
					buffer_left_offset = curr_gap_offset;
					buffer_right_offset = curr_gap_offset + read_res;
					prev_gap_header = (struct Gap_Header*)buffer;
				}
				
				prev_gap_header->next_gap_header_offset = gap_header->next_gap_header_offset;
				write_into_db_file(f_handle, gap_header->prev_gap_header_offset, sizeof(struct Gap_Header), prev_gap_header);
			}
			if (gap_header->next_gap_header_offset != -1) {

				uint32_t left_bound_gap_header_offset = gap_header->next_gap_header_offset;
				uint32_t right_bound_gap_header_offset = gap_header->next_gap_header_offset + sizeof(struct Gap_Header);

				struct Gap_Header* next_gap_header;
				if ((buffer_left_offset <= left_bound_gap_header_offset) && (right_bound_gap_header_offset <= buffer_right_offset)) {
					uint32_t gh_buff_pos = left_bound_gap_header_offset - buffer_left_offset;
					next_gap_header = (struct Gap_Header*)((uint8_t*)buffer + gh_buff_pos);
				}
				else {
					read_res = read_from_db_file(f_handle, gap_header->next_gap_header_offset, DB_MAX_ROW_SIZE, buffer);
					buffer_left_offset = curr_gap_offset;
					buffer_right_offset = curr_gap_offset + read_res;
					next_gap_header = (struct Gap_Header*)buffer;
				}

				next_gap_header->prev_gap_header_offset = gap_header->prev_gap_header_offset;
				write_into_db_file(f_handle, gap_header->next_gap_header_offset, sizeof(struct Gap_Header), next_gap_header);
			}

			void* zero_buffer = malloc(gap_header->gap_size);
			memset(zero_buffer, 0, gap_header->gap_size);
			write_into_db_file(f_handle, curr_gap_offset, gap_header->gap_size, zero_buffer);
			free(zero_buffer);
			free(buffer);
			return curr_gap_offset;
		}
		
	}
	free(buffer);
	return -1;
}



uint32_t add_table_to_f_header_and_update_list(struct File_Handle* f_handle, uint32_t table_metadata_offset) {
	//printf("add_table_to_f_header_and_update_list\n");
	struct File_Header file_header;
	uint32_t read_res = read_from_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);
	if (read_res < sizeof(struct File_Header)) {
		return -1;
	}
	

	uint32_t write_res;
	if (file_header.tables_number == 0) {
		file_header.first_table_offset = table_metadata_offset;
	}
	else {
		struct Table_Header prev_t_header;
		read_from_db_file(f_handle, file_header.last_table_offset, sizeof(struct Table_Header), &prev_t_header);
		prev_t_header.next_table_header_offset = table_metadata_offset;
		write_into_db_file(f_handle, file_header.last_table_offset, sizeof(struct Table_Header), &prev_t_header);

		struct Table_Header new_table_header;
		read_from_db_file(f_handle, table_metadata_offset, sizeof(struct Table_Header), &new_table_header);
		new_table_header.prev_table_header_offset = file_header.last_table_offset;
		write_into_db_file(f_handle, table_metadata_offset, sizeof(struct Table_Header), &new_table_header);
		
		
	}
	file_header.tables_number++;
	file_header.last_table_offset = table_metadata_offset;
	write_res = write_into_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);
	//printf("write res: %d\n", write_res);
	if (write_res == -1) {
		return -1;
	}

	return 0;
}

struct Cleared_Row_List {
	uint32_t first_gap_offset;
	uint32_t last_gap_offset;
	uint32_t first_preserved_row_offset;
	uint32_t last_preserved_row_offset;
	uint32_t number_of_rows_deleted;
	uint32_t gap_sz;
};



/*END INSERTION POLICY*/
int32_t create_table(struct File_Handle* f_handle, struct String table_name, struct Table_Schema schema) {
	
	//printf("create_table %s\n", table_name.value);
	if (find_table(f_handle, table_name).exists == 1) {
		//printf("tab ALREADY exists!\n");
		return -1;
	}
	void* table_metadata = transform_table_metadata_into_db_format( table_name, schema);
	//printf("%p tab metadata arr\n", table_metadata);
	uint32_t table_metadata_sz = ((struct Table_Header*)table_metadata)->table_metadata_size;

	uint32_t free_space_ptr = find_file_end(f_handle);
	uint32_t write_res = write_into_db_file(f_handle, free_space_ptr, table_metadata_sz, table_metadata);
	//printf("write res: %d\n", write_res);
	if (write_res == -1) {
		/*error*/
		return -1;
	}
	uint32_t metadata_upd_res = add_table_to_f_header_and_update_list(f_handle, free_space_ptr);
	if (metadata_upd_res == -1) {
		return -1;
	}

	free(table_metadata);
	return 0;
}


/*GAP FILLING POLICY VS END INSERTION POLICY*/
/*END INSERTION POLICY*/
int32_t insert_row(struct File_Handle* f_handle, struct String table_name, struct Data_Row_Node * data_row) {
	
	
	//printf("insert_row\n");
	struct Table_Handle tab_handle = find_table(f_handle, table_name);
	if (tab_handle.exists == 0) {
		printf("tab DOESNT exists, cannot insert!\n");
		return -1;
	}
	void* table_metadata_buffer = malloc(tab_handle.table_header.table_metadata_size);
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);
	
	void* row_data = transform_data_row_into_db_format(table_metadata_buffer, data_row);
	if (row_data == NULL) {
		printf("transformation error!\n");
		return -1;
	}

	struct Row_Header* row_header = (struct Row_Header*)row_data;
	int32_t file_end = find_file_end(f_handle);

	uint32_t last_row_in_table_offset = tab_handle.table_header.last_row_offset;
	if (last_row_in_table_offset != -1) {
		//printf("table wasnt empty\n");
		struct Row_Header last_row_in_table;
		read_from_db_file(f_handle, last_row_in_table_offset, sizeof(struct Row_Header), &last_row_in_table);
		last_row_in_table.next_row_header_offset = file_end;
		write_into_db_file(f_handle, last_row_in_table_offset, sizeof(struct Row_Header), &last_row_in_table);
		
	}
	else {
		/*table was empty*/
		//printf("table empty\n");
		tab_handle.table_header.first_row_offset = file_end;
	}
	
	row_header->prev_row_header_offset = last_row_in_table_offset;
	write_into_db_file(f_handle, file_end, row_header->row_size, row_data);

	tab_handle.table_header.last_row_offset = file_end;
	write_into_db_file(f_handle, tab_handle.table_metadata_offset, sizeof(struct Table_Header), &(tab_handle.table_header));

	free(table_metadata_buffer);
	free(row_data);
}






struct Update_Set {
	uint32_t columns_num;
	uint32_t updated_columns_num;
	uint8_t* updated_columns_map;
	struct Data_Row_Node** updated_data;
};

struct Update_Set invalid_update_set() {
	return (struct Update_Set) {
		.columns_num = 0,
		.updated_columns_num = 0,
		.updated_columns_map = NULL,
		.updated_data = NULL
	};
}

struct Update_Set prepare_updates(void* table_metadata_buffer, struct Data_Row_Node* new_data) {

	struct Table_Header* t_header = (struct Table_Header*)table_metadata_buffer;
	uint8_t* columns_updated = malloc(t_header->columns_number * sizeof(uint8_t));
	memset(columns_updated, 0, t_header->columns_number * sizeof(uint8_t));
	struct Data_Row_Node** updated_data = malloc(t_header->columns_number * sizeof(struct Data_Row_Node*));

	struct Data_Row_Node* curr_val_upd = new_data;
	uint32_t updated_columns_num = 0;
	while (curr_val_upd != NULL) {
		updated_columns_num++;
		void* curr_col_pos = (uint8_t*)table_metadata_buffer + sizeof(struct Table_Header) + t_header->table_name_metadata.length + 1;
		uint8_t column_found = 0;
		for (uint32_t i = 0; i < t_header->columns_number; i++)
		{
			if (columns_updated[i] == 1) {
				continue;
			}
			
			struct Column_Header* col_header = (struct Column_Header*)curr_col_pos;
			if (curr_val_upd->column_name.hash == col_header->column_name_metadata.hash) {
				char* curr_col_name = (char*)((uint8_t*)curr_col_pos + sizeof(struct Column_Header));
				//printf("currr co; %s --------- cmp with %s\n", curr_col_name, curr_val_upd->column_name.value);
				if (strcmp(curr_col_name, curr_val_upd->column_name.value) == 0) {
					
					if (col_header->data_type != curr_val_upd->value.data_type) {
						printf("INVALID UPDATED VALUE TYPE\n");
						free(columns_updated);
						free(updated_data);
						return invalid_update_set();
					}
					columns_updated[i] = 1;
					updated_data[i] = curr_val_upd;
					column_found = 1;
					break;
				}

			}
			curr_col_pos = (uint8_t*)curr_col_pos + sizeof(struct Column_Header) + col_header->column_name_metadata.length + 1;

		}
		if (column_found == 0) {
			/*if you're here, the column wasnt found or its new value is already set*/
			printf("INVALID COLUMN TO UPDATE\n");
			free(columns_updated);
			free(updated_data);
			return invalid_update_set();
		}
		curr_val_upd = curr_val_upd->next_node;

	}

	//free(columns_updated);
	return (struct Update_Set) {
		.columns_num = t_header->columns_number,
			.updated_columns_num = updated_columns_num,
			.updated_columns_map = columns_updated,
			.updated_data = updated_data
	};
}

void write_outer_format_value_into_buffer(void* buffer, uint32_t* buff_sz, uint32_t* position, enum DB_Data_Type data_type, struct Schema_Internals_Value value){
	uint32_t bytes_num_to_write;
	if (data_type == STRING) {
		bytes_num_to_write = sizeof(struct String_Metadata) + value.value.db_string.length + 1;

	}
	if (data_type == INT) {
		bytes_num_to_write = sizeof(int32_t);

	}
	if (data_type == FLOAT) {
		bytes_num_to_write = sizeof(float);

	}
	if (data_type == BOOL) {
		bytes_num_to_write = sizeof(enum Boolean);

	}

	if (bytes_num_to_write + *position > *buff_sz) {
		/*realloc*/
		*buff_sz = *buff_sz + bytes_num_to_write + *buff_sz / 2;
		buffer = realloc(buffer, *buff_sz);
	}

	uint8_t* place_to_wr = (uint8_t*)buffer + *position;
	if (data_type == STRING) {
		((struct String_Metadata*)place_to_wr)->hash = value.value.db_string.hash;
		((struct String_Metadata*)place_to_wr)->length = value.value.db_string.length;
		memcpy(place_to_wr + sizeof(struct String_Metadata),value.value.db_string.value, value.value.db_string.length + 1); // + "\0"
	}
	if (data_type == INT) {
		*((int32_t*)place_to_wr) = value.value.db_integer;

	}
	if (data_type == FLOAT) {
		*((float*)place_to_wr) =value.value.db_float;

	}
	if (data_type == BOOL) {
		*((enum Boolean*)place_to_wr) = value.value.db_boolean;

	}

	*position += bytes_num_to_write;
}


void copy_inner_format_value_into_buffer(void* buffer, uint32_t* buff_sz, uint32_t* dest_position, uint32_t* src_position, enum DB_Data_Type data_type, void* value) {
	uint32_t bytes_num_to_write = calc_inner_format_value_sz(data_type, value);
	
	if (bytes_num_to_write + *dest_position > *buff_sz) {
		/*realloc*/
		*buff_sz = *buff_sz + bytes_num_to_write + *buff_sz / 2;
		buffer = realloc(buffer, *buff_sz);
	}

	uint8_t* place_to_wr = (uint8_t*)buffer + *dest_position;
	
	memcpy(place_to_wr, value, bytes_num_to_write); 
	
	*dest_position += bytes_num_to_write;
	*src_position += bytes_num_to_write;
}


void* create_updated_row(void* tab_metadata_buffer, void* row, struct Update_Set prepared_updates) {
	
	struct Table_Header* t_header = (struct Table_Header*)tab_metadata_buffer;
	
	struct Row_Header* prev_row_header = (struct Row_Header*)row;
	
	void* buffer = malloc(prev_row_header->row_size);
	uint32_t buff_sz = prev_row_header->row_size;
	memset(buffer, 0, prev_row_header->row_size);
	
	struct Row_Header* upd_row_header = (struct Row_Header*)buffer;
	upd_row_header->next_row_header_offset = prev_row_header->next_row_header_offset;
	upd_row_header->prev_row_header_offset = prev_row_header->prev_row_header_offset;
	
	uint32_t prev_position = sizeof(struct Row_Header); // in old row
	uint32_t new_position = sizeof(struct Row_Header); // in new row
	uint32_t metadata_position = sizeof(struct Table_Header) + t_header->table_name_metadata.length + 1;

	int32_t row_diff = 0;
	for (uint32_t i = 0; i < t_header->columns_number; i++)
	{
		struct Column_Header* c_header = (struct Column_Header*)((uint8_t*)tab_metadata_buffer + metadata_position);
		if (prepared_updates.updated_columns_map[i] == 0) {
			// just copy val
			copy_inner_format_value_into_buffer(
				buffer,
				&buff_sz,
				&new_position, //updated
				&prev_position, // updated
				c_header->data_type,
				(uint8_t*)row + prev_position);

		}
		else {
			/*create new val*/
			write_outer_format_value_into_buffer(
				buffer,
				&buff_sz,
				&new_position, //updated
				c_header->data_type,
				prepared_updates.updated_data[i]->value);
			// skip current col val in old row
			uint32_t bytes_to_skip = calc_inner_format_value_sz(c_header->data_type, (uint8_t*)row + prev_position);
			prev_position += bytes_to_skip;
		}
		
		metadata_position = metadata_position + sizeof(struct Column_Header) + c_header->column_name_metadata.length + 1;
	}


	upd_row_header->row_size = new_position;
	return buffer;
}



/*END INSERTION POLICY*/
int32_t update_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition, struct Data_Row_Node* new_data, uint8_t allow_normalization) {
	//printf("Update_rows \n");
	struct Table_Handle tab_handle = find_table(f_handle, table_name);
	if (tab_handle.exists == 0) {
		//printf("tab DOESNT exists, cannot update rows!\n");
		return -1;
	}
	void* table_metadata_buffer = malloc(tab_handle.table_header.table_metadata_size);
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);

	struct Update_Set prepared_updates = prepare_updates(table_metadata_buffer, new_data);
	if (prepared_updates.updated_columns_map == NULL) {
		//printf("invalid update data\n");
		free(table_metadata_buffer);
		return -1;
	}

	void* buffer = malloc(DB_MAX_ROW_SIZE);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;
	uint32_t buffer_left_offset = 0;
	uint32_t buffer_right_offset = 0;

	uint32_t first_gap_offset = -1;
	uint32_t last_gap_offset = -1;

	int32_t number_of_rows_updated = 0;

	uint32_t current_row_offset = tab_handle.table_header.first_row_offset;

	uint32_t gaps_sz = 0;

	while (current_row_offset != -1) {

		/*FETCH CURRENT ROW*/
		uint32_t row_sz;
		uint32_t row_header_pos;

		buffer = fetch_row_into_buffer(f_handle,
			buffer,
			current_row_offset,
			&buff_sz,
			&buffer_left_offset,
			&buffer_right_offset,
			&row_header_pos,
			&row_sz, 0);

		

		struct Row_Header* r_header = (struct Row_Header*)((uint8_t*)buffer + row_header_pos);
		uint32_t next_row_offset = r_header->next_row_header_offset;
		/*row is in buffer on row_header_pos position*/

		/*APPLY FILTER*/
		uint8_t row_suitable = apply_filter(table_metadata_buffer, (uint8_t*)buffer + row_header_pos, condition);

		if (row_suitable == 1) {
			// should update
			number_of_rows_updated++;
			void* new_row = create_updated_row(table_metadata_buffer, (uint8_t*)buffer + row_header_pos, prepared_updates);
			struct Row_Header* upd_row_header = (struct Row_Header*)new_row;
			
			if ((upd_row_header->row_size == r_header->row_size) || (upd_row_header->row_size + sizeof(struct Gap_Header) <= r_header->row_size)) {
				// updated row can be leaved on its place

				if (upd_row_header->row_size == r_header->row_size) {
					memcpy((uint8_t*)buffer + row_header_pos, new_row, r_header->row_size);
				} else {
					uint32_t gap_sz = r_header->row_size - upd_row_header->row_size;
					gaps_sz += gap_sz;
					memcpy((uint8_t*)buffer + row_header_pos, new_row, upd_row_header->row_size);
					
					void* gap_start = (uint8_t*)buffer + row_header_pos + upd_row_header->row_size;
					memset(gap_start, 0, gap_sz);

					struct Gap_Header* g_header = (struct Gap_Header*)gap_start;
					g_header->prev_gap_header_offset = last_gap_offset;
					g_header->next_gap_header_offset = -1;
					g_header->gap_size = gap_sz;
					
					uint32_t new_gap_offset = row_header_pos + upd_row_header->row_size + buffer_left_offset;

					if (first_gap_offset == -1) {
						first_gap_offset = new_gap_offset;
					}
					else {
						// set next for prev gap
						uint32_t last_gap_header_pos;
						uint32_t last_gap_sz;
						/*WE CAN do that because Row and Gap headers are same*/
						buffer = fetch_row_into_buffer(f_handle,
							buffer,
							last_gap_offset,
							&buff_sz,
							&buffer_left_offset,
							&buffer_right_offset,
							&last_gap_header_pos,
							&last_gap_sz, 0);
						struct Gap_Header* last_gap_header = (struct Gap_Header*)((uint8_t*)buffer + last_gap_header_pos);
						last_gap_header->next_gap_header_offset = new_gap_offset;
					}

					last_gap_offset = new_gap_offset;

				}
				
			}
			else {
				// have to write to another place (end of file)
				// create new gap
				memset((uint8_t*)buffer + row_header_pos, 0, row_sz);
				struct Gap_Header* g_header = (struct Gap_Header*)((uint8_t*)buffer + row_header_pos);
				g_header->gap_size = row_sz;
				g_header->prev_gap_header_offset = last_gap_offset;
				g_header->next_gap_header_offset = -1;

				gaps_sz += row_sz;

				uint32_t new_gap_offset = current_row_offset;
				if (first_gap_offset == -1) {
					first_gap_offset = new_gap_offset;
				}
				else {
					// set next for prev gap
					uint32_t last_gap_header_pos;
					uint32_t last_gap_sz;
					/*WE CAN do that because Row and Gap headers are same*/
					buffer = fetch_row_into_buffer(f_handle,
						buffer,
						last_gap_offset,
						&buff_sz,
						&buffer_left_offset,
						&buffer_right_offset,
						&last_gap_header_pos,
						&last_gap_sz, 0);
					struct Gap_Header* last_gap_header = (struct Gap_Header*)((uint8_t*)buffer + last_gap_header_pos);
					last_gap_header->next_gap_header_offset = new_gap_offset;
				}
				last_gap_offset = new_gap_offset;

				// write updated row to the end of file
				uint32_t file_end = find_file_end(f_handle);
				write_into_db_file(f_handle, file_end, upd_row_header->row_size, new_row);
				
				//update next and prev for neighbours
				if (upd_row_header->prev_row_header_offset != -1) {
					uint32_t prev_row_sz;
					uint32_t prev_row_header_pos;

					buffer = fetch_row_into_buffer(f_handle,
						buffer,
						upd_row_header->prev_row_header_offset,
						&buff_sz,
						&buffer_left_offset,
						&buffer_right_offset,
						&prev_row_header_pos,
						&prev_row_sz, 0);

					struct Row_Header* prev_r_header = (struct Row_Header*)((uint8_t*)buffer + prev_row_header_pos);
					prev_r_header->next_row_header_offset = file_end;
				}

				if (upd_row_header->next_row_header_offset != -1) {
					uint32_t next_row_sz;
					uint32_t next_row_header_pos;

					buffer = fetch_row_into_buffer(f_handle,
						buffer,
						upd_row_header->next_row_header_offset,
						&buff_sz,
						&buffer_left_offset,
						&buffer_right_offset,
						&next_row_header_pos,
						&next_row_sz, 0);

					struct Row_Header* next_r_header = (struct Row_Header*)((uint8_t*)buffer + next_row_header_pos);
					next_r_header->prev_row_header_offset = file_end;
				}

				/*table consistency*/
				if (current_row_offset == tab_handle.table_header.first_row_offset) {
					tab_handle.table_header.first_row_offset = file_end;
				}
				if (current_row_offset == tab_handle.table_header.last_row_offset) {
					tab_handle.table_header.last_row_offset = file_end;
				}


			}
			free(new_row);
		}
		current_row_offset = next_row_offset;

	}
	flush_buffer(f_handle, buffer_left_offset, buffer_right_offset, buffer);

	/*write table header with updated 1 and -1 row offsets*/
	write_into_db_file(f_handle, tab_handle.table_metadata_offset, sizeof(struct Table_Header), &tab_handle.table_header);

	/*manage_gaps & write file header*/

	if (first_gap_offset != -1) {
		/*have to change gap list cause of new gaps*/
		struct File_Header f_header;
		read_from_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);
		f_header.gap_sz += gaps_sz;
		if (f_header.first_gap_offset == -1) {
			f_header.first_gap_offset = first_gap_offset;
		}
		else {
			/*should fetch prev last gap header from db and set ins next = first gap created from curr table*/
			struct Gap_Header prev_last_g_header;
			read_from_db_file(f_handle, f_header.last_gap_offset, sizeof(struct Gap_Header), &prev_last_g_header);
			prev_last_g_header.next_gap_header_offset = first_gap_offset;
			write_into_db_file(f_handle, f_header.last_gap_offset, sizeof(struct Gap_Header), &prev_last_g_header);

			/*should fetch first created gap header and set its prev*/
			struct Gap_Header first_created_g_header;
			read_from_db_file(f_handle, first_gap_offset, sizeof(struct Gap_Header), &first_created_g_header);
			first_created_g_header.prev_gap_header_offset = f_header.last_gap_offset;
			write_into_db_file(f_handle, first_gap_offset, sizeof(struct Gap_Header), &first_created_g_header);

		}
		f_header.last_gap_offset = last_gap_offset;
		write_into_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);

	}

	free(buffer);
	free(table_metadata_buffer);

	if (allow_normalization == 1) {
		normalize_db_file_after_command(f_handle);
	}
	

	return number_of_rows_updated;
	
}




/*JOIN TAB VALIDATOR -- ? */

void get_value_of_column(struct Table_Handle* tab_handle, 
												void* table_metadata, 
												void* row, 
												struct String target_column,
												struct Schema_Internals_Value* place_to_write_col_val) {
	
	uint32_t column_val_offset_in_row = sizeof(struct Row_Header);
	uint32_t col_metadata_offset = sizeof(struct Table_Header) + tab_handle->table_header.table_name_metadata.length + 1;

	for (uint32_t i = 0; i < tab_handle->table_header.columns_number; i++)
	{
		struct Column_Header* c_header = (struct Column_Header*)((uint8_t*)table_metadata + col_metadata_offset);
		if (c_header->column_name_metadata.hash == target_column.hash) {
			char* curr_col_name = (char*)c_header + sizeof(struct Column_Header);
			if (strcmp(curr_col_name, target_column.value) == 0) {

				place_to_write_col_val->data_type = c_header->data_type;
				if (c_header->data_type == STRING) {
					struct String_Metadata* str_metadata = (struct String_Metadata*)((uint8_t*)row + column_val_offset_in_row);
					/*char* str = malloc(str_metadata->length + 1);
					memcpy(str, ((uint8_t*)row + column_val_offset_in_row), str_metadata->length + 1)*/
					place_to_write_col_val->value.db_string.hash = str_metadata->hash;
					place_to_write_col_val->value.db_string.length = str_metadata->length;
					place_to_write_col_val->value.db_string.value = (uint8_t*)row + column_val_offset_in_row + sizeof(struct String_Metadata);
				}
				if (c_header->data_type == INT) {
					
					place_to_write_col_val->value.db_integer = *(int32_t*)((uint8_t*)row + column_val_offset_in_row);
				}
				if (c_header->data_type == FLOAT) {

					place_to_write_col_val->value.db_float = *(float*)((uint8_t*)row + column_val_offset_in_row);
				}
				if (c_header->data_type == BOOL) {

					place_to_write_col_val->value.db_boolean = *(enum Boolean*)((uint8_t*)row + column_val_offset_in_row);
				}
				
				return;
				
			}
		}
		column_val_offset_in_row += calc_inner_format_value_sz(c_header->data_type, (uint8_t*)row + column_val_offset_in_row);
		col_metadata_offset = col_metadata_offset + sizeof(struct Column_Header) + c_header->column_name_metadata.length + 1;
	}
}


struct Table_Row_Bunch* table_chain_single_recursive_select(struct File_Handle* f_handle,
	int32_t number_of_joined_tables,
	struct String* table_names,
	struct Join_Condition* join_conditions,
	struct Table_Handle* tab_handles, void** table_metadata_buffers,
	struct Condition** conditions_on_single_tables,
	struct Condition* join_condition_on_current_table,
	struct Row_Header** current_row_chain,
	uint32_t* cursor_offsets,
	uint32_t cur_tab_index,
	uint32_t max_row_num) {



	struct Condition final_condition_on_current_table = create_complex_condition(join_condition_on_current_table,
		conditions_on_single_tables[cur_tab_index],
		AND);
	uint32_t current_row_offset = cursor_offsets[cur_tab_index];


	if (current_row_offset == -1) {
		//tab is empty
		//printf("tab tail is empty!\n");
		return NULL;
	}


	uint32_t local_number_of_rows_selected = 0;
	uint32_t total_number_of_rows_selected = 0;
	uint32_t row_sz_sum = 0;

	void* result_buffer = malloc(MAX_RESULT_BUFFER_SIZE);
	uint32_t result_buffer_sz = MAX_RESULT_BUFFER_SIZE;
	uint32_t result_buffer_position = 0;

	//struct Row_Header** row_pointers_buffer = malloc(sizeof(struct Row_Header*) * max_row_num);
	uint32_t* row_starts_in_buffer = malloc(sizeof(uint32_t) * max_row_num);

	struct Table_Row_Bunch** row_tails_pointers_buffer = NULL;
	if (cur_tab_index != (number_of_joined_tables - 1)) { // not the last table
		row_tails_pointers_buffer = malloc(sizeof(struct Row_Header*) * max_row_num);
	}



	void* buffer = malloc(DB_MAX_ROW_SIZE);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;
	uint32_t buffer_left_offset = 0;
	uint32_t buffer_right_offset = 0;

	uint32_t next_row_offset;
	while ((current_row_offset != -1) && (total_number_of_rows_selected < max_row_num)) {

		uint32_t row_sz;
		uint32_t row_header_pos;

		buffer = fetch_row_into_buffer(f_handle,
			buffer,
			current_row_offset,
			&buff_sz,
			&buffer_left_offset,
			&buffer_right_offset,
			&row_header_pos,
			&row_sz, 1);
		struct Row_Header* r_header = (struct Row_Header*)((uint8_t*)buffer + row_header_pos);
		next_row_offset = r_header->next_row_header_offset;


		cursor_offsets[cur_tab_index] = next_row_offset; // points next row
		/*row is in buffer on row_header_pos position*/

		/*APPLY FILTER*/
		uint8_t row_suitable = apply_filter(table_metadata_buffers[cur_tab_index], (uint8_t*)buffer + row_header_pos, &final_condition_on_current_table);

		if (row_suitable == 1) {

			if (cur_tab_index != (number_of_joined_tables - 1)) { // not the last tab in chain
				/*try to find row tail in other tables recursively*/
				//max rows selected = calc

				//set it for next tabs to create join condition
				current_row_chain[cur_tab_index] = (struct Row_Header*)((uint8_t*)buffer + row_header_pos);

				uint32_t next_tab_index = cur_tab_index + 1;


				struct Schema_Internals_Value value_to_use_in_join_cond;
				struct Condition join_condition_on_next_table;



				/*NEXT TABLE JOIN COL CONDITION*/
				// a join b on a.x = b.y ; replace a.x with its value and use as b.y condition
				uint32_t related_tab_ind = join_conditions[cur_tab_index].related_table_index; // cur tab idx because there is no join cond on first table
				struct String related_tab_join_col_name = join_conditions[cur_tab_index].related_table_column_name;

				get_value_of_column(tab_handles + related_tab_ind,
					table_metadata_buffers[related_tab_ind],
					current_row_chain[related_tab_ind],
					related_tab_join_col_name,
					&value_to_use_in_join_cond);

				join_condition_on_next_table = create_simple_condition(join_conditions[cur_tab_index].current_table_column_name.value, value_to_use_in_join_cond, EQUALS);


				//reset cursor for the next table (scanning from start)
				cursor_offsets[next_tab_index] = tab_handles[next_tab_index].table_header.first_row_offset;
				struct Table_Row_Bunch* next_tab_res = table_chain_single_recursive_select(f_handle,
					number_of_joined_tables,
					table_names,
					join_conditions,
					tab_handles, table_metadata_buffers,
					conditions_on_single_tables,
					&join_condition_on_next_table,
					current_row_chain,
					cursor_offsets,
					next_tab_index,
					max_row_num - total_number_of_rows_selected); // rows left

				if (next_tab_res == NULL) {
					//empty result set
					// row has no tail -> try to fetch next row
					current_row_offset = next_row_offset;
					continue;

				}


				if (result_buffer_sz < (result_buffer_position + row_sz)) {
					result_buffer_sz = result_buffer_position + row_sz + result_buffer_sz / 2;
					result_buffer = realloc(result_buffer, result_buffer_sz);
				}

				memcpy((uint8_t*)result_buffer + result_buffer_position, (uint8_t*)buffer + row_header_pos, row_sz);
				row_sz_sum += row_sz;

				row_starts_in_buffer[local_number_of_rows_selected] = result_buffer_position;
				//row_pointers_buffer[local_number_of_rows_selected] = (struct Row_Header * )((uint8_t*)result_buffer + result_buffer_position);
				row_tails_pointers_buffer[local_number_of_rows_selected] = next_tab_res;

				local_number_of_rows_selected++;
				total_number_of_rows_selected += next_tab_res->total_fetched_rows_num;

				result_buffer_position += row_sz;

				//current_row_chain[cur_tab_index] = 
				//cursor_offsets[cur_tab_index] = 

			}
			else {
				// last table in chain, try to fetch max rows num
				// just copy them

				/*BUG!!!!!!! CHANGE POINTERS WITH OFFSETS*/
				if (result_buffer_sz < (result_buffer_position + row_sz)) { // bug with row pointers
					result_buffer_sz = result_buffer_position + row_sz + result_buffer_sz / 2;
					result_buffer = realloc(result_buffer, result_buffer_sz);
				}

				memcpy((uint8_t*)result_buffer + result_buffer_position, (uint8_t*)buffer + row_header_pos, row_sz);
				row_sz_sum += row_sz;

				row_starts_in_buffer[local_number_of_rows_selected] = result_buffer_position;
				//row_pointers_buffer[local_number_of_rows_selected] = (struct Row_Header*)((uint8_t*)result_buffer + result_buffer_position);
				result_buffer_position += row_sz;

				local_number_of_rows_selected++;
				total_number_of_rows_selected++;

				//cursor_offsets[cur_tab_index] = 

				//DONT HAVE TO DO THIS BECAUSE OF LAST TAB
				//current_row_chain[cur_tab_index] = 
			}


		}


		current_row_offset = next_row_offset;
	}

	free(buffer);

	if (total_number_of_rows_selected == 0) {
		free(result_buffer);
		//free(row_pointers_buffer);
		free(row_starts_in_buffer);
		free(row_tails_pointers_buffer);
		return NULL; // empty res
	}

	struct Table_Row_Bunch* result = malloc(sizeof(struct Table_Row_Bunch));
	result->total_fetched_rows_num = total_number_of_rows_selected;
	result->local_fetched_rows_num = local_number_of_rows_selected; // use as array length
	result->fetched_rows_buffer = result_buffer;
	//result->row_starts_in_buffer = row_pointers_buffer;
	result->row_starts_in_buffer = row_starts_in_buffer;
	result->row_tails = row_tails_pointers_buffer;
	result->row_sz_sum = row_sz_sum;

	return result;
}


struct Table_Chain_Result_Set* table_chain_select(struct File_Handle* f_handle,
	int32_t number_of_joined_tables,
	struct String* table_names,
	struct Join_Condition* join_conditions,
	struct Condition** conditions_on_single_tables,
	uint32_t* number_of_columns_from_each_table,
	struct String** column_names,
	uint32_t max_row_num) {
	
	//printf("table_chain_select\n");

	struct Table_Handle* tab_handle_array = malloc(sizeof(struct Table_Handle) * number_of_joined_tables);

	uint32_t* cursor_offsets = malloc(sizeof(uint32_t) * number_of_joined_tables);
	memset(cursor_offsets, 0, sizeof(uint32_t) * number_of_joined_tables);

	void** table_metadata_buffers = malloc(sizeof(void*) * number_of_joined_tables);


	//find all tables
	for (uint32_t i = 0; i < number_of_joined_tables; i++)
	{
		
		tab_handle_array[i] = find_table(f_handle, table_names[i]);
		if (tab_handle_array[i].exists == 0) {
			
			//printf("table %s doesnt exist\n", table_names[i].value);
			//printf("join select failed\n");
			free(tab_handle_array);
			free(cursor_offsets);
			for (uint32_t j = 0; j < number_of_joined_tables; j++)
			{
				free(table_metadata_buffers[j]);
			}
			free(table_metadata_buffers);
			return NULL;
		}
		cursor_offsets[i] = tab_handle_array[i].table_header.first_row_offset;
		if (cursor_offsets[i] == -1) {
			//printf("one of the tables is empty -> empty result\n");
			free(tab_handle_array);
			free(cursor_offsets);
			for (uint32_t j = 0; j < number_of_joined_tables; j++)
			{
				free(table_metadata_buffers[j]);
			}
			free(table_metadata_buffers);
			return NULL;
		}
		void* table_metadata_buffer = malloc(tab_handle_array[i].table_header.table_metadata_size);
		read_from_db_file(f_handle, tab_handle_array[i].table_metadata_offset, tab_handle_array[i].table_header.table_metadata_size, table_metadata_buffer);
		table_metadata_buffers[i] = table_metadata_buffer;
		//number_of_columns_from_each_table[i] = (number_of_columns_from_each_table[i] == -1) ? tab_handle_array[i].table_header.columns_number : number_of_columns_from_each_table[i]; // if -1 than bug
	}

	
	struct Row_Header** row_chain_buffer = malloc(sizeof(struct Row_Header*) * number_of_joined_tables); // to check join conditions
	
	struct Table_Row_Bunch* raw_rows_chain = table_chain_single_recursive_select(f_handle,
		number_of_joined_tables,
		table_names,
		join_conditions,
		tab_handle_array, table_metadata_buffers,
		conditions_on_single_tables,
		NULL,
		row_chain_buffer,
		cursor_offsets,
		0,
		max_row_num);

	// if raw_rows_chain == NULL - ??
	if (raw_rows_chain == NULL) {
		for (uint32_t j = 0; j < number_of_joined_tables; j++)
		{
			free(table_metadata_buffers[j]);
		}
		free(table_metadata_buffers);
		free(tab_handle_array);
		free(cursor_offsets);
		return NULL;
	}

	uint32_t fetched_rows_num = raw_rows_chain->total_fetched_rows_num;
	uint8_t probably_has_next = (fetched_rows_num == max_row_num) ? 1 : 0;

	struct Table_Row_Lists_Bunch* rows_chain = transform_row_bunch_into_ram_format(tab_handle_array, table_metadata_buffers, raw_rows_chain,
		0,
		number_of_columns_from_each_table,
		column_names);


	free(row_chain_buffer);

	struct Table_Chain_Result_Set* rs = malloc(sizeof(struct Table_Chain_Result_Set));
	rs->probably_has_next = probably_has_next;
	rs->rows_num = fetched_rows_num;
	rs->number_of_joined_tables = number_of_joined_tables;
	rs->join_conditions = join_conditions;
	rs->table_names = table_names;
	rs->cursor_offsets = cursor_offsets;
	rs->conditions_on_single_tables = conditions_on_single_tables;
	rs->tab_handles = tab_handle_array;
	rs->table_metadata_buffers = table_metadata_buffers;
	rs->rows_chain = rows_chain;
	rs->number_of_selected_columns = number_of_columns_from_each_table; // -1 => all cols
	rs->column_names = column_names;

	return rs;

}






struct Table_Chain_Result_Set* table_chain_get_next(struct File_Handle* f_handle,
	struct Table_Chain_Result_Set * rs,
	uint32_t max_row_num) {

	struct Row_Header** row_chain_buffer = malloc(sizeof(struct Row_Header*) * rs->number_of_joined_tables);

	free_table_row_bunch_struct_list(rs->rows_chain);
	rs->rows_chain = NULL;

	struct Table_Row_Bunch* new_row_bunch = table_chain_single_recursive_select(f_handle,
		rs->number_of_joined_tables,
		rs->table_names,
		rs->join_conditions,
		rs->tab_handles, rs->table_metadata_buffers,
		rs->conditions_on_single_tables,
		NULL,
		row_chain_buffer,
		rs->cursor_offsets,
		0,
		max_row_num);

	if (new_row_bunch == NULL) {
		free_table_chain_result_set_inner_fields(rs);
		///*clear everything created inside inner select function*/
		//free(row_chain_buffer);
		//free(rs->cursor_offsets);
		//free(rs->table_metadata_buffers);
		//free(rs->tab_handles);
		//free(rs);
		///*do not clear conditions, col names etc that were given as a parameter*/
		rs->rows_num = 0;
		rs->probably_has_next = 0;
		return rs;
	}

	uint32_t fetched_rows_num = new_row_bunch->total_fetched_rows_num;
	uint8_t probably_has_next = (fetched_rows_num == max_row_num) ? 1 : 0;

	struct Table_Row_Lists_Bunch* rows_chain = transform_row_bunch_into_ram_format(rs->tab_handles, rs->table_metadata_buffers, new_row_bunch,
		0,
		rs->number_of_selected_columns,
		rs->column_names);

	free(row_chain_buffer);

	// cursor offsets changed
	rs->rows_num = fetched_rows_num;
	rs->rows_chain = rows_chain;
	rs->probably_has_next = probably_has_next;

	return rs;
}


struct Cleared_Row_List clear_row_list_by_condition(struct File_Handle* f_handle, void* table_metadata_buffer, uint32_t first_row_offset, struct Condition* condition) {

	void* buffer = malloc(DB_MAX_ROW_SIZE);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;
	uint32_t buffer_left_offset = 0;
	uint32_t buffer_right_offset = 0;

	uint32_t first_gap_offset = -1;
	uint32_t last_gap_offset = -1;

	uint32_t first_preserved_row_offset = -1;
	uint32_t last_preserved_row_offset = -1;

	int32_t number_of_rows_deleted = 0;

	uint32_t current_row_offset = first_row_offset;

	uint32_t gaps_sz = 0;

	while (current_row_offset != -1) {

		/*FETCH CURRENT ROW*/


		uint32_t row_sz;
		uint32_t row_header_pos;

		buffer = fetch_row_into_buffer(f_handle,
			buffer,
			current_row_offset,
			&buff_sz,
			&buffer_left_offset,
			&buffer_right_offset,
			&row_header_pos,
			&row_sz, 0);

		struct Row_Header* r_header = (struct Row_Header*)((uint8_t*)buffer + row_header_pos);
		/*row is in buffer on row_header_pos position*/

		/*APPLY FILTER*/
		uint8_t row_suitable = apply_filter(table_metadata_buffer, (uint8_t*)buffer + row_header_pos, condition);

		uint32_t next_row_offset = r_header->next_row_header_offset;

		if (!row_suitable) {
			/*DO NOT DELETE*/
			
			if (first_preserved_row_offset == -1) {
				first_preserved_row_offset = current_row_offset;
			}
			else {

				if (last_preserved_row_offset != r_header->prev_row_header_offset) {
					// should fetch last row and set its next = current
					uint32_t last_preserved_row_header_pos;
					uint32_t last_preserved_row_sz;
					buffer = fetch_row_into_buffer(f_handle,
						buffer,
						last_preserved_row_offset,
						&buff_sz,
						&buffer_left_offset,
						&buffer_right_offset,
						&last_preserved_row_header_pos,
						&last_preserved_row_sz, 0);
					struct Row_Header* last_preserved_r_header = (struct Row_Header*)((uint8_t*)buffer + last_preserved_row_header_pos);
					last_preserved_r_header->next_row_header_offset = current_row_offset;
				}


			}
			r_header->prev_row_header_offset = last_preserved_row_offset;
			last_preserved_row_offset = current_row_offset;

		}
		else {
			/*DELETE ROW*/

			uint32_t next_potential_gap_offset = r_header->next_row_header_offset;
			uint32_t prev_row_header_offset = r_header->next_row_header_offset;

			memset((uint8_t*)buffer + row_header_pos, 0, row_sz);


			struct Gap_Header* g_header = (struct Gap_Header*)((uint8_t*)buffer + row_header_pos);
			g_header->gap_size = row_sz;
			g_header->prev_gap_header_offset = last_gap_offset;
			g_header->next_gap_header_offset = next_potential_gap_offset;

			gaps_sz += row_sz;

			if (first_gap_offset == -1) {
				// first deleted row
				first_gap_offset = current_row_offset;
			}
			else {
				if (last_gap_offset != prev_row_header_offset) {
					// should fetch last gap and set its next gap = current
					uint32_t last_gap_header_pos;
					uint32_t last_gap_sz;
					/*WE CAN do that because Row and Gap headers are same*/
					buffer = fetch_row_into_buffer(f_handle,
						buffer,
						last_gap_offset,
						&buff_sz,
						&buffer_left_offset,
						&buffer_right_offset,
						&last_gap_header_pos,
						&last_gap_sz, 0);
					struct Gap_Header* last_gap_header = (struct Gap_Header*)((uint8_t*)buffer + last_gap_header_pos);
					last_gap_header->next_gap_header_offset = current_row_offset;
				}
			}

			last_gap_offset = current_row_offset;

			number_of_rows_deleted++;

		}
		current_row_offset = next_row_offset;

	}
	flush_buffer(f_handle, buffer_left_offset, buffer_right_offset, buffer);
	free(buffer);

	return (struct Cleared_Row_List) {
		.first_gap_offset = first_gap_offset,
			.last_gap_offset = last_gap_offset,
			.first_preserved_row_offset = first_preserved_row_offset,
			.last_preserved_row_offset = last_preserved_row_offset,
			.number_of_rows_deleted = number_of_rows_deleted,
			.gap_sz = gaps_sz
	};

	
}


int32_t delete_table(struct File_Handle* f_handle, struct String table_name, uint8_t allow_normalization) {
	
	//printf("delete_table\n");

	struct Table_Handle t_handle = find_table(f_handle, table_name);
	if (t_handle.exists == 0) {
		//printf("tab doesnt exist\n");
		return -1;
	}

	struct File_Header file_header;
	uint32_t read_res = read_from_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);
	if (read_res < sizeof(struct File_Header)) {
		return -1;
	}
	// consistency of tab metadata list
	if (file_header.first_table_offset == t_handle.table_metadata_offset) {
		file_header.first_table_offset = t_handle.table_header.next_table_header_offset;
	}
	if (file_header.last_table_offset == t_handle.table_metadata_offset) {
		file_header.last_table_offset = t_handle.table_header.prev_table_header_offset;
	}
	file_header.tables_number--;

	if (t_handle.table_header.next_table_header_offset != -1) {
		struct Table_Header next_th;
		read_from_db_file(f_handle, t_handle.table_header.next_table_header_offset, sizeof(struct Table_Header), &next_th);
		next_th.prev_table_header_offset = t_handle.table_header.prev_table_header_offset;
		write_into_db_file(f_handle, t_handle.table_header.next_table_header_offset, sizeof(struct Table_Header), &next_th);
	}
	if (t_handle.table_header.prev_table_header_offset != -1) {
		struct Table_Header prev_th;
		read_from_db_file(f_handle, t_handle.table_header.prev_table_header_offset, sizeof(struct Table_Header), &prev_th);
		prev_th.next_table_header_offset = t_handle.table_header.next_table_header_offset;
		write_into_db_file(f_handle, t_handle.table_header.prev_table_header_offset, sizeof(struct Table_Header), &prev_th);
	}


	/*clear rows*/
	struct Cleared_Row_List crow_list = clear_row_list_by_condition(f_handle, NULL, t_handle.table_header.first_row_offset, NULL);

	/*clear metadata*/
	void* buffer = malloc(t_handle.table_header.table_metadata_size);
	memset(buffer, 0, t_handle.table_header.table_metadata_size);

	struct Gap_Header* g_header = (struct Gap_Header*)buffer;
	g_header->gap_size = t_handle.table_header.table_metadata_size;
	g_header->next_gap_header_offset = crow_list.first_gap_offset;
	g_header->prev_gap_header_offset = -1;

	if (crow_list.first_gap_offset != -1) {

		// any gaps added

		file_header.gap_sz += crow_list.gap_sz; // gap sz in file inc

		struct Gap_Header first_new_gap_header;
		read_from_db_file(f_handle, crow_list.first_gap_offset, sizeof(struct Gap_Header), &first_new_gap_header);

		first_new_gap_header.prev_gap_header_offset = t_handle.table_metadata_offset;
		write_into_db_file(f_handle, crow_list.first_gap_offset, sizeof(struct Gap_Header), &first_new_gap_header);
	}
	else {
		crow_list.last_gap_offset = t_handle.table_metadata_offset;
	}

	/*first gap in new gap list is created from table metadata*/
	crow_list.first_gap_offset = t_handle.table_metadata_offset;


	if (file_header.first_gap_offset == -1) {

		write_into_db_file(f_handle, crow_list.first_gap_offset, t_handle.table_header.table_metadata_size, g_header);

		file_header.first_gap_offset = crow_list.first_gap_offset;
		file_header.last_gap_offset = crow_list.last_gap_offset;

	}
	else {
		/*maintain gap linked list*/
		/*lenghten ll*/
		struct Gap_Header last_gap_header;
		read_from_db_file(f_handle, file_header.last_gap_offset, sizeof(struct Gap_Header), &last_gap_header);
		last_gap_header.next_gap_header_offset = crow_list.first_gap_offset;
		write_into_db_file(f_handle, file_header.last_gap_offset, sizeof(struct Gap_Header), &last_gap_header);

		g_header->prev_gap_header_offset = file_header.last_gap_offset;
		write_into_db_file(f_handle, crow_list.first_gap_offset, t_handle.table_header.table_metadata_size, g_header);

		file_header.last_gap_offset = crow_list.last_gap_offset;

	}


	free(buffer);
	write_into_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);

	if (allow_normalization == 1) {
		normalize_db_file_after_command(f_handle);
	}


	return 0;
}



int32_t delete_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition, uint8_t allow_normalization) {

	struct Table_Handle tab_handle = find_table(f_handle, table_name);
	if (tab_handle.exists == 0) {
		//printf("tab DOESNT exists, cannot delete from!\n");
		return -1;
	}

	void* table_metadata_buffer = malloc(tab_handle.table_header.table_metadata_size);
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);


	struct Cleared_Row_List cleared_row_list = clear_row_list_by_condition(f_handle, table_metadata_buffer, tab_handle.table_header.first_row_offset, condition);


	/*TABLE ROW LINKED LIST CONSISTENCY*/
	tab_handle.table_header.first_row_offset = cleared_row_list.first_preserved_row_offset;
	tab_handle.table_header.last_row_offset = cleared_row_list.last_preserved_row_offset;
	write_into_db_file(f_handle, tab_handle.table_metadata_offset, sizeof(struct Table_Header), &tab_handle.table_header);
	if (cleared_row_list.last_preserved_row_offset != -1) {
		struct Row_Header r_header;
		read_from_db_file(f_handle, cleared_row_list.last_preserved_row_offset, sizeof(struct Row_Header), &r_header);
		if (r_header.next_row_header_offset != -1) {
			r_header.next_row_header_offset = -1;
			write_into_db_file(f_handle, cleared_row_list.last_preserved_row_offset, sizeof(struct Row_Header), &r_header);
		}
	}


	/*GAP LINKED LIST CONSISTENCY*/

	if (cleared_row_list.first_gap_offset != -1) {
		/*have to change gap list cause of new gaps*/
		//printf("we have deleted rows\n");
		struct File_Header f_header;
		read_from_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);
		if (f_header.first_gap_offset == -1) {
			f_header.first_gap_offset = cleared_row_list.first_gap_offset;
		}
		else {
			/*should fetch prev last gap header from db and set ins next = first gap created from curr table*/

			struct Gap_Header prev_last_g_header;
			read_from_db_file(f_handle, f_header.last_gap_offset, sizeof(struct Gap_Header), &prev_last_g_header);
			prev_last_g_header.next_gap_header_offset = cleared_row_list.first_gap_offset;
			write_into_db_file(f_handle, f_header.last_gap_offset, sizeof(struct Gap_Header), &prev_last_g_header);

			/*should fetch first created gap header and set its prev*/
			struct Gap_Header first_created_g_header;
			read_from_db_file(f_handle, cleared_row_list.first_gap_offset, sizeof(struct Gap_Header), &first_created_g_header);
			first_created_g_header.prev_gap_header_offset = f_header.last_gap_offset;
			write_into_db_file(f_handle, cleared_row_list.first_gap_offset, sizeof(struct Gap_Header), &first_created_g_header);

		}
		f_header.last_gap_offset = cleared_row_list.last_gap_offset;

		f_header.gap_sz += cleared_row_list.gap_sz; // gap sz in file inc

		write_into_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);


		struct Gap_Header g_header;
		read_from_db_file(f_handle, cleared_row_list.last_gap_offset, sizeof(struct Gap_Header), &g_header); // last gap offset != -1
		if (g_header.next_gap_header_offset != -1) {
			g_header.next_gap_header_offset = -1;
			write_into_db_file(f_handle, cleared_row_list.last_gap_offset, sizeof(struct Gap_Header), &g_header);
		}

	}


	free(table_metadata_buffer);

	if (allow_normalization == 1) {
		normalize_db_file_after_command(f_handle);
	}


	return cleared_row_list.number_of_rows_deleted;
}






struct File_Table_Schema_Metadata get_table_schema_data(struct File_Handle* f_handle, struct String table_name) {
	
	struct Table_Handle tab_handle = find_table(f_handle, table_name);
	if (tab_handle.exists == 0) {
		//printf("tab DOESNT exists, cannot insert!\n");
		return (struct File_Table_Schema_Metadata ) {.exists = 0};
	}

	void* table_metadata_buffer = malloc(tab_handle.table_header.table_metadata_size);
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);

	uint32_t str_buff_sz = tab_handle.table_header.table_metadata_size - sizeof(struct Table_Header) - tab_handle.table_header.columns_number * sizeof(struct Column_Header);
	void* string_buffer = malloc(str_buff_sz);

	struct Column_Info_Block* columns_metadata = malloc(sizeof(struct Column_Info_Block) * tab_handle.table_header.columns_number);

	uint32_t string_buffer_pos = 0;
	uint32_t table_metadata_buffer_pos = sizeof(struct Table_Header) + tab_handle.table_header.table_name_metadata.length + 1;

	for (uint32_t i = 0; i < tab_handle.table_header.columns_number; i++)
	{
		struct Column_Header* c_header = (struct Column_Header*)((uint8_t*)table_metadata_buffer + table_metadata_buffer_pos);
		columns_metadata[i].column_type = c_header->data_type;
		char* col_name = (char*)c_header + sizeof(struct Column_Header);
		memcpy((uint8_t*)string_buffer + string_buffer_pos, col_name, c_header->column_name_metadata.length + 1);
		columns_metadata[i].column_name.value = (uint8_t*)string_buffer + string_buffer_pos;
		columns_metadata[i].column_name.hash = c_header->column_name_metadata.hash;
		columns_metadata[i].column_name.length = c_header->column_name_metadata.length;

		string_buffer_pos += c_header->column_name_metadata.length + 1;
		table_metadata_buffer_pos += sizeof(struct Column_Header) + c_header->column_name_metadata.length + 1;
	}

	free(table_metadata_buffer);

	return (struct File_Table_Schema_Metadata) {
		.columns_data = columns_metadata,
			.exists = 1,
			.string_buffer = string_buffer,
			.table_name = table_name,
			.columns_number = tab_handle.table_header.columns_number
	};

}