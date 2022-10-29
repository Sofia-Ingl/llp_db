#include "file_structs.h"
#include "file_handler.h"

//#include "common_structs.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

struct File_Handle {
	FILE* file;
};

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
	printf("find_file_end\n");
	fseek(f_handle->file, 0, SEEK_END);
	return ftell(f_handle->file);
}

uint32_t write_init_file_header(FILE* f) {
	struct File_Header f_header = (struct File_Header){
		.first_gap_offset = -1,
		.last_gap_offset = -1,
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

void close_db_file(struct File_Handle* f_handle) {
	fclose(f_handle->file);
	free(f_handle);
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

struct File_Handle* open_or_create_db_file(char* filename) {
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

	if (is_created == 0) {
		
		uint32_t sig_check_res = check_db_file_signature(f_handle);
		if (sig_check_res == -1) {
			fclose(f);
			return NULL;
		}
	}
	else {
		uint32_t f_header_write_res = write_init_file_header(f);
		if (f_header_write_res == -1) {
			fclose(f);
			return NULL;
		}
	}
	
	return f_handle;
}

struct Table_Handle {
	uint8_t exists;
	uint32_t table_metadata_offset;
	struct Table_Header table_header;
};

struct Table_Handle not_existing_table_handle() {
	return (struct Table_Handle) {
		.exists = 0,
			.table_metadata_offset = -1,
			.table_header = { 0 }
	};
}


struct Table_Handle find_table(struct File_Handle* f_handle, struct String table_name) {
	printf("find_table\n");
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
			printf("BUFFER\n");
			/*current th already inside buffer => dont have to read data from files*/
			uint32_t new_table_header_buffer_position = current_table_header_offset - buffer_left_offset;
			table_header = (struct Table_Header*)((uint8_t*)buffer + new_table_header_buffer_position);

		}
		else {
			printf("BUFFER MISS\n");
			uint32_t read_result = read_from_db_file(f_handle, current_table_header_offset, DB_MAX_TABLE_METADATA_SIZE, buffer);
			buffer_left_offset = current_table_header_offset;
			buffer_right_offset = current_table_header_offset + read_result;
			table_header = (struct Table_Header*)buffer;
		}
		printf("tab NAME: %s\n", (char*)((uint8_t*)table_header + sizeof(struct Table_Header)));
		if (table_header->table_name_metadata.hash == table_name.hash) {
			char* current_table_name = (uint8_t*)table_header + sizeof(struct Table_Header);
			if (strcmp(current_table_name, table_name.value) == 0) {
				free(buffer);
				return (struct Table_Handle) {
					.exists = 1,
						.table_metadata_offset = current_table_header_offset,
						.table_header = *table_header
				};
			}
		}

		current_table_header_offset = table_header->next_table_header_offset;
	}

	free(buffer);
	return not_existing_table_handle();
}


uint32_t find_free_space(struct File_Handle* f_handle, uint32_t size) {
	printf("find_free_space\n");
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
		printf("found\n");
		if (gap_header->gap_size >= (size + minimal_gap_size)) {
			// split
			gap_header->gap_size = gap_header->gap_size - size;
			write_into_db_file(f_handle, curr_gap_offset, sizeof(struct Gap_Header), &gap_header);
			/*error handle*/
			uint32_t free_space_offset = curr_gap_offset + gap_header->gap_size;
			printf("free space offset: %d\n", free_space_offset);
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
	printf("not found\n");
	free(buffer);
	return -1;
}

void* transform_table_metadata_into_db_format(struct String table_name, struct Table_Schema schema) {
	printf("transform_table_metadata_into_db_format\n");
	struct String_Metadata table_name_metadata = (struct String_Metadata){
		.hash = table_name.hash,
		.length = table_name.length
	};

	struct Table_Header t_header = (struct Table_Header){
		.columns_number = schema.number_of_columns,
		.first_row_offset = -1,
		.last_row_offset = -1,
		.next_table_header_offset = -1,
		.prev_table_header_offset = -1,
		.table_metadata_size = 0,
		.table_name_metadata = table_name_metadata
	};

	void* buffer = malloc(DB_MAX_TABLE_METADATA_SIZE);
	//printf("%p buff ptr\n", buffer);
	uint32_t buffer_size = DB_MAX_TABLE_METADATA_SIZE;
	//printf("TABLE: %s ; len = %d; hash = %d\n", table_name.value, table_name.length, table_name.hash);
	memcpy(buffer, &t_header, sizeof(struct Table_Header));
	memcpy((uint8_t*)buffer + sizeof(struct Table_Header), table_name.value, table_name.length + 1); // +'\0'
	uint32_t position = sizeof(struct Table_Header) + table_name.length + 1;
	uint32_t header_num = 0;

	//printf("tab end %d position\n", position);

	
	for (uint32_t i = 0; i < schema.number_of_columns; i++)
	{
		struct Column_Info_Block current_column = schema.column_info[i];
		//printf("col: %s ; len = %d; hash = %d\n", current_column.column_name.value, current_column.column_name.length, current_column.column_name.hash);
		struct String_Metadata column_name_metadata = (struct String_Metadata){
			.hash = current_column.column_name.hash,
			.length = current_column.column_name.length
		};
		struct Column_Header c_header = (struct Column_Header){
			.data_type = current_column.column_type,
			.column_name_metadata = column_name_metadata
		};

		if (buffer_size < (position + sizeof(struct Column_Header) + current_column.column_name.length + 1)) {
			/*realloc buffer using upper bound of avg col metadata sz*/
			//printf("realloc\n");
			uint32_t avg_metadata_sz = (header_num == 0) ? buffer_size : (buffer_size/ (header_num - 1)); // - current ; not consider tab metadat
			uint32_t columns_left = schema.number_of_columns - header_num - 1; // - current
			uint32_t curr_col_metadata_sz = sizeof(struct Column_Header) + current_column.column_name.length + 1;
			buffer_size = buffer_size + avg_metadata_sz * columns_left + curr_col_metadata_sz;
			buffer = realloc(buffer, buffer_size);
		}

		memcpy((uint8_t*)buffer + position, &c_header, sizeof(c_header));
		position += sizeof(c_header);
		memcpy((uint8_t*)buffer + position, current_column.column_name.value, current_column.column_name.length + 1);
		position += current_column.column_name.length + 1;
		//printf("%d position", position);

		header_num++;
		
	}

	((struct Table_Header*)buffer)->table_metadata_size = position;

	return buffer;


}


uint32_t add_table_to_f_header_and_update_list(struct File_Handle * f_handle, uint32_t table_metadata_offset) {
	printf("add_table_to_f_header_and_update_list\n");
	struct File_Header file_header;
	uint32_t read_res = read_from_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);
	if (read_res < sizeof(struct File_Header)) {
		return -1;
	}
	

	uint32_t write_res;
	if (file_header.tables_number == 0) {
		printf("no tabs\n");
		file_header.first_table_offset = table_metadata_offset;
	}
	else {
		printf("at least 1 tab in file\n");
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
	printf("write res: %d\n", write_res);
	if (write_res == -1) {
		return -1;
	}

	return 0;
}

struct Gap_List {
	uint32_t first_gap_offset;
	uint32_t last_gap_offset;
};

/*ADD BUFFER OPTIMISATION FOR READ AND WRITE*/
struct Gap_List clear_row_list(struct File_Handle* f_handle, uint32_t first_row_offset) {
	printf("clear_row_list\n");
	uint32_t curr_row_offset = first_row_offset;
	uint32_t first_gap_offset = first_row_offset;
	uint32_t last_gap_offset = -1;
	while (curr_row_offset != -1) {
		
		struct Row_Header row_header;
		read_from_db_file(f_handle, curr_row_offset, sizeof(struct Row_Header), &row_header);
		void* buffer = malloc(row_header.row_size);
		memset(buffer, 0, row_header.row_size);
		struct Gap_Header* gap_header = (struct Gap_Header*)buffer;
		gap_header->gap_size = row_header.row_size;
		gap_header->next_gap_header_offset = row_header.next_row_header_offset;
		gap_header->prev_gap_header_offset = row_header.prev_row_header_offset;

		last_gap_offset = curr_row_offset;

		write_into_db_file(f_handle, curr_row_offset, row_header.row_size, buffer);
		free(buffer);

		curr_row_offset = row_header.next_row_header_offset;
		printf("another row is cleared\n");
	}

	return (struct Gap_List) {
		.first_gap_offset = first_gap_offset,
		.last_gap_offset = last_gap_offset
	};
}

/*GAP FILLING POLICY*/
int32_t create_table(struct File_Handle* f_handle, struct String table_name, struct Table_Schema schema) {
	printf("create_table %s\n", table_name.value);
	if (find_table(f_handle, table_name).exists == 1) {
		printf("tab ALREADY exists!\n");
		return -1;
	}
	void* table_metadata = transform_table_metadata_into_db_format( table_name, schema);
	printf("%p tab metadata arr\n", table_metadata);
	uint32_t table_metadata_sz = ((struct Table_Header*)table_metadata)->table_metadata_size;

	uint32_t free_space_ptr = find_free_space(f_handle, table_metadata_sz);
	if (free_space_ptr == -1) {
		// write to the end of file
		printf("wr to f end\n");
		free_space_ptr = find_file_end(f_handle);
		printf("%x FILE ENDDDDDDDDDDD\n", free_space_ptr);
		uint32_t write_res = write_into_db_file(f_handle, free_space_ptr, table_metadata_sz, table_metadata);
		printf("write res: %d\n", write_res);
		if (write_res == -1) {
			/*error*/
			return -1;
		}
	}
	else {
		// write to free space
		printf("wr to free space\n");
		uint32_t write_res = write_into_db_file(f_handle, free_space_ptr, table_metadata_sz, table_metadata);
		if (write_res == -1) {
			/*error*/
			return -1;
		}
	}
	uint32_t metadata_upd_res = add_table_to_f_header_and_update_list(f_handle, free_space_ptr);
	if (metadata_upd_res == -1) {
		return -1;
	}

	free(table_metadata);
	return 0;
}


int32_t delete_table(struct File_Handle* f_handle, struct String table_name) {
	printf("delete_table\n");

	struct Table_Handle t_handle = find_table(f_handle, table_name);
	if (t_handle.exists == 0) {
		printf("tab doesnt exist\n");
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
	struct Gap_List new_gap_list = clear_row_list(f_handle, t_handle.table_header.first_row_offset);

	printf("new gap list: first %d; last %d\n", new_gap_list.first_gap_offset, new_gap_list.last_gap_offset);

	/*clear metadata*/
	void* buffer = malloc(t_handle.table_header.table_metadata_size);
	memset(buffer, 0, t_handle.table_header.table_metadata_size);
	printf("t_handle.table_header.table_metadata_size %d\n", t_handle.table_header.table_metadata_size);
	
	struct Gap_Header* g_header = (struct Gap_Header*)buffer;
	g_header->gap_size = t_handle.table_header.table_metadata_size;
	g_header->next_gap_header_offset = new_gap_list.first_gap_offset;
	g_header->prev_gap_header_offset = -1;

	if (new_gap_list.first_gap_offset != -1) {
	
		struct Gap_Header first_new_gap_header;
		read_from_db_file(f_handle, new_gap_list.first_gap_offset, sizeof(struct Gap_Header), &first_new_gap_header);
		
		first_new_gap_header.prev_gap_header_offset = t_handle.table_metadata_offset;
		printf("new_gap_list.first_gap_offset %d \n", new_gap_list.first_gap_offset);
		write_into_db_file(f_handle, new_gap_list.first_gap_offset, sizeof(struct Gap_Header), &first_new_gap_header);
	}
	else {
		printf("no rows were in table AAAAAAAAAA\n");
		new_gap_list.last_gap_offset = t_handle.table_metadata_offset;
	}
	
	/*first gap in new gap list is created from table metadata*/
	new_gap_list.first_gap_offset = t_handle.table_metadata_offset;
	

	if (file_header.first_gap_offset == -1) {
		
		write_into_db_file(f_handle, new_gap_list.first_gap_offset, t_handle.table_header.table_metadata_size, g_header);

		file_header.first_gap_offset = new_gap_list.first_gap_offset;
		file_header.last_gap_offset = new_gap_list.last_gap_offset;
		
	}
	else {
		/*maintain linked list*/
		/*lenghten ll*/
		struct Gap_Header last_gap_header;
		read_from_db_file(f_handle, file_header.last_gap_offset, sizeof(struct Gap_Header), &last_gap_header);
		last_gap_header.next_gap_header_offset = new_gap_list.first_gap_offset;
		write_into_db_file(f_handle, file_header.last_gap_offset, sizeof(struct Gap_Header), &last_gap_header);

		g_header->prev_gap_header_offset = file_header.last_gap_offset;
		write_into_db_file(f_handle, new_gap_list.first_gap_offset, t_handle.table_header.table_metadata_size, g_header);

		file_header.last_gap_offset = new_gap_list.last_gap_offset;

	}
	
	
	free(buffer);
	write_into_db_file(f_handle, 0, sizeof(struct File_Header), &file_header);

	return 0;
}

void* transform_data_row_into_db_format(void* tab_metadata_buffer, struct Data_Row_Node* data_row) {
	
	struct Table_Header* tab_header = (struct Table_Header*)tab_metadata_buffer;
	uint8_t column_is_present[tab_header->columns_number];

	void* buffer = malloc(DB_MAX_ROW_SIZE);
	struct Row_Header* row_header = (struct Row_Header*)buffer;
	row_header->next_row_header_offset = -1;
	row_header->prev_row_header_offset = -1;
	row_header->row_size = 0;

	uint32_t position = sizeof(struct Row_Header);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;

	struct Data_Row_Node* current_row = data_row;

	uint32_t found_cols_num = 0;

	while (current_row != NULL) {

		void* curr_column_metadata = (void*)((uint8_t*)tab_metadata_buffer + sizeof(struct Table_Header) + tab_header->table_name_metadata.length + 1);
		struct String col_name = current_row->column_name;
		uint8_t found = 0;
		for (uint32_t i = 0; i < tab_header->columns_number; i++) {

			struct Column_Header* col_header = (struct Column_Header*)curr_column_metadata;
			if (column_is_present[i] == 1) {
				// skip
				curr_column_metadata = (void*) ((uint8_t*)curr_column_metadata + sizeof(struct Column_Header) + (col_header->column_name_metadata).length + 1);
			}
			if (col_header->column_name_metadata.hash == col_name.hash) {
				char* current_schema_col_name = (char*)curr_column_metadata + sizeof(struct Column_Header);
				if (strcmp(current_schema_col_name, col_name.value) == 0) {
					// found
					column_is_present[i] = 1;
					if (col_header->data_type != current_row->new_value.data_type) {
						printf("INVALID DATA TYPE!");
						/*error*/
						free(buffer);
						return NULL;
					}
					found = 1;
					found_cols_num++;

					/*write data*/
					
					uint32_t bytes_num_to_write;
					if (col_header->data_type == STRING) {
						bytes_num_to_write = sizeof(struct String_Metadata) + current_row->new_value.value.db_string.length + 1;
						
					}
					if (col_header->data_type == INT) {
						bytes_num_to_write = sizeof(int32_t);

					}
					if (col_header->data_type == FLOAT) {
						bytes_num_to_write = sizeof(float);

					}
					if (col_header->data_type == BOOL) {
						bytes_num_to_write = sizeof(enum Boolean);

					}

					if (bytes_num_to_write + position > buff_sz) {
						/*realloc*/
						buff_sz = buff_sz + bytes_num_to_write + buff_sz / 2;
						buffer = realloc(buffer, buff_sz);
					}

					uint8_t* place_to_wr = (uint8_t*)buffer + position;
					if (col_header->data_type == STRING) {
						((struct String_Metadata*)place_to_wr)->hash = current_row->new_value.value.db_string.hash;
						((struct String_Metadata*)place_to_wr)->length = current_row->new_value.value.db_string.length;
						memcpy(place_to_wr + sizeof(struct String_Metadata), current_row->new_value.value.db_string.value, current_row->new_value.value.db_string.length + 1); // + "\0"
					}
					if (col_header->data_type == INT) {
						*((int32_t*)place_to_wr) = current_row->new_value.value.db_integer;

					}
					if (col_header->data_type == FLOAT) {
						*((float*)place_to_wr) = current_row->new_value.value.db_float;

					}
					if (col_header->data_type == BOOL) {
						*((enum Boolean*)place_to_wr) = current_row->new_value.value.db_boolean;

					}


					position += bytes_num_to_write;
					break;
				}
			}
		}
		if (found == 0) {
			printf("column with name %s DOESNT EXIST in table\n", current_row->column_name.value);
			/*error*/
			free(buffer);
			return NULL;
		}

		current_row = current_row->next_node;
	}
	if (found_cols_num != tab_header->columns_number) {
		printf("INVALID number of columns!");
		/*error*/
		free(buffer);
		return NULL;
	}

	row_header->row_size = position;
	return buffer;
}

/*GAP FILLING POLICY VS END INSERTION POLICY*/
/*END INSERTION POLICY*/
int32_t insert_row(struct File_Handle* f_handle, struct String table_name, struct Data_Row_Node * data_row) {
	
	printf("insert_row\n");
	struct Table_Handle tab_handle = find_table(f_handle, table_name);
	if (tab_handle.exists == 0) {
		printf("tab DOESNT exists, cannot insert!\n");
		return -1;
	}
	void* table_metadata_buffer = malloc(tab_handle.table_header.table_metadata_size);
	printf("tab metadata sz : %d\n", tab_handle.table_header.table_metadata_size);
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);
	
	void* row_data = transform_data_row_into_db_format(table_metadata_buffer, data_row);
	printf("here\n");
	struct Row_Header* row_header = (struct Row_Header*)row_data;
	int32_t file_end = find_file_end(f_handle);
	printf("file end::::: %x\n", file_end);

	uint32_t last_row_in_table_offset = tab_handle.table_header.last_row_offset;
	if (last_row_in_table_offset != -1) {
		printf("last tb row off %d\n", last_row_in_table_offset);
		printf("table wasnt empty\n");
		struct Row_Header last_row_in_table;
		read_from_db_file(f_handle, last_row_in_table_offset, sizeof(struct Row_Header), &last_row_in_table);
		last_row_in_table.next_row_header_offset = file_end;
		write_into_db_file(f_handle, last_row_in_table_offset, sizeof(struct Row_Header), &last_row_in_table);
		
	}
	else {
		/*table was empty*/
		printf("table empty\n");
		tab_handle.table_header.first_row_offset = file_end;
	}
	
	row_header->prev_row_header_offset = last_row_in_table_offset;
	write_into_db_file(f_handle, file_end, row_header->row_size, row_data);

	tab_handle.table_header.last_row_offset = file_end;
	write_into_db_file(f_handle, tab_handle.table_metadata_offset, sizeof(struct Table_Header), &(tab_handle.table_header));

	free(table_metadata_buffer);
	free(row_data);
}