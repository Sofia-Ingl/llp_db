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

/*POTENTIAL BUG, NEEDS TESTING*/
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

/*END INSERTION POLICY*/
int32_t create_table(struct File_Handle* f_handle, struct String table_name, struct Table_Schema schema) {
	printf("create_table %s\n", table_name.value);
	if (find_table(f_handle, table_name).exists == 1) {
		printf("tab ALREADY exists!\n");
		return -1;
	}
	void* table_metadata = transform_table_metadata_into_db_format( table_name, schema);
	printf("%p tab metadata arr\n", table_metadata);
	uint32_t table_metadata_sz = ((struct Table_Header*)table_metadata)->table_metadata_size;

	uint32_t free_space_ptr = find_file_end(f_handle);
	uint32_t write_res = write_into_db_file(f_handle, free_space_ptr, table_metadata_sz, table_metadata);
	printf("write res: %d\n", write_res);
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
		/*maintain gap linked list*/
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

	void* buffer = malloc(DB_MAX_ROW_SIZE);
	struct Row_Header* row_header = (struct Row_Header*)buffer;
	row_header->next_row_header_offset = -1;
	row_header->prev_row_header_offset = -1;
	row_header->row_size = 0;

	uint32_t position = sizeof(struct Row_Header);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;

	struct Data_Row_Node* current_row = data_row;

	uint32_t found_cols_num = 0;

	void* curr_column_metadata = (void*)((uint8_t*)tab_metadata_buffer + sizeof(struct Table_Header) + tab_header->table_name_metadata.length + 1);
	
	for (uint32_t i = 0; i < tab_header->columns_number; i++) {
		
		if (current_row  == NULL) {
			printf("INVALID COL SEQUENCE 1\n");
			free(buffer);
			return NULL;
		}
		struct String col_name = current_row->column_name;
		struct Column_Header* col_header = (struct Column_Header*)curr_column_metadata;
		/*CURRENT NODE VALIDATION*/
		
		if (col_header->column_name_metadata.hash != col_name.hash) {
			printf("INVALID COL SEQUENCE 2\n");
			free(buffer);
			return NULL;
		}
		char* current_schema_col_name = (char*)curr_column_metadata + sizeof(struct Column_Header);
		if (strcmp(current_schema_col_name, col_name.value) != 0) {
			printf("INVALID COL SEQUENCE 3\n");
			free(buffer);
			return NULL;
		}
		if (col_header->data_type != current_row->new_value.data_type) {
			printf("INVALID DATA TYPE!\n");
			free(buffer);
			return NULL;
		}
		/*BUFFER WRITING*/
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
		curr_column_metadata = (void*)((uint8_t*)curr_column_metadata + sizeof(struct Column_Header) + (col_header->column_name_metadata).length + 1);
		current_row = current_row->next_node;
	}
	if (current_row != NULL) {
		printf("EXTRA COLUMNS\n");
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
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);
	
	void* row_data = transform_data_row_into_db_format(table_metadata_buffer, data_row);
	struct Row_Header* row_header = (struct Row_Header*)row_data;
	int32_t file_end = find_file_end(f_handle);

	uint32_t last_row_in_table_offset = tab_handle.table_header.last_row_offset;
	if (last_row_in_table_offset != -1) {
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


uint8_t process_condition_relation(void* value_pos, struct Schema_Internals_Value target_value, enum Condition_Relation relation) {
	if (target_value.data_type == INT) {
		int32_t curr_row_val = *((int32_t*)value_pos);
		if (relation == EQUALS) {
			return (curr_row_val == target_value.value.db_integer) ? 1 : 0;
		}
		if (relation == NOT_EQUALS) {
			return (curr_row_val != target_value.value.db_integer) ? 1 : 0;
		}
		if (relation == LESS) {
			return (curr_row_val < target_value.value.db_integer) ? 1 : 0;
		}
		if (relation == BIGGER) {
			return (curr_row_val > target_value.value.db_integer) ? 1 : 0;
		}
	}

	if (target_value.data_type == FLOAT) {
		float curr_row_val = *((float*)value_pos);
		if (relation == EQUALS) {
			return (curr_row_val == target_value.value.db_float) ? 1 : 0;
		}
		if (relation == NOT_EQUALS) {
			return (curr_row_val != target_value.value.db_float) ? 1 : 0;
		}
		if (relation == LESS) {
			return (curr_row_val < target_value.value.db_float) ? 1 : 0;
		}
		if (relation == BIGGER) {
			return (curr_row_val > target_value.value.db_float) ? 1 : 0;
		}
	}

	if (target_value.data_type == BOOL) {
		enum Boolean curr_row_val = *((enum Boolean*)value_pos);
		if (relation == EQUALS) {
			return (curr_row_val == target_value.value.db_boolean) ? 1 : 0;
		}
		if (relation == NOT_EQUALS) {
			return (curr_row_val != target_value.value.db_boolean) ? 1 : 0;
		}
	}

	if (target_value.data_type == STRING) {
		struct String_Metadata* str_metadata = (struct String_Metadata*)value_pos;
		char* curr_row_str = (char*)value_pos + sizeof(struct String_Metadata);
		if (relation == EQUALS) {	
			return ((str_metadata->hash == target_value.value.db_string.hash) && (strcmp(curr_row_str, target_value.value.db_string.value) == 0)) ? 1 : 0;
		}
		if (relation == NOT_EQUALS) {
			return ((str_metadata->hash != target_value.value.db_string.hash) && (strcmp(curr_row_str, target_value.value.db_string.value) != 0)) ? 1 : 0;
		}
		if (relation == LESS) {
			return (strcmp(curr_row_str, target_value.value.db_string.value) < 0) ? 1 : 0;
		}
		if (relation == BIGGER) {
			return (strcmp(curr_row_str, target_value.value.db_string.value) > 0) ? 1 : 0;
		}
	}
	printf("ERROR: _");
	return -1;
}

uint8_t check_simple_condition(void* table_metadata_buffer, void* row_buffer, struct Simple_Condition condition) {

	struct Table_Header* t_header = (struct Table_Header*)table_metadata_buffer;
	struct Row_Header* r_header = (struct Row_Header*)(row_buffer);

	struct String target_column = condition.column_name;

	uint32_t current_metadata_pos = sizeof(struct Table_Header) + t_header->table_name_metadata.length + 1;
	uint32_t current_data_pos = sizeof(struct Row_Header);

	for (uint32_t i = 0; i < t_header->columns_number; i++)
	{
		struct Column_Header* c_header = (struct Column_Header*)((uint8_t*)table_metadata_buffer +current_metadata_pos);
		if (c_header->column_name_metadata.hash == target_column.hash) {
			char* curr_col_name = (char*)c_header + sizeof(struct Column_Header);
			if (strcmp(curr_col_name, target_column.value) == 0) {

				//check data type
				
				if (c_header->data_type != condition.right_part.data_type) {
					printf("INVAID DATA TYPE in condition\n");
					return -1;
				}

				// cmp value
				return process_condition_relation((uint8_t*)row_buffer + current_data_pos, condition.right_part, condition.relation);

			}
		}
		current_metadata_pos = current_metadata_pos + sizeof(struct Column_Header) + c_header->column_name_metadata.length + 1;
		uint32_t data_sz;
		if (c_header->data_type == INT) { data_sz = sizeof(int32_t); }
		if (c_header->data_type == FLOAT) { data_sz = sizeof(float); }
		if (c_header->data_type == BOOL) { data_sz = sizeof(enum Boolean); }
		if (c_header->data_type == STRING) { 
			struct String_Metadata* str_metadata = (struct String_Metadata*)((uint8_t*)row_buffer + current_data_pos);
			data_sz = sizeof(struct String_Metadata) + str_metadata->length + 1;
		}

		current_data_pos = current_data_pos + data_sz;
	}

	printf("INVAID TARGET COLUMN NAME in condition: %s\n", target_column.value);
	return -1;

}

uint8_t check_complex_condition(uint8_t left_res, uint8_t right_res, enum Condition_Chain_Relation operation) {

	if (operation == AND) {
		return left_res && right_res;
	}

	if (operation == OR) {
		return left_res || right_res;
	}

	return -1;

}

uint8_t apply_filter(void* table_metadata_buffer, void* row_buffer, struct Condition* condition) {

	if (condition == NULL) {
		return 1;
	}

	if (condition->is_simple == 1) {
		return check_simple_condition(table_metadata_buffer, row_buffer, condition->condition.simple_condition);
	}
	else {
		uint8_t left_res = apply_filter(table_metadata_buffer, row_buffer, condition->condition.complex_condition.left);
		uint8_t right_res = apply_filter(table_metadata_buffer, row_buffer, condition->condition.complex_condition.right);
		return check_complex_condition(left_res, right_res, condition->condition.complex_condition.relation);
	}

}


void flush_buffer(struct File_Handle* f_handle, uint32_t buffer_left_offset, uint32_t buffer_right_offset, void* buffer) {
	printf("flush buffer\n");
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
							uint32_t* row_sz) {
	struct Row_Header* r_header;
	uint32_t right_bound_row_header_offset = current_row_offset + sizeof(struct Row_Header);
	printf("current_row_offset: %x, buffer_left_offset: %x, buffer_right_offset: %x\n", current_row_offset, *buffer_left_offset, *buffer_right_offset);
	if ((*buffer_left_offset <= current_row_offset) && (right_bound_row_header_offset <= *buffer_right_offset)) {
		// ROW HEADER IN BUFFER
		*row_header_pos = current_row_offset - *buffer_left_offset;
		r_header = (struct Row_Header*)((uint8_t*)buffer + *row_header_pos);
		*row_sz = r_header->row_size;
		if ((current_row_offset + *row_sz) > *buffer_right_offset) {

			// not the whole row in array
			// should rewrite buffer data
			printf("not the whole row in array\n");
			flush_buffer(f_handle, *buffer_left_offset, *buffer_right_offset, buffer);

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
		printf("ROW HEADER NOT IN BUFFER\n");
		flush_buffer(f_handle, *buffer_left_offset, *buffer_right_offset, buffer);
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


int32_t delete_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition) {
	
	struct Table_Handle tab_handle = find_table(f_handle, table_name);
	if (tab_handle.exists == 0) {
		printf("tab DOESNT exists, cannot delete from!\n");
		return -1;
	}
	void* table_metadata_buffer = malloc(tab_handle.table_header.table_metadata_size);
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);

	void* buffer = malloc(DB_MAX_ROW_SIZE);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;
	uint32_t buffer_left_offset = 0;
	uint32_t buffer_right_offset = 0;

	uint32_t first_gap_offset = -1;
	uint32_t last_gap_offset = -1;

	uint32_t first_preserved_row_offset = -1;
	uint32_t last_preserved_row_offset = -1;

	int32_t number_of_rows_deleted = 0;

	uint32_t current_row_offset = tab_handle.table_header.first_row_offset;

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
			&row_sz);
		
		struct Row_Header* r_header = (struct Row_Header*)((uint8_t*)buffer + row_header_pos);
		/*row is in buffer on row_header_pos position*/

		/*APPLY FILTER*/
		uint8_t row_suitable = apply_filter(table_metadata_buffer, (uint8_t*)buffer + row_header_pos, condition);

		uint32_t next_row_offset = r_header->next_row_header_offset;

		if (!row_suitable) {
			/*DO NOT DELETE*/
			r_header->prev_row_header_offset = last_preserved_row_offset;
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
						&last_preserved_row_sz);
					struct Row_Header* last_preserved_r_header = (struct Row_Header*)((uint8_t*)buffer + last_preserved_row_header_pos);
					last_preserved_r_header->next_row_header_offset = current_row_offset;
				}
				

			}
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
						&last_gap_sz);
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

	/*TABLE ROW LINKED LIST CONSISTENCY*/
	tab_handle.table_header.first_row_offset = first_preserved_row_offset;
	tab_handle.table_header.last_row_offset = last_preserved_row_offset;
	write_into_db_file(f_handle, tab_handle.table_metadata_offset, sizeof(struct Table_Header), &tab_handle.table_header);
	if (last_preserved_row_offset != -1) {
		struct Row_Header r_header;
		read_from_db_file(f_handle, last_preserved_row_offset, sizeof(struct Row_Header), &r_header);
		if (r_header.next_row_header_offset != -1) {
			r_header.next_row_header_offset = -1;
			write_into_db_file(f_handle, last_preserved_row_offset, sizeof(struct Row_Header), &r_header);
		}
	}
	

	/*GAP LINKED LIST CONSISTENCY*/

	if (first_gap_offset != -1) {
		/*have to change gap list cause of new gaps*/
		printf("we have deleted rows\n");
		struct File_Header f_header;
		read_from_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);
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


		struct Gap_Header g_header;
		read_from_db_file(f_handle, last_gap_offset, sizeof(struct Gap_Header), &g_header); // last gap offset != -1
		if (g_header.next_gap_header_offset != -1) {
			g_header.next_gap_header_offset = -1;
			write_into_db_file(f_handle, last_gap_offset, sizeof(struct Gap_Header), &g_header);
		}
		
	}

	

	free(buffer);
	free(table_metadata_buffer);
	return number_of_rows_deleted;
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
				printf("currr co; %s --------- cmp with %s\n", curr_col_name, curr_val_upd->column_name.value);
				if (strcmp(curr_col_name, curr_val_upd->column_name.value) == 0) {
					
					if (col_header->data_type != curr_val_upd->new_value.data_type) {
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


uint32_t calc_inner_format_value_sz(enum DB_Data_Type data_type, void* value) {
	uint32_t bytes_num;
	if (data_type == STRING) {
		struct String_Metadata* str_metadata = (struct String_Metadata*)value;
		bytes_num = sizeof(struct String_Metadata) + str_metadata->length + 1;

	}
	if (data_type == INT) {
		bytes_num = sizeof(int32_t);

	}
	if (data_type == FLOAT) {
		bytes_num = sizeof(float);

	}
	if (data_type == BOOL) {
		bytes_num = sizeof(enum Boolean);

	}

	return bytes_num;
}

void copy_inner_format_value_into_buffer(void* buffer, uint32_t* buff_sz, uint32_t* dest_position, uint32_t* src_position, enum DB_Data_Type data_type, void* value) {
	uint32_t bytes_num_to_write = calc_inner_format_value_sz(data_type, value);
	/*if (data_type == STRING) {
		struct String_Metadata* str_metadata = (struct String_Metadata*)value;
		bytes_num_to_write = sizeof(struct String_Metadata) + str_metadata->length + 1;

	}
	if (data_type == INT) {
		bytes_num_to_write = sizeof(int32_t);

	}
	if (data_type == FLOAT) {
		bytes_num_to_write = sizeof(float);

	}
	if (data_type == BOOL) {
		bytes_num_to_write = sizeof(enum Boolean);

	}*/

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
				prepared_updates.updated_data[i]->new_value);
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
int32_t update_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition, struct Data_Row_Node* new_data) {
	printf("Update_rows \n");
	struct Table_Handle tab_handle = find_table(f_handle, table_name);
	if (tab_handle.exists == 0) {
		printf("tab DOESNT exists, cannot update rows!\n");
		return -1;
	}
	void* table_metadata_buffer = malloc(tab_handle.table_header.table_metadata_size);
	read_from_db_file(f_handle, tab_handle.table_metadata_offset, tab_handle.table_header.table_metadata_size, table_metadata_buffer);

	struct Update_Set prepared_updates = prepare_updates(table_metadata_buffer, new_data);
	if (prepared_updates.updated_columns_map == NULL) {
		printf("invalid update data\n");
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

	while (current_row_offset != -1) {

		/*FETCH CURRENT ROW*/
		printf("current row offset %d\n", current_row_offset);
		uint32_t row_sz;
		uint32_t row_header_pos;

		buffer = fetch_row_into_buffer(f_handle,
			buffer,
			current_row_offset,
			&buff_sz,
			&buffer_left_offset,
			&buffer_right_offset,
			&row_header_pos,
			&row_sz);

		

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
							&last_gap_sz);
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
				printf("last_gap_offset %x\n", last_gap_offset);
				struct Gap_Header* g_header = (struct Gap_Header*)((uint8_t*)buffer + row_header_pos);
				g_header->gap_size = row_sz;
				g_header->prev_gap_header_offset = last_gap_offset;
				g_header->next_gap_header_offset = -1;

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
						&last_gap_sz);
					struct Gap_Header* last_gap_header = (struct Gap_Header*)((uint8_t*)buffer + last_gap_header_pos);
					last_gap_header->next_gap_header_offset = new_gap_offset;
					printf("last_gap_header->next_gap_header_offset  %x\n", last_gap_header->next_gap_header_offset);
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
						&prev_row_sz);

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
						&next_row_sz);

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
		printf("we have created gaps\n");
		struct File_Header f_header;
		read_from_db_file(f_handle, 0, sizeof(struct File_Header), &f_header);
		if (f_header.first_gap_offset == -1) {
			f_header.first_gap_offset = first_gap_offset;
		}
		else {
			/*should fetch prev last gap header from db and set ins next = first gap created from curr table*/
			printf("here?!!!\n");
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

	
	return number_of_rows_updated;
	
}