#include "file_structs.h"
#include "file_handler.h"

//#include "common_structs.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define MAX_RESULT_BUFFER_SIZE 4096

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

/*TRANSFORMATION LAYER*/
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


uint32_t add_table_to_f_header_and_update_list(struct File_Handle* f_handle, uint32_t table_metadata_offset) {
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

/*TRANSFORMATION LAYER*/
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
		if (col_header->data_type != current_row->value.data_type) {
			printf("INVALID DATA TYPE!\n");
			free(buffer);
			return NULL;
		}
		/*BUFFER WRITING*/
		uint32_t bytes_num_to_write;
		if (col_header->data_type == STRING) {
			bytes_num_to_write = sizeof(struct String_Metadata) + current_row->value.value.db_string.length + 1;

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
			((struct String_Metadata*)place_to_wr)->hash = current_row->value.value.db_string.hash;
			((struct String_Metadata*)place_to_wr)->length = current_row->value.value.db_string.length;
			memcpy(place_to_wr + sizeof(struct String_Metadata), current_row->value.value.db_string.value, current_row->value.value.db_string.length + 1); // + "\0"
		}
		if (col_header->data_type == INT) {
			*((int32_t*)place_to_wr) = current_row->value.value.db_integer;

		}
		if (col_header->data_type == FLOAT) {
			*((float*)place_to_wr) = current_row->value.value.db_float;

		}
		if (col_header->data_type == BOOL) {
			*((enum Boolean*)place_to_wr) = current_row->value.value.db_boolean;

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




/*FILTERING LAYER*/

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

/*------------END OF FILTERING LAYER --------------*/


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

	return number_of_rows_updated;
	
}


//struct Single_Table_Select {
//	int8_t all_columns;
//	struct String table_name;
//	int32_t number_of_columns;
//	struct String* column_names;
//	struct Condition condition;
//};


void* add_row_to_result_buffer(void* table_metadata_buffer,
								int32_t number_of_selected_columns,
								struct String* column_names,
								void* result_buffer,
								//uint32_t* result_buffer_sz,
								uint32_t* result_buffer_position,
								void* row) {
	printf("add_row_to_result_buffer\n");
	void* new_result_row_start = (uint8_t*)result_buffer + *result_buffer_position;

	struct Row_Header* row_header = (struct Row_Header*)row;
	uint8_t select_all_columns = (number_of_selected_columns == -1) ? 1 : 0;

	struct Table_Header* table_header = (struct Table_Header*)table_metadata_buffer;
	uint32_t total_columns_num = table_header->columns_number;
	uint32_t metadata_buffer_position = sizeof(struct Table_Header) + table_header->table_name_metadata.length + 1;

	uint32_t row_position = sizeof(struct Row_Header);

	uint32_t prev_col_val_in_res_position = -1;

	for (uint32_t i = 0; i < total_columns_num; i++)
	{
		struct Column_Header* column_header = (struct Column_Header*)((uint8_t*)table_metadata_buffer + metadata_buffer_position);
		uint8_t add_to_res = 0;
		if (number_of_selected_columns == -1) {
			// select all cols
			add_to_res = 1;
		}
		else {
			for (uint32_t j = 0; j < number_of_selected_columns; j++)
			{
				struct String current_selected_col = column_names[j];
				if (current_selected_col.hash == column_header->column_name_metadata.hash) {
					char* searched_col_name = (uint8_t*)column_header + sizeof(struct Column_Header);
					if (strcmp(searched_col_name, current_selected_col.value) == 0) {
						add_to_res = 1;
						break;
					}
				}
			}
		}
		
		void* curr_col_val = (uint8_t*)row + row_position;
		uint32_t curr_col_val_sz = calc_inner_format_value_sz(column_header->data_type, curr_col_val); // += row_position

		if (add_to_res == 1) {

			// should_add current col val to result set
			
			/*uint32_t curr_col_result_val_sz = sizeof(struct Data_Row_Node) + column_header->column_name_metadata.length + 1;

			if (column_header->data_type == STRING) {
				struct String_Metadata* str_metadata = (struct String_Metadata*)curr_col_val;
				curr_col_result_val_sz = curr_col_result_val_sz + str_metadata->length + 1;
			}*/

			//if ((curr_col_result_val_sz + *result_buffer_position) > *result_buffer_sz) {
			//	
			//	/*ERROR: CANNOT REALLOC WITHOUT POINTERS CHANGING*/
			//	printf("invalid buffer sz\n");
			//	/**result_buffer_sz = *result_buffer_sz + *result_buffer_sz / 2 + curr_col_result_val_sz;
			//	result_buffer = realloc(result_buffer, *result_buffer_sz);*/
			//}

			void* pos_to_write_result_col_val = (uint8_t*)result_buffer + *result_buffer_position;
			struct Data_Row_Node* result_col_val = (struct Data_Row_Node*)pos_to_write_result_col_val;

			result_col_val->next_node = NULL;
			
			result_col_val->column_name.hash = column_header->column_name_metadata.hash;
			result_col_val->column_name.length = column_header->column_name_metadata.length;
			result_col_val->column_name.value = (char*)pos_to_write_result_col_val + sizeof(struct Data_Row_Node);

			//copy column name
			uint32_t offset_from_row_node_end = column_header->column_name_metadata.length + 1;
			memcpy(result_col_val->column_name.value, (uint8_t*)column_header + sizeof(struct Column_Header), column_header->column_name_metadata.length + 1);

			result_col_val->value.data_type = column_header->data_type;

			if (column_header->data_type == INT) {
				result_col_val->value.value.db_integer = *((int32_t*)curr_col_val);
			}	
			if (column_header->data_type == FLOAT) {
				result_col_val->value.value.db_float = *((float*)curr_col_val);
			}
			if (column_header->data_type == BOOL) {
				result_col_val->value.value.db_boolean = *((enum Boolean*)curr_col_val);
			}
			if (column_header->data_type == STRING) {
				struct String_Metadata* str_metadata = (struct String_Metadata*)curr_col_val;

				//curr_col_result_val_sz = curr_col_result_val_sz + str_metadata->length + 1;
				//if ((curr_col_result_val_sz + *result_buffer_position) > *result_buffer_sz) {
				//	/*ERROR: CANNOT REALLOC WITHOUT POINTERS CHANGING*/
				//	printf("invalid buffer sz\n");
				//	//*result_buffer_sz = *result_buffer_sz + *result_buffer_sz / 2 + curr_col_result_val_sz;
				//	//result_buffer = realloc(result_buffer, *result_buffer_sz);
				//}


				result_col_val->value.value.db_string.hash = str_metadata->hash;
				result_col_val->value.value.db_string.length = str_metadata->length;
				result_col_val->value.value.db_string.value = (char*)pos_to_write_result_col_val + sizeof(struct Data_Row_Node) + offset_from_row_node_end;
				
				memcpy(result_col_val->value.value.db_string.value, (uint8_t*)curr_col_val + sizeof(struct String_Metadata), str_metadata->length + 1);
				offset_from_row_node_end = offset_from_row_node_end + str_metadata->length + 1;
			}

			if (prev_col_val_in_res_position != -1) {
				// set prev col val's next
				struct Data_Row_Node* prev_result_col_val = (struct Data_Row_Node*)((uint8_t*)result_buffer + prev_col_val_in_res_position);
				prev_result_col_val->next_node = (struct Data_Row_Node* )((uint8_t*)result_buffer + *result_buffer_position);
			}

			prev_col_val_in_res_position = *result_buffer_position; // prev = current
			*result_buffer_position = *result_buffer_position + sizeof(struct Data_Row_Node) + offset_from_row_node_end;

		}

		metadata_buffer_position = metadata_buffer_position + sizeof(struct Column_Header) + column_header->column_name_metadata.length + 1;
		row_position = row_position + curr_col_val_sz;
	}

	return new_result_row_start;
}


//struct Result_Set {
//	uint8_t is_empty;
//	uint8_t whole_table;
//	uint32_t rows_num;
//	//uint32_t tail_rows_num; // how many rows at start of the buff fetched before cur_off = table_first_row
//	uint32_t result_buffers_num; // to clear after use
//	void** result_buffers_pool; // to clear after use
//	uint32_t next_table_row_offset;
//};



//struct Result_Set single_table_bunch_select(struct File_Handle* f_handle,
//							struct Table_Handle table_handle, 
//							//struct Cursor cursor,
//							uint32_t starting_row_offset,
//							uint8_t cycle_fetch, // for join select
//							struct Condition* condition, 
//							int32_t number_of_selected_columns, // -1 => all cols
//							struct String* column_names,
//							uint32_t max_row_num) {
//
//	//uint32_t starting_row_offset = cursor.next_row_offset;
//	
//	uint32_t current_row_offset = starting_row_offset;
//	if ((current_row_offset == -1) && (cycle_fetch == 1)) {
//		current_row_offset = table_handle.table_header.first_row_offset;
//		if (current_row_offset == -1) {
//			//tab is empty
//			printf("tab is empty!\n");
//			return (struct Result_Set) {
//				.is_empty = 1
//			};
//		}
//	}
//
//
//	void* table_metadata_buffer = malloc(table_handle.table_header.table_metadata_size);
//	read_from_db_file(f_handle, table_handle.table_metadata_offset, table_handle.table_header.table_metadata_size, table_metadata_buffer);
//	
//	//uint8_t select_all_columns = (number_of_selected_columns == -1) ? 1 : 0;
//
//	uint32_t number_of_rows_selected = 0;
//	
//	void* current_result_buffer = malloc(MAX_RESULT_BUFFER_SIZE);
//	uint32_t current_result_buffer_sz = MAX_RESULT_BUFFER_SIZE;
//	uint32_t current_result_buffer_position = 0;
//
//	void** result_buffers_pool = malloc(10 * sizeof(void*));
//	uint32_t result_buffers_pool_sz = 10;
//	result_buffers_pool[0] = current_result_buffer;
//	uint32_t result_buffers_num = 1;
//
//	struct Data_Row_Node** row_pointers_buffer = malloc(sizeof(struct Data_Row_Node*) * max_row_num);
//	uint32_t row_pointers_buffer_position = 0;
//
//	void* buffer = malloc(DB_MAX_ROW_SIZE);
//	uint32_t buff_sz = DB_MAX_ROW_SIZE;
//	uint32_t buffer_left_offset = 0;
//	uint32_t buffer_right_offset = 0;
//
//	//uint32_t current_row_offset = table_handle.table_header.first_row_offset;
//	//uint32_t current_row_offset = cursor.next_row_offset;
//	
//	uint32_t loop_condition = (cycle_fetch == 1) 
//							? (number_of_rows_selected < max_row_num) 
//							: ((current_row_offset != -1) && (number_of_rows_selected < max_row_num));
//	uint32_t tail_rows_num = 0;
//	uint8_t tail_rows = 1;
//
//	/*while (current_row_offset != -1) {*/
//	uint32_t next_row_offset;
//	while (loop_condition) {
//		
//		uint32_t row_sz;
//		uint32_t row_header_pos;
//
//		buffer = fetch_row_into_buffer(f_handle,
//			buffer,
//			current_row_offset,
//			&buff_sz,
//			&buffer_left_offset,
//			&buffer_right_offset,
//			&row_header_pos,
//			&row_sz);
//
//		struct Row_Header* r_header = (struct Row_Header*)((uint8_t*)buffer + row_header_pos);
//		next_row_offset = r_header->next_row_header_offset;
//		/*row is in buffer on row_header_pos position*/
//
//		/*APPLY FILTER*/
//		uint8_t row_suitable = apply_filter(table_metadata_buffer, (uint8_t*)buffer + row_header_pos, condition);
//		
//		if (row_suitable == 1) {
//			// should add to result_set
//
//			uint32_t upper_bound_on_row_sz = sizeof(struct Data_Row_Node)*number_of_selected_columns 
//											+ r_header->row_size 
//											+ table_handle.table_header.table_metadata_size
//											- sizeof(struct Table_Header)
//											- sizeof(struct Column_Header)* table_handle.table_header.columns_number;
//			if ((upper_bound_on_row_sz + current_result_buffer_position) > current_result_buffer_sz) {
//				// alloc new buffer in buffer pool
//
//				current_result_buffer_sz = (MAX_RESULT_BUFFER_SIZE > upper_bound_on_row_sz) ? MAX_RESULT_BUFFER_SIZE : upper_bound_on_row_sz;
//				current_result_buffer = malloc(current_result_buffer_sz);
//				current_result_buffer_position = 0;
//
//				if (result_buffers_num == result_buffers_pool_sz) {
//					result_buffers_pool_sz = result_buffers_pool_sz + result_buffers_pool_sz / 2;
//					result_buffers_pool = realloc(result_buffers_pool, result_buffers_pool_sz);
//				}
//				result_buffers_pool[result_buffers_num] = current_result_buffer;
//				result_buffers_num++;
//
//			}
//			void* new_result_row_pointer = add_row_to_result_buffer(table_metadata_buffer,
//																		number_of_selected_columns,
//																		column_names,
//																		current_result_buffer,
//																		&current_result_buffer_position,
//																		r_header);
//			row_pointers_buffer[row_pointers_buffer_position] = (struct Data_Row_Node*)new_result_row_pointer;
//			row_pointers_buffer_position++;
//			number_of_rows_selected++;
//
//			if (tail_rows == 1) {
//				tail_rows_num++;
//			}
//
//			if (number_of_rows_selected == max_row_num) {
//				/*bunch already in result buffer*/
//				
//				free(buffer);
//				free(table_metadata_buffer);
//				return (struct Result_Set) {
//					.is_empty = 0,
//					.whole_table = (next_row_offset == starting_row_offset)? 1:0,
//					.rows_num = max_row_num,
//					.tail_rows_num = tail_rows_num,
//					.result_buffers_num = result_buffers_num,
//					.result_buffers_pool = result_buffers_pool,
//					.next_table_row_offset = next_row_offset
//				};
//			}
//			
//		}
//		current_row_offset = next_row_offset;
//
//		loop_condition = (cycle_fetch == 1)
//						? (number_of_rows_selected < max_row_num)
//						: ((current_row_offset != -1) && (number_of_rows_selected < max_row_num));
//
//		if ((current_row_offset == -1) && (cycle_fetch == 1)) {
//			current_row_offset = table_handle.table_header.first_row_offset;
//			tail_rows = 0;
//		}
//
//		if (current_row_offset == starting_row_offset) {
//			// all tab rows were fetched even if cycle_fetch == 1
//			break;
//		}
//	}
//
//	free(buffer);
//	free(table_metadata_buffer);
//	// < max_row_num rows were fetched, but the whole table
//	return (struct Result_Set) {
//		.is_empty = 0,
//		.whole_table = 1,
//		.rows_num = number_of_rows_selected,
//		.tail_rows_num = tail_rows_num,
//		.result_buffers_num = result_buffers_num,
//		.result_buffers_pool = result_buffers_pool,
//		.next_table_row_offset = current_row_offset
//	};
//}




struct Result_Set* single_table_bunch_select(struct File_Handle* f_handle,
											struct Table_Handle* table_handle,
											//struct Cursor cursor,
											uint32_t starting_row_offset,
											struct Condition* condition, // with join column restriction
											int32_t number_of_selected_columns, // -1 => all cols
											struct String* column_names,
											uint32_t max_row_num) {

	//uint32_t starting_row_offset = cursor.next_row_offset;
	printf("single_table_bunch_select\n");
	uint32_t current_row_offset = starting_row_offset;

	struct Result_Set* rs = malloc(sizeof(struct Result_Set));

	if (current_row_offset == -1) {
			//tab is empty
		printf("tab tail is empty!\n");
		rs->rows_num = 0;
		return rs;
	}

	void* table_metadata_buffer = malloc(table_handle->table_header.table_metadata_size);
	read_from_db_file(f_handle, table_handle->table_metadata_offset, table_handle->table_header.table_metadata_size, table_metadata_buffer);

	//uint8_t select_all_columns = (number_of_selected_columns == -1) ? 1 : 0;

	uint32_t number_of_rows_selected = 0;

	void* current_result_buffer = malloc(MAX_RESULT_BUFFER_SIZE);
	uint32_t current_result_buffer_sz = MAX_RESULT_BUFFER_SIZE;
	uint32_t current_result_buffer_position = 0;

	void** result_buffers_pool = malloc(10 * sizeof(void*));
	uint32_t result_buffers_pool_sz = 10;
	result_buffers_pool[0] = current_result_buffer;
	uint32_t result_buffers_num = 1;

	struct Data_Row_Node** row_pointers_buffer = malloc(sizeof(struct Data_Row_Node*) * max_row_num);
	uint32_t row_pointers_buffer_position = 0;

	void* buffer = malloc(DB_MAX_ROW_SIZE);
	uint32_t buff_sz = DB_MAX_ROW_SIZE;
	uint32_t buffer_left_offset = 0;
	uint32_t buffer_right_offset = 0;

	//uint32_t current_row_offset = table_handle.table_header.first_row_offset;
	//uint32_t current_row_offset = cursor.next_row_offset;

	/*while (current_row_offset != -1) {*/
	uint32_t next_row_offset;
	while ((current_row_offset != -1) && (number_of_rows_selected < max_row_num)) {

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
		next_row_offset = r_header->next_row_header_offset;
		/*row is in buffer on row_header_pos position*/

		/*APPLY FILTER*/
		uint8_t row_suitable = apply_filter(table_metadata_buffer, (uint8_t*)buffer + row_header_pos, condition);

		if (row_suitable == 1) {
			// should add to result_set

			uint32_t upper_bound_on_row_sz = sizeof(struct Data_Row_Node) * number_of_selected_columns
				+ r_header->row_size
				+ table_handle->table_header.table_metadata_size
				- sizeof(struct Table_Header)
				- sizeof(struct Column_Header) * table_handle->table_header.columns_number;
			if ((upper_bound_on_row_sz + current_result_buffer_position) > current_result_buffer_sz) {
				// alloc new buffer in buffer pool
				current_result_buffer_sz = (MAX_RESULT_BUFFER_SIZE > upper_bound_on_row_sz) ? MAX_RESULT_BUFFER_SIZE : upper_bound_on_row_sz;
				current_result_buffer = malloc(current_result_buffer_sz);
				current_result_buffer_position = 0;

				if (result_buffers_num == result_buffers_pool_sz) {
					result_buffers_pool_sz = result_buffers_pool_sz + result_buffers_pool_sz / 2;
					result_buffers_pool = realloc(result_buffers_pool, result_buffers_pool_sz);
				}
				result_buffers_pool[result_buffers_num] = current_result_buffer;
				result_buffers_num++;

			}
			void* new_result_row_pointer = add_row_to_result_buffer(table_metadata_buffer,
				number_of_selected_columns,
				column_names,
				current_result_buffer,
				&current_result_buffer_position,
				r_header);
			row_pointers_buffer[row_pointers_buffer_position] = (struct Data_Row_Node*)new_result_row_pointer;
			row_pointers_buffer_position++;
			number_of_rows_selected++;
		}
		current_row_offset = next_row_offset;

	}

	free(buffer);
	free(table_metadata_buffer);
	// < max_row_num rows were fetched, but the whole table
	rs->whole_table = (current_row_offset == -1) ? 1 : 0;
	rs->rows_num = number_of_rows_selected;
	rs->table_handle = table_handle;
	rs->result_buffers_num = result_buffers_num;
	rs->result_buffers_pool = result_buffers_pool;
	rs->row_pointers = row_pointers_buffer;
	rs->next_table_row_offset = current_row_offset;
	rs->column_names = column_names;
	rs->number_of_selected_columns = number_of_selected_columns;
	rs->condition = condition;
	return rs;
}





struct Result_Set* single_table_select(struct File_Handle* f_handle,
									struct String table_name,
									struct Condition* condition,
									int32_t number_of_selected_columns, // -1 => all cols
									struct String* column_names,
									uint32_t max_row_num) {
	printf("single_table_select\n");
	struct Table_Handle* table_handle = malloc(sizeof(struct Table_Handle));
	*table_handle = find_table(f_handle, table_name);
	if (table_handle->exists == 0) {
		printf("wrong tab name\n");
		return NULL;
	}
	return single_table_bunch_select(f_handle,
		table_handle,
		//struct Cursor cursor,
		table_handle->table_header.first_row_offset,
		condition, // with join column restriction
		number_of_selected_columns, // -1 => all cols
		column_names,
		max_row_num);
}


/*NEEDS TESTING*/
struct Result_Set* single_table_get_next(struct File_Handle* f_handle,
	struct Result_Set* rs,
	uint32_t max_row_num) {
	
	/*clean prev data*/
	for (uint32_t i = 0; i < rs->result_buffers_num; i++)
	{
		free((rs->result_buffers_pool)[i]);
	}
	free(rs->result_buffers_pool);
	free(rs->row_pointers);

	struct Result_Set* new_rs = single_table_bunch_select(f_handle,
		rs->table_handle,
		rs->next_table_row_offset,
		rs->condition, // with join column restriction
		rs->number_of_selected_columns, // -1 => all cols
		rs->column_names,
		max_row_num);

	free(rs);
	return new_rs;

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
					place_to_write_col_val->value.db_string.value = (uint8_t*)row + column_val_offset_in_row;
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


/*rows in inner format*/
struct Table_Row_Bunch {
	uint32_t local_fetched_rows_num;
	uint32_t total_fetched_rows_num;
	uint32_t row_sz_sum;
	void* fetched_rows_buffer;
	//struct Row_Header** row_starts_in_buffer;
	uint32_t* row_starts_in_buffer;
	struct Table_Row_Bunch** row_tails;
};



struct Table_Row_Bunch* table_chain_single_recursive_select(struct File_Handle* f_handle,
										struct Joined_Table joined_table,
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
		printf("tab tail is empty!\n");
		//struct Table_Row_Bunch* res = malloc(sizeof(struct Table_Row_Bunch));
		//res->fetched_rows_num = 0;
		//return res;
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
	if (cur_tab_index != (joined_table.number_of_joined_tables - 1)) { // not the last table
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
			&row_sz);
		struct Row_Header* r_header = (struct Row_Header*)((uint8_t*)buffer + row_header_pos);
		next_row_offset = r_header->next_row_header_offset;
		
		
		cursor_offsets[cur_tab_index] = next_row_offset; // points next row
		/*row is in buffer on row_header_pos position*/

		/*APPLY FILTER*/
		uint8_t row_suitable = apply_filter(table_metadata_buffers[cur_tab_index], (uint8_t*)buffer + row_header_pos, &final_condition_on_current_table);

		if (row_suitable == 1) {

			if (cur_tab_index != (joined_table.number_of_joined_tables - 1)) { // not the last tab in chain
				/*try to find row tail in other tables recursively*/
				//max rows selected = calc
				
				//set it for next tabs to create join condition
				current_row_chain[cur_tab_index] = (struct Row_Header* )((uint8_t*)buffer + row_header_pos);
				
				uint32_t next_tab_index = cur_tab_index + 1;
				

				struct Schema_Internals_Value value_to_use_in_join_cond;
				struct Condition join_condition_on_next_table;
				
				

				/*NEXT TABLE JOIN COL CONDITION*/
				// a join b on a.x = b.y ; replace a.x with its value and use as b.y condition
				uint32_t related_tab_ind = joined_table.join_conditions[cur_tab_index].related_table_index; // cur tab idx because there is no join cond on first table
				struct String related_tab_join_col_name = joined_table.join_conditions[cur_tab_index].related_table_column_name;
					
				get_value_of_column(tab_handles + related_tab_ind,
						table_metadata_buffers[related_tab_ind],
						current_row_chain[related_tab_ind],
						related_tab_join_col_name,
						&value_to_use_in_join_cond);

				join_condition_on_next_table = create_simple_condition(joined_table.join_conditions[cur_tab_index].current_table_column_name.value, value_to_use_in_join_cond, EQUALS);

			
				//reset cursor for the next table (scanning from start)
				cursor_offsets[next_tab_index] = tab_handles[next_tab_index].table_header.first_row_offset;
				struct Table_Row_Bunch* next_tab_res = table_chain_single_recursive_select(f_handle,
					joined_table,
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

				/*BUG*/
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




void free_table_row_bunch_struct(struct Table_Row_Bunch* trb) {
	free(trb->fetched_rows_buffer);
	free(trb->row_tails);
	free(trb->row_starts_in_buffer);
	free(trb);
}

struct Table_Row_Lists_Bunch* transform_row_bunch_into_ram_format(struct Table_Handle* tab_handle_array, void** table_metadata_buffers, struct Table_Row_Bunch* trb,
										uint32_t current_tab_idx,
										uint32_t* number_of_columns_from_each_table,
										struct String** column_names) {

	if (trb == NULL) {
		return NULL;
	}
	uint32_t number_of_columns = (number_of_columns_from_each_table[current_tab_idx] == -1)? tab_handle_array[current_tab_idx].table_header.columns_number : number_of_columns_from_each_table[current_tab_idx];
	uint32_t upper_bound_on_row_lists_buffer_sz = trb->row_sz_sum
		- trb->local_fetched_rows_num * sizeof(struct Row_Header)
		+ sizeof(struct Data_Row_Node) * number_of_columns * trb->local_fetched_rows_num;

	void* row_lists_buffer = malloc(upper_bound_on_row_lists_buffer_sz);
	
	//struct Data_Row_Node** row_starts_in_buffer = malloc(sizeof(struct Data_Row_Node*) * trb->local_fetched_rows_num);
	uint32_t* row_starts_in_buffer = malloc(sizeof(uint32_t) * trb->local_fetched_rows_num);
	struct Table_Row_Lists_Bunch** row_tails = NULL;
	
	if (trb->row_tails != NULL) {
		/*not the last tab in chain*/
		row_tails = malloc(sizeof(struct Table_Row_Lists_Bunch*) * trb->local_fetched_rows_num);
	}

	uint32_t row_lists_buffer_position = 0;


	for (uint32_t i = 0; i < trb->local_fetched_rows_num; i++)
	{
		struct Row_Header* rh = (struct Row_Header* )((uint8_t*)trb->fetched_rows_buffer + trb->row_starts_in_buffer[i]);
		printf("%p\n", table_metadata_buffers[current_tab_idx]);
		row_starts_in_buffer[i] = row_lists_buffer_position;
		
		void* new_result_row_pointer = add_row_to_result_buffer(table_metadata_buffers[current_tab_idx],
			number_of_columns_from_each_table[current_tab_idx],
			(column_names == NULL)? NULL:column_names[current_tab_idx],
			row_lists_buffer,
			&row_lists_buffer_position,
			rh);

		//row_starts_in_buffer[i] = new_result_row_pointer;

		if (trb->row_tails != NULL) {
			struct Table_Row_Lists_Bunch* current_row_tails = transform_row_bunch_into_ram_format(tab_handle_array, table_metadata_buffers, trb->row_tails[i],
				current_tab_idx + 1,
				number_of_columns_from_each_table,
				column_names);
			row_tails[i] = current_row_tails;
		}
		
	}


	struct Table_Row_Lists_Bunch* current_tab_row_lists = malloc(sizeof(struct Table_Row_Lists_Bunch));
	current_tab_row_lists->row_lists_buffer = row_lists_buffer;
	current_tab_row_lists->row_starts_in_buffer = row_starts_in_buffer;
	current_tab_row_lists->row_tails = row_tails;
	current_tab_row_lists->local_rows_num = trb->local_fetched_rows_num;

	free_table_row_bunch_struct(trb);
	return current_tab_row_lists;

}




struct Join_Result_Set* table_chain_select(struct File_Handle* f_handle,
						struct Joined_Table joined_table, 
						struct Condition** conditions_on_single_tables,
						uint32_t* number_of_columns_from_each_table,
						struct String** column_names,
						uint32_t max_row_num) {

	printf("table_chain_select\n");

	struct Table_Handle* tab_handle_array = malloc(sizeof(struct Table_Handle) * joined_table.number_of_joined_tables);

	uint32_t* cursor_offsets = malloc(sizeof(uint32_t) * joined_table.number_of_joined_tables);
	memset(cursor_offsets, 0, sizeof(uint32_t) * joined_table.number_of_joined_tables);

	void** table_metadata_buffers = malloc(sizeof(void*) * joined_table.number_of_joined_tables);


	//find all tables
	for (uint32_t i = 0; i < joined_table.number_of_joined_tables; i++)
	{
		
		tab_handle_array[i] = find_table(f_handle, joined_table.table_names[i]);
		if (tab_handle_array[i].exists == 0) {
			
			printf("table %s doesnt exist\n", joined_table.table_names[i].value);
			printf("join select failed\n");
			free(tab_handle_array);
			free(cursor_offsets);
			for (uint32_t j = 0; j < joined_table.number_of_joined_tables; j++)
			{
				free(table_metadata_buffers[j]);
			}
			free(table_metadata_buffers);
			return NULL;
		}
		cursor_offsets[i] = tab_handle_array[i].table_header.first_row_offset;
		if (cursor_offsets[i] == -1) {
			printf("one of the tables is empty -> empty result\n");
			free(tab_handle_array);
			free(cursor_offsets);
			for (uint32_t j = 0; j < joined_table.number_of_joined_tables; j++)
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

	
	struct Row_Header** row_chain_buffer = malloc(sizeof(struct Row_Header*) * joined_table.number_of_joined_tables); // to check join conditions
	
	struct Table_Row_Bunch* raw_rows_chain = table_chain_single_recursive_select(f_handle,
		joined_table,
		tab_handle_array, table_metadata_buffers,
		conditions_on_single_tables,
		NULL,
		row_chain_buffer,
		cursor_offsets,
		0,
		max_row_num);

	uint32_t fetched_rows_num = raw_rows_chain->total_fetched_rows_num;

	struct Table_Row_Lists_Bunch* rows_chain = transform_row_bunch_into_ram_format(tab_handle_array, table_metadata_buffers, raw_rows_chain,
		0,
		number_of_columns_from_each_table,
		column_names);

	free(row_chain_buffer);

	struct Join_Result_Set* rs = malloc(sizeof(struct Join_Result_Set));
	rs->rows_num = fetched_rows_num;
	rs->joined_table = joined_table;
	rs->cursor_offsets = cursor_offsets;
	rs->conditions_on_single_tables = conditions_on_single_tables;
	rs->tab_handles = tab_handle_array;
	rs->table_metadata_buffers = table_metadata_buffers;
	rs->rows_chain = rows_chain;
	rs->number_of_selected_columns = number_of_columns_from_each_table; // -1 => all cols
	rs->column_names = column_names;

	return rs;

	
}

/*used in join result sets*/
void free_table_row_bunch_struct_list(struct Table_Row_Lists_Bunch* trb) {
	
	if (trb != NULL) {

		if (trb->row_tails != NULL) {
			for (uint32_t i = 0; i < trb->local_rows_num; i++)
			{
				free_table_row_bunch_struct_list(trb->row_tails[i]);
			}
		}

		free(trb->row_lists_buffer);
		free(trb->row_starts_in_buffer);
		free(trb->row_tails);
		free(trb);
	}
}


struct Join_Result_Set* table_chain_get_next(struct File_Handle* f_handle,
	struct Join_Result_Set* rs,
	uint32_t max_row_num) {

	struct Row_Header** row_chain_buffer = malloc(sizeof(struct Row_Header*) * rs->joined_table.number_of_joined_tables);

	free_table_row_bunch_struct_list(rs->rows_chain);

	struct Table_Row_Bunch* new_row_bunch = table_chain_single_recursive_select(f_handle,
		rs->joined_table,
		rs->tab_handles, rs->table_metadata_buffers,
		rs->conditions_on_single_tables,
		NULL,
		row_chain_buffer,
		rs->cursor_offsets,
		0,
		max_row_num);

	if (new_row_bunch == NULL) {
		/*clear everything created inside inner select function*/
		free(row_chain_buffer);
		free(rs->cursor_offsets);
		free(rs->table_metadata_buffers);
		free(rs->tab_handles);
		free(rs);
		/*do not clear conditions, col names etc that were given as a parameter*/
		return NULL;
	}

	uint32_t fetched_rows_num = new_row_bunch->total_fetched_rows_num;

	struct Table_Row_Lists_Bunch* rows_chain = transform_row_bunch_into_ram_format(rs->tab_handles, rs->table_metadata_buffers, new_row_bunch,
		0,
		rs->number_of_selected_columns,
		rs->column_names);

	free(row_chain_buffer);

	// cursor offsets changed
	rs->rows_num = fetched_rows_num;
	rs->rows_chain = rows_chain;

	return rs;
}