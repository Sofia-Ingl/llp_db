#include "format_translators.h"
#include "file_handler_inner_structs.h"

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


uint32_t add_row_to_result_buffer(void* table_metadata_buffer,
	uint32_t number_of_selected_columns,
	struct String* column_names,
	void* result_buffer,
	//uint32_t* result_buffer_sz,
	/*uint32_t* result_buffer_position,*/
	uint32_t result_buffer_position,
	void* row) {
	//printf("add_row_to_result_buffer\n");
	void* new_result_row_start = (uint8_t*)result_buffer + result_buffer_position;

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

			void* pos_to_write_result_col_val = (uint8_t*)result_buffer + result_buffer_position;
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
				prev_result_col_val->next_node = (struct Data_Row_Node*)((uint8_t*)result_buffer + result_buffer_position);
			}

			prev_col_val_in_res_position = result_buffer_position; // prev = current
			result_buffer_position = result_buffer_position + sizeof(struct Data_Row_Node) + offset_from_row_node_end;

		}

		metadata_buffer_position = metadata_buffer_position + sizeof(struct Column_Header) + column_header->column_name_metadata.length + 1;
		row_position = row_position + curr_col_val_sz;
	}

	return result_buffer_position;
}



/*TRANSFORMATION LAYER*/
void* transform_table_metadata_into_db_format(struct String table_name, struct Table_Schema schema) {
	//printf("transform_table_metadata_into_db_format\n");
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
	uint32_t buffer_size = DB_MAX_TABLE_METADATA_SIZE;
	memcpy(buffer, &t_header, sizeof(struct Table_Header));
	memcpy((uint8_t*)buffer + sizeof(struct Table_Header), table_name.value, table_name.length + 1); // +'\0'
	uint32_t position = sizeof(struct Table_Header) + table_name.length + 1;
	uint32_t header_num = 0;

	for (uint32_t i = 0; i < schema.number_of_columns; i++)
	{
		struct Column_Info_Block current_column = schema.column_info[i];
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
			
			uint32_t avg_metadata_sz = (header_num == 0) ? buffer_size : (buffer_size / (header_num - 1)); // - current ; not consider tab metadat
			uint32_t columns_left = schema.number_of_columns - header_num - 1; // - current
			uint32_t curr_col_metadata_sz = sizeof(struct Column_Header) + current_column.column_name.length + 1;
			buffer_size = buffer_size + avg_metadata_sz * columns_left + curr_col_metadata_sz;
			buffer = realloc(buffer, buffer_size);
		}

		memcpy((uint8_t*)buffer + position, &c_header, sizeof(c_header));
		position += sizeof(c_header);
		memcpy((uint8_t*)buffer + position, current_column.column_name.value, current_column.column_name.length + 1);
		position += current_column.column_name.length + 1;

		header_num++;

	}

	((struct Table_Header*)buffer)->table_metadata_size = position;

	return buffer;


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

		if (current_row == NULL) {
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
		//printf("EXTRA COLUMNS\n");
		free(buffer);
		return NULL;
	}

	row_header->row_size = position;
	return buffer;
}


struct Table_Row_Lists_Bunch* transform_row_bunch_into_ram_format(struct Table_Handle * tab_handle_array, void** table_metadata_buffers, struct Table_Row_Bunch* trb,
	uint32_t current_tab_idx,
	uint32_t* number_of_columns_from_each_table,
	struct String** column_names) {

	if (trb == NULL) {
		return NULL;
	}

	uint32_t metadata_sz = tab_handle_array[current_tab_idx].table_header.table_metadata_size;
	uint32_t number_of_columns = (number_of_columns_from_each_table[current_tab_idx] == -1) ? tab_handle_array[current_tab_idx].table_header.columns_number : number_of_columns_from_each_table[current_tab_idx];
	uint32_t upper_bound_on_row_lists_buffer_sz = 
		trb->row_sz_sum
		+ metadata_sz
		//- sizeof(struct Table_Header)
		//- sizeof(struct Column_Header) * number_of_columns
		//- trb->local_fetched_rows_num * sizeof(struct Row_Header)
		+ sizeof(struct Data_Row_Node) * number_of_columns * trb->local_fetched_rows_num;
	
	void* row_lists_buffer = malloc(upper_bound_on_row_lists_buffer_sz);

	uint32_t* row_starts_in_buffer = malloc(sizeof(uint32_t) * trb->local_fetched_rows_num);
	struct Table_Row_Lists_Bunch** row_tails = NULL;

	if (trb->row_tails != NULL) {
		/*not the last tab in chain*/
		row_tails = malloc(sizeof(struct Table_Row_Lists_Bunch*) * trb->local_fetched_rows_num);
	}

	uint32_t row_lists_buffer_position = 0;

	for (uint32_t i = 0; i < trb->local_fetched_rows_num; i++)
	{
		struct Row_Header* rh = (struct Row_Header*)((uint8_t*)trb->fetched_rows_buffer + trb->row_starts_in_buffer[i]);
		row_starts_in_buffer[i] = row_lists_buffer_position;
		row_lists_buffer_position = add_row_to_result_buffer(table_metadata_buffers[current_tab_idx],
			number_of_columns_from_each_table[current_tab_idx],
			(column_names == NULL) ? NULL : column_names[current_tab_idx],
			row_lists_buffer,
			row_lists_buffer_position,
			rh);
		
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