#include "filters.h"
#include "file_structs.h"

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
	//printf("ERROR: _");
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
		struct Column_Header* c_header = (struct Column_Header*)((uint8_t*)table_metadata_buffer + current_metadata_pos);
		if (c_header->column_name_metadata.hash == target_column.hash) {
			char* curr_col_name = (char*)c_header + sizeof(struct Column_Header);
			if (strcmp(curr_col_name, target_column.value) == 0) {

				//check data type

				if (c_header->data_type != condition.right_part.data_type) {
					//printf("INVAID DATA TYPE in condition\n");
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

	//printf("INVAID TARGET COLUMN NAME in condition: %s\n", target_column.value);
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