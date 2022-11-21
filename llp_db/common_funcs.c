#include "common_structs.h"


uint32_t hash(char* str, uint32_t str_len) {
	uint32_t hash = 5381;
	uint32_t c;

	for (uint32_t i = 0; i < str_len; i++)
	{
		c = (uint32_t)*(str + i);
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
	}
		

	return hash;
}


struct String inner_string_create(char* str) {
	int32_t str_length = strlen(str);

	return (struct String) {
		.hash = hash(str, str_length),
			.length = str_length,
			.value = str
	};
}

struct Condition create_simple_condition(char* column_name, struct Schema_Internals_Value val, enum Condition_Relation relation) {

	struct Simple_Condition simp_cond = {
		.column_name = inner_string_create(column_name),
		.relation = relation,
		.right_part = val
	};
	union Condition_Union cond_union = {
		.simple_condition = simp_cond
	};
	return (struct Condition) {
		.is_simple = 1,
			.condition = cond_union
	};
}

struct Condition create_complex_condition(struct Condition* left, struct Condition* right, enum Condition_Chain_Relation relation) {

	struct Complex_Condition complex_cond = (struct Complex_Condition){
		.left = left,
		.relation = relation,
		.right = right
	};
	union Condition_Union cond_union = {
		.complex_condition = complex_cond
	};
	return (struct Condition) {
		.is_simple = 0,
			.condition = cond_union
	};

}



void free_table_row_bunch_struct(struct Table_Row_Bunch* trb) {
	free(trb->fetched_rows_buffer);
	free(trb->row_tails);
	free(trb->row_starts_in_buffer);
	free(trb);
}




struct Data_Row_Node create_data_row_node(char* column_name, enum DB_Data_Type data_type, void* value_pointer) {
	struct String hashed_column_name = inner_string_create(column_name);
	union Data data;
	if (data_type == INT) {
		data.db_integer = *((int32_t*)value_pointer);
	}
	if (data_type == FLOAT) {
		data.db_float = *((float*)value_pointer);
	}
	if (data_type == BOOL) {
		data.db_float = *((enum Boolean*)value_pointer);
	}
	if (data_type == STRING) { 
		char* string = *((char**)value_pointer);
		struct String hashed_string = inner_string_create(string);
		data.db_string = hashed_string;
	}

	struct Schema_Internals_Value schema_internal_value = (struct Schema_Internals_Value){
		.data_type = data_type,
		.value = data
	};
	return (struct Data_Row_Node) {
		.column_name = hashed_column_name,
			.value = schema_internal_value,
			.next_node = NULL
	};
}

