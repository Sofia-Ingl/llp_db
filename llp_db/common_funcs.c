#include "common_structs.h"




uint32_t hash(char* string, uint32_t st_len) {
	return 0;
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