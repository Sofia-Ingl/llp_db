#ifndef COMMON_STRUCTS
#define COMMON_STRUCTS

#include <stdint.h>
#include <string.h>
#include <stdlib.h>

struct String {
	uint32_t hash;
	uint16_t length;
	char* value;
};

enum DB_Data_Type {
	INT,
	FLOAT,
	BOOL,
	STRING
};

enum Boolean {
	FALSE,
	TRUE
};


union Data {
	struct String db_string;
	enum Boolean db_boolean;
	int32_t db_integer;
	float db_float;
};

struct Schema_Internals_Value {
	enum DB_Data_Type data_type;
	union Data value;
};

struct Data_Row_Node {
	struct String column_name;
	struct Schema_Internals_Value value;
	struct Data_Row_Node* next_node;
};


/*FILTERS*/

enum Condition_Relation {
	EQUALS,
	LESS,
	BIGGER,
	NOT_EQUALS
};

enum Condition_Chain_Relation {
	AND,
	OR
};

struct Simple_Condition {
	struct String column_name;
	enum Condition_Relation relation;
	struct Schema_Internals_Value right_part;
};

struct Condition;

struct Complex_Condition {
	struct Condition* left;
	struct Condition* right;
	enum Condition_Chain_Relation relation;
};

union Condition_Union {
	struct Complex_Condition complex_condition;
	struct Simple_Condition simple_condition;
};

struct Condition {
	uint8_t is_simple;
	union Condition_Union condition;
};


struct Join_Condition {
	uint32_t related_table_index;
	struct String related_table_column_name;
	struct String current_table_column_name;
};

struct Joined_Table {
	int32_t number_of_joined_tables;
	struct String* table_names;
	struct Join_Condition* join_conditions; // length = number_of_joined_tables - 1
};





/*rows in ram format*/
struct Table_Row_Lists_Bunch {
	uint32_t local_rows_num;
	void* row_lists_buffer;
	uint32_t* row_starts_in_buffer;
	struct Table_Row_Lists_Bunch** row_tails;
};

/*rows in inner format*/
struct Table_Row_Bunch {
	uint32_t local_fetched_rows_num;
	uint32_t total_fetched_rows_num;
	uint32_t row_sz_sum;
	void* fetched_rows_buffer;
	uint32_t* row_starts_in_buffer;
	struct Table_Row_Bunch** row_tails;
};


uint32_t hash(char* string, uint32_t st_len);

struct String inner_string_create(char* str);

struct Data_Row_Node create_data_row_node(char* column_name, enum DB_Data_Type data_type, void* value_pointer);

struct Condition create_simple_condition(char* column_name, struct Schema_Internals_Value val, enum Condition_Relation relation);

struct Condition create_complex_condition(struct Condition* left, struct Condition* right, enum Condition_Chain_Relation relation);

void free_table_row_bunch_struct(struct Table_Row_Bunch* trb);

#endif