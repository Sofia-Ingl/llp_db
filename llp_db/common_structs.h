#ifndef COMMON_STRUCTS
#define COMMON_STRUCTS

#include <stdint.h>
#include <string.h>

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

struct Extended_Column_Name {
	struct String column_name;
	struct String table_name;
};

union Column_Name_Union {
	struct Extended_Column_Name extended_column_name;
	struct String simple_column_name;
};

struct Column_Name {
	uint8_t is_simple;
	union Column_Name_Union column_name;
};

struct Simple_Condition {
	//struct Column_Name column_name;
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

/*response*/
//struct Row_Set {
//	//struct Table_Schema
//	struct Data_Row_Node** rows;
//
//};



struct Join_Condition {
	//int32_t first_table_index;
	//int32_t second_table_index;
	uint32_t related_table_index;
	struct String related_table_column_name;
	struct String current_table_column_name;
};

struct Joined_Table {
	int32_t number_of_joined_tables;
	struct String* table_names;
	struct Join_Condition* join_conditions; // length = number_of_joined_tables - 1
};









uint32_t hash(char* string, uint32_t st_len);

struct String inner_string_create(char* str);

struct Condition create_simple_condition(char* column_name, struct Schema_Internals_Value val, enum Condition_Relation relation);

struct Condition create_complex_condition(struct Condition* left, struct Condition* right, enum Condition_Chain_Relation relation);


#endif