#ifndef COMMON_STRUCTS
#define COMMON_STRUCTS

#include <stdint.h>

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
	struct Schema_Internals_Value new_value;
	struct Data_Row_Node* next_node;
};

#endif