#ifndef REQUEST_STRUCTS
#define REQUEST_STRUCTS

#include <stdint.h>
#include "common_structs.h"

enum Request_Type {
	SELECT,
	UPDATE,
	DELETE,
	INSERT
};


/*RECURSIVE CONDITION*/

//enum Condition_Relation {
//	EQUALS,
//	LESS,
//	BIGGER,
//	LESS_OR_EQUALS,
//	BIGGER_OR_EQUALS,
//	NOT_EQUALS,
//	IN,
//	NOT_IN
//};
//
//enum Condition_Chain_Relation {
//	AND,
//	OR
//};

struct Select;


//struct Extended_Column_Name {
//	struct String column_name;
//	struct String table_name;
//};
//
//union Column_Name_Union {
//	struct Extended_Column_Name extended_column_name;
//	struct String simple_column_name;
//};
//
//struct Column_Name {
//	uint8_t is_simple;
//	union Column_Name_Union column_name;
//};
//
//struct Simple_Condition {
//	struct Column_Name column_name;
//	enum Condition_Relation relation;
//	struct Schema_Internals_Value right_part;
//};
//
//struct Condition;
//
//struct Complex_Condition {
//	struct Condition* left;
//	struct Condition* right;
//	enum Condition_Chain_Relation relation;
//};
//
//union Condition_Union {
//	struct Complex_Condition complex_condition;
//	struct Simple_Condition simple_condition;
//};
//
//struct Condition {
//	uint8_t is_simple;
//	union Condition_Union condition;
//};

/*DATA ROW for insert and update*/




/*JOIN SELECT*/

struct Join_Condition {
	int32_t first_table_index;
	int32_t second_table_index;
	struct String first_table_column_name;
	struct String second_table_column_name;
};

struct Joined_Table {
	int32_t number_of_joined_tables;
	struct String* table_names;
	struct Join_Condition* join_conditions; // length = number_of_joined_tables - 1
};

struct Joined_Table_Select {
	int8_t all_columns;
	struct Joined_Table joined_table;
	int32_t number_of_columns;
	struct Extended_Column_Name* column_names;
	struct Condition condition;
};

/*REQUESTS*/

struct Single_Table_Select {
	int8_t all_columns;
	struct String table_name;
	int32_t number_of_columns;
	struct String* column_names;
	struct Condition condition;
};

union Select_Union {
	struct Single_Table_Select single_table_select;
	struct Joined_Table_Select joined_table_select;
};

struct Select {
	uint8_t is_single_table_select;
	//int8_t all_columns;
	//int32_t number_of_columns;
	//struct Condition condition;
	union Select_Union query_details;
};

struct Update {
	struct String table_name;
	struct Data_Row_Node* new_data;
	struct Condition condition;
};

struct Delete {
	struct String table_name;
	struct Condition condition;
};

struct Insert {
	struct String table_name;
	//uint32_t number_of_rows;
	//struct Data_Row_Node** new_data; // array of linked lists, every ll == row
	struct Data_Row_Node* new_data; // one row
};


union Request_Details {
	struct Select select_command;
	struct Update update_command;
	struct Delete delete_command;
	struct Insert insert_command;
};

struct Request {
	//struct DB_String table_name;
	enum Request_Type request_type;
	union Request_Details request_details;
};

/*select response*/

//struct Row_Set {
//	//struct Table_Schema
//	struct Data_Row_Node** rows;
//
//};


#endif