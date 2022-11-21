#ifndef REQUEST_STRUCTS
#define REQUEST_STRUCTS

#include <stdint.h>
#include "common_structs.h"

#define DEFAULT_ROW_NUM_IN_SELECT_QUERY 50

enum Request_Type {
	SELECT,
	UPDATE,
	DELETE,
	INSERT
};


/*JOIN SELECT*/

struct Joined_Table_Select {
	//int8_t all_columns;
	struct Joined_Table joined_table;
	uint32_t* number_of_columns_from_each_table;
	struct String** column_names; // array with len == joined_tables_num
	struct Condition** conditions; // array with len == joined_tables_num
};

/*REQUESTS*/

struct Single_Table_Select {
	struct String table_name;
	uint32_t number_of_columns; // -1 == all cols
	struct String* column_names;
	struct Condition* condition;
};

union Select_Union {
	struct Single_Table_Select single_table_select;
	struct Joined_Table_Select joined_table_select;
};

struct Select {
	uint8_t is_single_table_select;
	union Select_Union query_details;
};

struct Update {
	struct String table_name;
	struct Data_Row_Node* new_data;
	struct Condition* condition;
};

struct Delete {
	struct String table_name;
	struct Condition* condition;
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
	enum Request_Type request_type;
	union Request_Details request_details;
};


#endif