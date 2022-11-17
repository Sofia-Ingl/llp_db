#ifndef RAM_STRUCTS
#define RAM_STRUCTS

#include "common_structs.h"


struct File_Handle;
struct Table_Handle;

struct Column_Info_Block {
	struct String column_name; // ��� ������ � ���� ������ ������ ���� ��������
	enum DB_Data_Type column_type;
};

struct Table_Schema {
	uint32_t number_of_columns;
	uint32_t column_array_size; // to calc if there is free space or not
	struct Column_Info_Block* column_info;
};

//struct Result_Set {
//	uint8_t whole_table;
//	uint32_t rows_num;
//	struct Table_Handle* table_handle;
//	struct Data_Row_Node** row_pointers;
//	uint32_t result_buffers_num; // to clear after use
//	void** result_buffers_pool; // to clear after use
//	uint32_t next_table_row_offset;
//	struct Condition* condition;
//	int32_t number_of_selected_columns; // -1 => all cols
//	struct String* column_names;
//};

struct Table_Chain_Result_Set {

	uint32_t rows_num; // if rows_num < max_row_num -> EOF reached
	//uint8_t has_next; 
	int32_t number_of_joined_tables;
	struct String* table_names;
	struct Join_Condition* join_conditions;
	uint32_t* cursor_offsets;
	struct Condition** conditions_on_single_tables;
	struct Table_Handle* tab_handles;
	void** table_metadata_buffers;
	struct Table_Row_Lists_Bunch* rows_chain;
	int32_t* number_of_selected_columns; // -1 => all cols
	struct String** column_names;

};

/*rows in ram format*/
struct Table_Row_Lists_Bunch {
	uint32_t local_rows_num;
	void* row_lists_buffer;
	//struct Data_Row_Node** row_starts_in_buffer;
	uint32_t* row_starts_in_buffer;
	struct Table_Row_Lists_Bunch** row_tails;
};

#endif