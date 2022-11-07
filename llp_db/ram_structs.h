#ifndef RAM_STRUCTS
#define RAM_STRUCTS

#include "common_structs.h"


struct File_Handle;
struct Table_Handle;

struct Column_Info_Block {
	struct String column_name; // для записи в файл строки должны быть отдельно
	enum DB_Data_Type column_type;
};

struct Table_Schema {
	uint32_t number_of_columns;
	uint32_t column_array_size; // to calc if there is free space or not
	struct Column_Info_Block* column_info;
};

struct Result_Set {
	uint8_t whole_table;
	uint32_t rows_num;
	struct Table_Handle* table_handle;
	struct Data_Row_Node** row_pointers;
	//uint32_t tail_rows_num; // how many rows at start of the buff fetched before cur_off = table_first_row
	uint32_t result_buffers_num; // to clear after use
	void** result_buffers_pool; // to clear after use
	uint32_t next_table_row_offset;
};

//struct Table {
//	struct String table_name;
//	struct Table_Schema schema;
//};

#endif