#ifndef RAM_STRUCTS
#define RAM_STRUCTS

#include "common_structs.h"


struct File_Handle;

struct Column_Info_Block {
	struct String column_name; // для записи в файл строки должны быть отдельно
	enum DB_Data_Type column_type;
};

struct Table_Schema {
	uint32_t number_of_columns;
	uint32_t column_array_size; // to calc if there is free space or not
	struct Column_Info_Block* column_info;
};

//struct Table {
//	struct String table_name;
//	struct Table_Schema schema;
//};

#endif