#ifndef FILE_HANDLER
#define FILE_HANDLER

#include "ram_structs.h"

struct Cursor {
	uint32_t next_row_offset;
};

struct Joined_Table_Cursor {
	uint32_t number_of_tables;
	struct Cursor* cursors;
};


int32_t create_table(struct File_Handle* f_handle, struct String table_name, struct Table_Schema schema);

struct File_Handle* open_or_create_db_file(char* filename);

int32_t delete_table(struct File_Handle* f_handle, struct String table_name);

int32_t insert_row(struct File_Handle* f_handle, struct String table_name, struct Data_Row_Node* data_row);

int32_t delete_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition);

int32_t update_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition, struct Data_Row_Node* new_data);


struct Result_Set single_table_select(struct File_Handle* f_handle,
	struct String table_name,
	struct Condition* condition,
	int32_t number_of_selected_columns, // -1 => all cols
	struct String* column_names,
	uint32_t max_row_num);

#endif