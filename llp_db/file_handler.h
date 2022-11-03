#ifndef FILE_HANDLER
#define FILE_HANDLER

#include "ram_structs.h"

int32_t create_table(struct File_Handle* f_handle, struct String table_name, struct Table_Schema schema);

struct File_Handle* open_or_create_db_file(char* filename);

int32_t delete_table(struct File_Handle* f_handle, struct String table_name);

int32_t insert_row(struct File_Handle* f_handle, struct String table_name, struct Data_Row_Node* data_row);

int32_t delete_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition);

int32_t update_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition, struct Data_Row_Node* new_data);

#endif