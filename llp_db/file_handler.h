#ifndef FILE_HANDLER
#define FILE_HANDLER

#include "ram_structs.h"

int32_t create_table(struct File_Handle* f_handle, struct String table_name, struct Table_Schema schema);

struct File_Handle* open_or_create_db_file(char* filename);

int32_t delete_table(struct File_Handle* f_handle, struct String table_name);

#endif