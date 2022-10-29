#ifndef QUERY_HANDLER
#define QUERY_HANDLER

#include "ram_structs.h"


struct File_Handle;

struct File_Handle* file_open_or_create(char* filename);

struct Table_Schema table_schema_init();

int8_t table_schema_expand(struct Table_Schema* schema, char* column_name, enum DB_Data_Type data_type);

int8_t table_create(struct File_Handle* f_handle, char* table_name, struct Table_Schema schema);

int8_t table_delete(struct File_Handle* f_handle, char* table_name);

#endif