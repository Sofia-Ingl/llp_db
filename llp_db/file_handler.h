#ifndef FILE_HANDLER
#define FILE_HANDLER

#include "ram_structs.h"

struct File_Handle* open_or_create_db_file(char* filename, float critical_gap_rate);
void close_or_normalize_db_file(struct File_Handle* f_handle, uint8_t normalize);

int32_t create_table(struct File_Handle* f_handle, struct String table_name, struct Table_Schema schema);
int32_t delete_table(struct File_Handle* f_handle, struct String table_name, uint8_t allow_normalization);
struct File_Table_Schema_Metadata get_table_schema_data(struct File_Handle* f_handle, struct String table_name);


int32_t insert_row(struct File_Handle* f_handle, struct String table_name, struct Data_Row_Node* data_row);
int32_t delete_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition, uint8_t allow_normalization);
int32_t update_rows(struct File_Handle* f_handle, struct String table_name, struct Condition* condition, struct Data_Row_Node* new_data, uint8_t allow_normalization);



struct Table_Chain_Result_Set* table_chain_select(struct File_Handle* f_handle,
	int32_t number_of_joined_tables,
	struct String* table_names,
	struct Join_Condition* join_conditions,
	struct Condition** conditions_on_single_tables,
	uint32_t* number_of_columns_from_each_table,
	struct String** column_names,
	uint32_t max_row_num);


struct Table_Chain_Result_Set* table_chain_get_next(struct File_Handle* f_handle,
	struct Table_Chain_Result_Set* rs,
	uint32_t max_row_num);


#endif