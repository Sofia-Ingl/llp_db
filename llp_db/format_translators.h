#ifndef FORMAT_TRANSLATORS
#define FORMAT_TRANSLATORS

#include "file_structs.h"
#include "ram_structs.h"

//#include "common_structs.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

uint32_t calc_inner_format_value_sz(enum DB_Data_Type data_type, void* value);

void* transform_table_metadata_into_db_format(struct String table_name, struct Table_Schema schema);


void* transform_data_row_into_db_format(void* tab_metadata_buffer, struct Data_Row_Node* data_row);

struct Table_Row_Lists_Bunch* transform_row_bunch_into_ram_format(struct Table_Handle* tab_handle_array, void** table_metadata_buffers, struct Table_Row_Bunch* trb,
	uint32_t current_tab_idx,
	uint32_t* number_of_columns_from_each_table,
	struct String** column_names);

#endif