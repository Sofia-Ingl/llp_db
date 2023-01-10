#include "ram_structs.h"

void free_table_chain_result_set_inner_fields(struct Table_Chain_Result_Set* rs) {
	if (rs->rows_chain != NULL) {
		free_table_row_bunch_struct_list(rs->rows_chain);
	}
	
	
	for (uint32_t i = 0; i < rs->number_of_joined_tables; i++)
	{
		/*if (rs->number_of_selected_columns[i] != -1) {
			free(rs->column_names[i]);
		}*/
		//free(rs->conditions_on_single_tables[i]); // free(Null) - ?
		free(rs->table_metadata_buffers[i]);
		
	}
	free(rs->column_names);
	free(rs->number_of_selected_columns);
	free(rs->table_metadata_buffers);
	free(rs->conditions_on_single_tables);
	free(rs->cursor_offsets);
	free(rs->join_conditions);
	free(rs->table_names);
	free(rs->tab_handles);
	//free(rs);
}