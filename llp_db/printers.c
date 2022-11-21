#include "printers.h"

void print_table_row(struct Data_Row_Node* dr) {
	while (dr != NULL) {
		if (dr->value.data_type == STRING) {
			printf("%s	", dr->value.value.db_string.value);
		}
		if (dr->value.data_type == INT) {
			printf("%d	", dr->value.value.db_integer);
		}
		if (dr->value.data_type == FLOAT) {
			printf("%f	", dr->value.value.db_float);
		}
		if (dr->value.data_type == BOOL) {
			printf("%d	", dr->value.value.db_boolean);
		}
		dr = dr->next_node;
	}
}


void print_table_column_names(struct Data_Row_Node* dr, struct String tab_name) {
	while (dr != NULL) {
		printf("%s(%s)	", dr->column_name.value, tab_name.value);
		dr = dr->next_node;
	}
}


void print_joined_table_row(struct Table_Row_Lists_Bunch** trlb_cur_row_list, uint32_t* row_positions, uint32_t tab_num) {
	for (uint32_t i = 0; i < tab_num; i++)
	{
		void* row_list_buff = trlb_cur_row_list[i]->row_lists_buffer;
		uint32_t row_start_in_buff = trlb_cur_row_list[i]->row_starts_in_buffer[row_positions[i]]; // trlb_cur_row_list[i]->row_starts_in_buffer[row_positions[i]] bug
		print_table_row((struct Data_Row_Node*)((uint8_t*)row_list_buff + row_start_in_buff));
	}
	printf("\n");
}


void print_joined_table_column_names(struct Table_Row_Lists_Bunch** trlb_cur_row_list, uint32_t* row_positions, uint32_t tab_num, struct String* tab_names) {
	for (uint32_t i = 0; i < tab_num; i++)
	{
		void* row_list_buff = trlb_cur_row_list[i]->row_lists_buffer;
		uint32_t row_start_in_buff = trlb_cur_row_list[i]->row_starts_in_buffer[row_positions[i]]; // trlb_cur_row_list[i]->row_starts_in_buffer[row_positions[i]] bug
		print_table_column_names((struct Data_Row_Node*)((uint8_t*)row_list_buff + row_start_in_buff), tab_names[i]);
	}
	printf("\n");
}

void reset_row_arr_pos(struct Table_Row_Lists_Bunch** trlb_cur_row_list, uint32_t* row_positions /*in array of row starts*/, uint32_t tab_num) {

	uint32_t not_fully_fetched_row_bunch_index = 0;

	for (int32_t i = tab_num - 1; i >= 0; i--)
	{
		//try to find trlb to change
		struct Table_Row_Lists_Bunch* table = trlb_cur_row_list[i];
		if ((table->local_rows_num - 1) > row_positions[i]) {
			not_fully_fetched_row_bunch_index = i;
			row_positions[i]++;
			break;
		}
	}

	for (uint32_t i = not_fully_fetched_row_bunch_index + 1; i < tab_num; i++)
	{
		row_positions[i] = 0;
		trlb_cur_row_list[i] = trlb_cur_row_list[i - 1]->row_tails[row_positions[i - 1]];
	}

}


void print_joined_table_rows(struct Table_Chain_Result_Set* rs) {

	if (rs == NULL) {
		printf("EMPTY JOIN RESULT SET\n");
		return;
	}

	struct Table_Row_Lists_Bunch** trlb_cur_row_list = malloc(rs->number_of_joined_tables * sizeof(struct Table_Row_Lists_Bunch*));
	uint32_t* row_positions = malloc(rs->number_of_joined_tables * sizeof(uint32_t));

	struct Table_Row_Lists_Bunch* trlb = rs->rows_chain;
	for (uint32_t i = 0; i < rs->number_of_joined_tables; i++)
	{
		trlb_cur_row_list[i] = trlb;
		trlb = (trlb->row_tails == NULL) ? NULL : trlb->row_tails[0];
		row_positions[i] = 0;
	}


	printf("JOINED TABLE\n");
	print_joined_table_column_names(trlb_cur_row_list, row_positions, rs->number_of_joined_tables, rs->table_names);
	for (uint32_t i = 0; i < rs->rows_num; i++)
	{
		//print_joined_table_row(arr, rs->joined_table.number_of_joined_tables);
		print_joined_table_row(trlb_cur_row_list, row_positions, rs->number_of_joined_tables);
		reset_row_arr_pos(trlb_cur_row_list, row_positions, rs->number_of_joined_tables);
	}


	free(trlb_cur_row_list);
	free(row_positions);
}








