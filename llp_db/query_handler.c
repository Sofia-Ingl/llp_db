#include "query_handler.h"
#include "file_handler.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>



struct Table_Schema table_schema_init() {
	void* column_metadata_array = malloc(sizeof(struct Column_Info_Block) * 10);
	return (struct Table_Schema) {
			.number_of_columns = 0,
			.column_array_size = 10,
			.column_info = (struct Column_Info_Block*) column_metadata_array
	};
}



int8_t table_schema_expand(struct Table_Schema* schema, char* column_name, enum DB_Data_Type data_type) {
	printf("table_schema_expand\n");
	struct String hashed_column_name = inner_string_create(column_name);

	struct Column_Info_Block new_column = (struct Column_Info_Block){
		.column_name = hashed_column_name,
		.column_type = data_type
	};

	for (int32_t i = 0; i < schema->number_of_columns; i++) {

		if (schema->column_info->column_name.hash == hashed_column_name.hash) {

			if (strcmp((schema->column_info)[i].column_name.value, hashed_column_name.value) == 0) {
				printf("Equal col names: %s %s \n", (schema->column_info)[i].column_name.value, hashed_column_name.value);
				return -1; // уже есть такая колонка
			}
		}
	}

	if (schema->number_of_columns == schema->column_array_size) {
		printf("here\n");
		int32_t new_size = schema->column_array_size + schema->column_array_size / 2;
		schema->column_info = realloc(schema->column_info, sizeof(struct Column_Info_Block) * new_size);
		schema->column_array_size = new_size;
	}

	schema->column_info[schema->number_of_columns] = new_column;
	printf("added to schema: COL %s \n", schema->column_info[schema->number_of_columns].column_name.value);
	schema->number_of_columns++;
	return 0;
}


struct Data_Row_Node create_data_row_node(char* column_name, enum DB_Data_Type data_type, void* value_pointer) {
	struct String hashed_column_name = inner_string_create(column_name);
	union Data data;
	if (data_type == INT) {
		data.db_integer = *((int32_t*)value_pointer);
	}
	if (data_type == FLOAT) {
		data.db_float = *((float*)value_pointer);
	}
	if (data_type == BOOL) {
		data.db_float = *((enum Boolean*)value_pointer);
	}
	if (data_type == STRING) { // мб лучше копировать строку?????? потенциальная утечка памяти
		char* string = *((char**)value_pointer);
		struct String hashed_string = inner_string_create(string);
		data.db_string = hashed_string;
		//char* string = ((char*)value_pointer);
		//// copy to buffer
		//struct String hashed_string = inner_string_create(string);
		//data.db_string = hashed_string; 
	}

	struct Schema_Internals_Value schema_internal_value = (struct Schema_Internals_Value){
		.data_type = data_type,
		.value = data
	};
	return (struct Data_Row_Node) {
		.column_name = hashed_column_name,
			.value = schema_internal_value,
			.next_node = NULL
	};
}






int8_t table_create(struct File_Handle* f_handle, char* table_name, struct Table_Schema schema) {
	
	
	struct String hashed_tab_name = inner_string_create(table_name);
	create_table(f_handle, hashed_tab_name, schema);
}

struct File_Handle* file_open_or_create(char* filename) {
	struct File_Handle* file_handle = open_or_create_db_file(filename);
	// errors handle
	return file_handle;
}

int8_t table_delete(struct File_Handle* f_handle, char* table_name) {
	struct String hashed_tab_name = inner_string_create(table_name);
	delete_table(f_handle, hashed_tab_name);
}

int8_t process_insert(struct File_Handle* f_handle, struct Insert insert_command) {
	insert_row(f_handle, insert_command.table_name, insert_command.new_data);
}


//void print_table_row(struct Data_Row_Node* dr) {
//	printf("\t-ROW:\n");
//	while (dr != NULL) {
//		if (dr->value.data_type == STRING) {
//			printf("string %s\n", dr->value.value.db_string.value);
//		}
//		if (dr->value.data_type == INT) {
//			printf("int %d\n", dr->value.value.db_integer);
//		}
//		if (dr->value.data_type == FLOAT) {
//			printf("float %.3f\n", dr->value.value.db_float);
//		}
//		if (dr->value.data_type == BOOL) {
//			printf("bool %d\n", dr->value.value.db_boolean);
//		}
//		dr = dr->next_node;
//	}
//}

void print_table_row(struct Data_Row_Node* dr) {
	printf("\t-ROW:\n");
	while (dr != NULL) {
		if (dr->value.data_type == STRING) {
			printf("%s (string)   ", dr->value.value.db_string.value);
		}
		if (dr->value.data_type == INT) {
			printf("%d (int)   ", dr->value.value.db_integer);
		}
		if (dr->value.data_type == FLOAT) {
			printf("%.3f (float)   ", dr->value.value.db_float);
		}
		if (dr->value.data_type == BOOL) {
			printf("%d (bool)   ", dr->value.value.db_boolean);
		}
		dr = dr->next_node;
	}
	printf("\n");
}


void print_joined_table_row(struct Table_Row_Lists_Bunch** trlb_cur_row_list, uint32_t* row_positions, uint32_t tab_num) {
	printf("\t\t-JOINED ROW:\n");
	for (uint32_t i = 0; i < tab_num; i++)
	{
		void* row_list_buff = trlb_cur_row_list[i]->row_lists_buffer;
		uint32_t row_start_in_buff = trlb_cur_row_list[i]->row_starts_in_buffer[row_positions[i]]; // trlb_cur_row_list[i]->row_starts_in_buffer[row_positions[i]] bug
		print_table_row((struct Data_Row_Node*)((uint8_t*)row_list_buff + row_start_in_buff));
	}
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

	//struct Table_Row_Lists_Bunch* prev_table = trlb_cur_row_list[not_fully_fetched_row_bunch_index];
	//for (uint32_t i = not_fully_fetched_row_bunch_index + 1; i < tab_num; i++)
	//{
	//	row_positions[i] = 0;
	//	trlb_cur_row_list[i] = prev_table->row_tails[];
	//}

}


void print_joined_table_rows(struct Table_Chain_Result_Set* rs) {

	if (rs == NULL) {
		printf("EMPTY JOIN RESULT SET\n");
		return;
	}
	
	//struct Data_Row_Node** arr = malloc(rs->joined_table.number_of_joined_tables * sizeof(struct Data_Row_Node*));

	struct Table_Row_Lists_Bunch** trlb_cur_row_list = malloc(rs->number_of_joined_tables * sizeof(struct Table_Row_Lists_Bunch*));
	uint32_t* row_positions = malloc(rs->number_of_joined_tables * sizeof(uint32_t));

	struct Table_Row_Lists_Bunch* trlb = rs->rows_chain;
	for (uint32_t i = 0; i < rs->number_of_joined_tables; i++)
	{
		/*struct Data_Row_Node* drn = trlb->row_starts_in_buffer[0];
		arr[i] = drn;*/

		trlb_cur_row_list[i] = trlb;
		trlb = (trlb->row_tails == NULL)? NULL : trlb->row_tails[0];
		row_positions[i] = 0;
	}

	for (uint32_t i = 0; i < rs->number_of_joined_tables; i++)
	{
		/*struct Data_Row_Node* drn = trlb->row_starts_in_buffer[0];
		arr[i] = drn;*/

		printf("%p trlb pointer\n", trlb_cur_row_list[i]);
	}

	

	for (uint32_t i = 0; i < rs->rows_num; i++)
	{
		//print_joined_table_row(arr, rs->joined_table.number_of_joined_tables);
		print_joined_table_row(trlb_cur_row_list, row_positions, rs->number_of_joined_tables);
		reset_row_arr_pos(trlb_cur_row_list, row_positions, rs->number_of_joined_tables);
		printf("pos0 %d pos1 %d\n", row_positions[0], row_positions[1]);
	}


	free(trlb_cur_row_list);
	free(row_positions);
}















//void test_func(struct File_Handle* f_handle) {
//	struct String hashed_table_name = inner_string_create("tab2");
//	char* s1 = "Lis";
//	struct Data_Row_Node rn = create_data_row_node("col1", STRING, &s1);
//	char* s2 = "Lisuudhue11111";
//	struct Data_Row_Node rn2 = create_data_row_node("col2", STRING, &s2);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name, &rn);
//
//	//struct String hashed_table_name = inner_string_create("tab2");
//	s1 = "kis";
//	rn = create_data_row_node("col1", STRING, &s1);
//	s2 = "Lisuudhue11111";
//	rn2 = create_data_row_node("col2", STRING, &s2);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name, &rn);
//
//	//struct String hashed_table_name = inner_string_create("tab2");
//	s1 = "kis";
//	rn = create_data_row_node("col1", STRING, &s1);
//	s2 = "Kisuudhue";
//	rn2 = create_data_row_node("col2", STRING, &s2);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name, &rn);
//
//	s2 = "Lisuudhue11111";
//	//(union Data) {
//	//	.db_string = inner_string_create(s2)
//	//};
//	struct Schema_Internals_Value val = (struct Schema_Internals_Value){
//		.data_type = STRING,
//		.value = (union Data) {
//			.db_string = inner_string_create(s2)
//		}
//	};
//	struct Condition con = create_simple_condition("col2", val, EQUALS);
//	val = (struct Schema_Internals_Value){
//		.data_type = STRING,
//		.value = (union Data) {
//			.db_string = inner_string_create(s1)
//		}
//	};
//	struct Condition con1 = create_simple_condition("col1", val, EQUALS);
//	struct Condition con2 = create_complex_condition(&con, &con1, OR);
//	
//	struct Result_Set* rs = single_table_select(f_handle,
//		hashed_table_name,
//		&con2,
//		-1, // -1 => all cols
//		NULL,
//		1);
//
//	printf("RESULT SET:\n");
//	printf(" rs.rows_num: %d\n", rs->rows_num);
//	printf(" rs.whole_table: %d\n", rs->whole_table);
//	printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
//	printf("ROWS:\n");
//	for (uint32_t i = 0; i < rs->rows_num; i++)
//	{
//		print_table_row(rs->row_pointers[i]);
//	}
//
//	if (rs->whole_table == 0) {
//		printf("need to feth another data bunch\n");
//		rs = single_table_get_next(f_handle, rs, 10);
//		printf("TAIL RESULT SET:\n");
//		printf(" rs.rows_num: %d\n", rs->rows_num);
//		printf(" rs.whole_table: %d\n", rs->whole_table);
//		printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
//		printf("ROWS:\n");
//		for (uint32_t i = 0; i < rs->rows_num; i++)
//		{
//			print_table_row(rs->row_pointers[i]);
//		}
//	}
//
//
//	//con = create_simple_condition("col1", val, EQUALS);
//	//char* colname = "col1";
//	//struct String cn = inner_string_create(colname);
//	//rs = single_table_select(f_handle,
//	//	hashed_table_name,
//	//	&con,
//	//	1, // -1 => all cols
//	//	&cn,
//	//	13);
//
//	//printf("RESULT SET2:\n");
//	//printf(" rs.rows_num: %d\n", rs->rows_num);
//	//printf(" rs.whole_table: %d\n", rs->whole_table);
//	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
//	//printf("ROWS:\n");
//	//for (uint32_t i = 0; i < rs->rows_num; i++)
//	//{
//	//	print_table_row(rs->row_pointers[i]);
//	//}
//
//	/*char* s3 = "i";
//	rn2 = create_data_row_node("col2", STRING, &s3);
//
//	int32_t upd_rs = update_rows(f_handle, hashed_table_name, &con, &rn2);
//	printf("updated_rows %d\n", upd_rs);*/
//	//delete_rows(f_handle, hashed_table_name, &con);
//}



void test_func1(struct File_Handle* f_handle) {

	struct Table_Schema schema1 = table_schema_init();
	table_schema_expand(&schema1, "name", STRING);
	table_schema_expand(&schema1, "year", INT);

	table_create(f_handle, "course", schema1);

	struct Table_Schema schema2 = table_schema_init();
	table_schema_expand(&schema2, "name", STRING);
	table_schema_expand(&schema2, "year", INT);
	table_schema_expand(&schema2, "group_number", INT);

	table_create(f_handle, "student", schema2);

	struct Table_Schema schema3 = table_schema_init();
	table_schema_expand(&schema3, "number", INT);

	table_create(f_handle, "group", schema3);


	struct String hashed_table_name1 = inner_string_create("student");

	char* name = "Lis";
	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
	int32_t year = 1;
	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	int32_t g_number = 0;
	struct Data_Row_Node rn3 = create_data_row_node("group_number", INT, &g_number);
	rn2.next_node = &rn3;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "Adam";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	g_number = 2;
	rn3 = create_data_row_node("group_number", INT, &g_number);
	rn2.next_node = &rn3;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "Rose";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	g_number = 2;
	rn3 = create_data_row_node("group_number", INT, &g_number);
	rn2.next_node = &rn3;
	insert_row(f_handle, hashed_table_name1, &rn);


	struct String hashed_table_name2 = inner_string_create("course");

	name = "LLP";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "OS";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "Math";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);


	struct String hashed_table_name3 = inner_string_create("group");

	g_number = 0;
	struct Data_Row_Node rn4 = create_data_row_node("number", INT, &g_number);
	insert_row(f_handle, hashed_table_name3, &rn4);

	g_number = 2;
	rn4 = create_data_row_node("number", INT, &g_number);
	insert_row(f_handle, hashed_table_name3, &rn4);

	g_number = 1;
	rn4 = create_data_row_node("number", INT, &g_number);
	insert_row(f_handle, hashed_table_name3, &rn4);


	////struct Result_Set* rs = single_table_select(f_handle,
	////	hashed_table_name2,
	////	NULL,
	////	-1, // -1 => all cols
	////	NULL,
	////	10);

	////printf("COURSE RESULT SET:\n");
	////printf(" rs.rows_num: %d\n", rs->rows_num);
	////printf(" rs.whole_table: %d\n", rs->whole_table);
	////printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
	////printf("ROWS:\n");
	////for (uint32_t i = 0; i < rs->rows_num; i++)
	////{
	////	print_table_row(rs->row_pointers[i]);
	////}


	////rs = single_table_select(f_handle,
	////	hashed_table_name1,
	////	NULL,
	////	-1, // -1 => all cols
	////	NULL,
	////	10);

	////printf("STUDENT RESULT SET:\n");
	////printf(" rs.rows_num: %d\n", rs->rows_num);
	////printf(" rs.whole_table: %d\n", rs->whole_table);
	////printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
	////printf("ROWS:\n");
	////for (uint32_t i = 0; i < rs->rows_num; i++)
	////{
	////	print_table_row(rs->row_pointers[i]);
	////}

	struct String str_arr[3];
	str_arr[0] = hashed_table_name1; // student
	str_arr[1] = hashed_table_name2; //course
	str_arr[2] = hashed_table_name3; //group

	struct Join_Condition jc01 = (struct Join_Condition){
		.current_table_column_name = inner_string_create("year"),
		.related_table_column_name = inner_string_create("year"),
		.related_table_index = 0
	};

	struct Join_Condition jc02 = (struct Join_Condition){
		.current_table_column_name = inner_string_create("number"),
		.related_table_column_name = inner_string_create("group_number"),
		.related_table_index = 0
	};

	struct Join_Condition jcs[2];
	jcs[0] = jc01;
	jcs[1] = jc02;


	struct Joined_Table jt = (struct Joined_Table){
		.number_of_joined_tables = 3,
		.table_names = str_arr,
		.join_conditions = jcs
	};

	struct Condition* single_tab_cond[3];
	single_tab_cond[0] = NULL;
	single_tab_cond[1] = NULL;
	single_tab_cond[2] = NULL;

	uint32_t number_of_cols[3];
	number_of_cols[0] = -1;
	number_of_cols[1] = -1;
	number_of_cols[2] = -1;

	struct String* col_names[3];
	col_names[0] = NULL;
	col_names[1] = NULL;
	col_names[2] = NULL;


	struct Table_Chain_Result_Set* jrs = table_chain_select(f_handle, jt.number_of_joined_tables, jt.table_names, jt.join_conditions,
		single_tab_cond, number_of_cols, col_names, 5);
	
	print_joined_table_rows(jrs);

}






















void test_func2(struct File_Handle* f_handle) {

	struct Table_Schema schema1 = table_schema_init();
	table_schema_expand(&schema1, "name", STRING);
	table_schema_expand(&schema1, "year", INT);

	table_create(f_handle, "course", schema1);

	table_schema_expand(&schema1, "group_number", INT);
	
	table_create(f_handle, "student", schema1);

	struct Table_Schema schema2 = table_schema_init();
	table_schema_expand(&schema2, "number", INT);
	
	table_create(f_handle, "group", schema2);


	struct String hashed_table_name1 = inner_string_create("student");

	char* name = "Lis";
	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
	int32_t year = 1;
	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	int32_t g_number = 0;
	struct Data_Row_Node rn3 = create_data_row_node("group_number", INT, &g_number);
	rn2.next_node = &rn3;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "Adam";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	g_number = 2;
	rn3 = create_data_row_node("group_number", INT, &g_number);
	rn2.next_node = &rn3;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "Rose";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	g_number = 2;
	rn3 = create_data_row_node("group_number", INT, &g_number);
	rn2.next_node = &rn3;
	insert_row(f_handle, hashed_table_name1, &rn);


	struct String hashed_table_name2 = inner_string_create("course");

	name = "LLP";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "OS";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "Math";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);


	struct String hashed_table_name3 = inner_string_create("group");

	g_number = 0;
	rn3 = create_data_row_node("group_number", INT, &g_number);
	insert_row(f_handle, hashed_table_name1, &rn3);

	g_number = 2;
	rn3 = create_data_row_node("group_number", INT, &g_number);
	insert_row(f_handle, hashed_table_name1, &rn3);

	g_number = 1;
	rn3 = create_data_row_node("group_number", INT, &g_number);
	insert_row(f_handle, hashed_table_name1, &rn3);


	//struct Result_Set* rs = single_table_select(f_handle,
	//	hashed_table_name2,
	//	NULL,
	//	-1, // -1 => all cols
	//	NULL,
	//	10);

	//printf("COURSE RESULT SET:\n");
	//printf(" rs.rows_num: %d\n", rs->rows_num);
	//printf(" rs.whole_table: %d\n", rs->whole_table);
	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
	//printf("ROWS:\n");
	//for (uint32_t i = 0; i < rs->rows_num; i++)
	//{
	//	print_table_row(rs->row_pointers[i]);
	//}


	//rs = single_table_select(f_handle,
	//	hashed_table_name1,
	//	NULL,
	//	-1, // -1 => all cols
	//	NULL,
	//	10);

	//printf("STUDENT RESULT SET:\n");
	//printf(" rs.rows_num: %d\n", rs->rows_num);
	//printf(" rs.whole_table: %d\n", rs->whole_table);
	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
	//printf("ROWS:\n");
	//for (uint32_t i = 0; i < rs->rows_num; i++)
	//{
	//	print_table_row(rs->row_pointers[i]);
	//}

	


	uint32_t num_of_cols = -1;
	struct Condition* condit = NULL;
	struct String* col_nams = NULL;
	struct Table_Chain_Result_Set* jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
		&condit, &num_of_cols, &col_nams, 5);
	printf("As single select\n");
	print_joined_table_rows(jrs);
	delete_table(f_handle, hashed_table_name2);
	jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
		&condit, &num_of_cols, &col_nams, 5);
	print_joined_table_rows(jrs);
	printf("%d rnum\n", delete_rows(f_handle, hashed_table_name1, NULL));
	jrs = table_chain_select(f_handle, 1, &hashed_table_name1, NULL,
		&condit, &num_of_cols, &col_nams, 5);
	print_joined_table_rows(jrs);

}



void test_func3(struct File_Handle* f_handle) {

	struct Table_Schema schema = table_schema_init();
	table_schema_expand(&schema, "name", STRING);
	table_schema_expand(&schema, "year", INT);
	table_create(f_handle, "student", schema);

	table_create(f_handle, "course", schema);


	struct String hashed_table_name1 = inner_string_create("student");

	char* name = "Lis";
	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
	int32_t year = 1;
	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "Adam";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "Rose";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name1, &rn);


	struct String hashed_table_name2 = inner_string_create("course");

	name = "LLP";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "OS";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "Math";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);


	//struct Result_Set* rs = single_table_select(f_handle,
	//	hashed_table_name2,
	//	NULL,
	//	-1, // -1 => all cols
	//	NULL,
	//	10);

	//printf("COURSE RESULT SET:\n");
	//printf(" rs.rows_num: %d\n", rs->rows_num);
	//printf(" rs.whole_table: %d\n", rs->whole_table);
	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
	//printf("ROWS:\n");
	//for (uint32_t i = 0; i < rs->rows_num; i++)
	//{
	//	print_table_row(rs->row_pointers[i]);
	//}


	//rs = single_table_select(f_handle,
	//	hashed_table_name1,
	//	NULL,
	//	-1, // -1 => all cols
	//	NULL,
	//	10);

	//printf("STUDENT RESULT SET:\n");
	//printf(" rs.rows_num: %d\n", rs->rows_num);
	//printf(" rs.whole_table: %d\n", rs->whole_table);
	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
	//printf("ROWS:\n");
	//for (uint32_t i = 0; i < rs->rows_num; i++)
	//{
	//	print_table_row(rs->row_pointers[i]);
	//}

	struct String str_arr[2];
	str_arr[0] = hashed_table_name1; // student
	str_arr[1] = hashed_table_name2; //course

	struct Join_Condition jc = (struct Join_Condition){
		.current_table_column_name = inner_string_create("year"),
		.related_table_column_name = inner_string_create("year"),
		.related_table_index = 0
	};

	struct Joined_Table jt = (struct Joined_Table){
		.number_of_joined_tables = 2,
		.table_names = str_arr,
		.join_conditions = &jc
	};

	struct Condition* single_tab_cond[2];
	single_tab_cond[0] = NULL;
	single_tab_cond[1] = NULL;

	uint32_t number_of_cols[2];
	number_of_cols[0] = 1;
	number_of_cols[1] = -1;

	struct String* col_names[2];
	struct String col1_name = inner_string_create("name");
	col_names[0] = &col1_name;
	col_names[1] = NULL;


	struct Table_Chain_Result_Set* jrs = table_chain_select(f_handle, jt.number_of_joined_tables, jt.table_names, jt.join_conditions,
		single_tab_cond, number_of_cols, col_names, 3);

	print_joined_table_rows(jrs);

	jrs = table_chain_get_next(f_handle, jrs, 3);

	print_joined_table_rows(jrs);

	uint32_t num_of_cols = 1;
	struct Condition* condit = NULL;
	struct String* col_nams = NULL;
	jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
		&condit, &num_of_cols, col_names, 5);
	printf("As single select\n");
	print_joined_table_rows(jrs);

}


void test_func4(struct File_Handle* f_handle) {

	struct Table_Schema schema = table_schema_init();
	table_schema_expand(&schema, "name", STRING);
	table_schema_expand(&schema, "year", INT);
	table_create(f_handle, "student", schema);

	table_create(f_handle, "course", schema);


	struct String hashed_table_name1 = inner_string_create("student");

	char* name = "Lis";
	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
	int32_t year = 1;
	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name1, &rn);


	struct String hashed_table_name2 = inner_string_create("course");

	name = "LLP";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "Adam";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "OS";
	rn = create_data_row_node("name", STRING, &name);
	year = 2;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	name = "Rose";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name1, &rn);

	name = "Math";
	rn = create_data_row_node("name", STRING, &name);
	year = 1;
	rn2 = create_data_row_node("year", INT, &year);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name2, &rn);

	struct File_Table_Schema_Metadata tab_info = get_table_schema_data(f_handle, hashed_table_name2);
	printf("TAB INFO: %s\n", tab_info.table_name.value);
	for (uint32_t i = 0; i < tab_info.columns_number; i++)
	{
		printf("%s\n", tab_info.columns_data[i].column_name.value);
	}

	//normalize_db_file(f_handle);
}