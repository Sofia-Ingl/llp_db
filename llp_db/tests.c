#include "query_handler.h"










//#include "file_handler.h"
//
//
//
//
//
//void test_func1(struct File_Handle* f_handle) {
//
//	struct Table_Schema schema1 = table_schema_init();
//	table_schema_expand(&schema1, "name", STRING);
//	table_schema_expand(&schema1, "year", INT);
//
//	table_create(f_handle, "course", schema1);
//
//	struct Table_Schema schema2 = table_schema_init();
//	table_schema_expand(&schema2, "name", STRING);
//	table_schema_expand(&schema2, "year", INT);
//	table_schema_expand(&schema2, "group_number", INT);
//
//	table_create(f_handle, "student", schema2);
//
//	struct Table_Schema schema3 = table_schema_init();
//	table_schema_expand(&schema3, "number", INT);
//
//	table_create(f_handle, "group", schema3);
//
//
//	struct String hashed_table_name1 = inner_string_create("student");
//
//	char* name = "Lis";
//	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
//	int32_t year = 1;
//	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	int32_t g_number = 0;
//	struct Data_Row_Node rn3 = create_data_row_node("group_number", INT, &g_number);
//	rn2.next_node = &rn3;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "Adam";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	g_number = 2;
//	rn3 = create_data_row_node("group_number", INT, &g_number);
//	rn2.next_node = &rn3;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "Rose";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	g_number = 2;
//	rn3 = create_data_row_node("group_number", INT, &g_number);
//	rn2.next_node = &rn3;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//
//	struct String hashed_table_name2 = inner_string_create("course");
//
//	name = "LLP";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "OS";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "Math";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//
//	struct String hashed_table_name3 = inner_string_create("group");
//
//	g_number = 0;
//	struct Data_Row_Node rn4 = create_data_row_node("number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name3, &rn4);
//
//	g_number = 2;
//	rn4 = create_data_row_node("number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name3, &rn4);
//
//	g_number = 1;
//	rn4 = create_data_row_node("number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name3, &rn4);
//
//	struct String str_arr[3];
//	str_arr[0] = hashed_table_name1; // student
//	str_arr[1] = hashed_table_name2; //course
//	str_arr[2] = hashed_table_name3; //group
//
//	struct Join_Condition jc01 = (struct Join_Condition){
//		.current_table_column_name = inner_string_create("year"),
//		.related_table_column_name = inner_string_create("year"),
//		.related_table_index = 0
//	};
//
//	struct Join_Condition jc02 = (struct Join_Condition){
//		.current_table_column_name = inner_string_create("number"),
//		.related_table_column_name = inner_string_create("group_number"),
//		.related_table_index = 0
//	};
//
//	struct Join_Condition jcs[2];
//	jcs[0] = jc01;
//	jcs[1] = jc02;
//
//
//	struct Joined_Table jt = (struct Joined_Table){
//		.number_of_joined_tables = 3,
//		.table_names = str_arr,
//		.join_conditions = jcs
//	};
//
//	struct Condition* single_tab_cond[3];
//	single_tab_cond[0] = NULL;
//	single_tab_cond[1] = NULL;
//	single_tab_cond[2] = NULL;
//
//	uint32_t number_of_cols[3];
//	number_of_cols[0] = -1;
//	number_of_cols[1] = -1;
//	number_of_cols[2] = -1;
//
//	struct String* col_names[3];
//	col_names[0] = NULL;
//	col_names[1] = NULL;
//	col_names[2] = NULL;
//
//
//	struct Table_Chain_Result_Set* jrs = table_chain_select(f_handle, jt.number_of_joined_tables, jt.table_names, jt.join_conditions,
//		single_tab_cond, number_of_cols, col_names, 5);
//
//	print_joined_table_rows(jrs);
//
//}
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//void test_func2(struct File_Handle* f_handle) {
//
//	struct Table_Schema schema1 = table_schema_init();
//	table_schema_expand(&schema1, "name", STRING);
//	table_schema_expand(&schema1, "year", INT);
//
//	table_create(f_handle, "course", schema1);
//
//	table_schema_expand(&schema1, "group_number", INT);
//
//	table_create(f_handle, "student", schema1);
//
//	struct Table_Schema schema2 = table_schema_init();
//	table_schema_expand(&schema2, "number", INT);
//
//	table_create(f_handle, "group", schema2);
//
//
//	struct String hashed_table_name1 = inner_string_create("student");
//
//	char* name = "Lis";
//	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
//	int32_t year = 1;
//	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	int32_t g_number = 0;
//	struct Data_Row_Node rn3 = create_data_row_node("group_number", INT, &g_number);
//	rn2.next_node = &rn3;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "Adam";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	g_number = 2;
//	rn3 = create_data_row_node("group_number", INT, &g_number);
//	rn2.next_node = &rn3;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "Rose";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	g_number = 2;
//	rn3 = create_data_row_node("group_number", INT, &g_number);
//	rn2.next_node = &rn3;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//
//	struct String hashed_table_name2 = inner_string_create("course");
//
//	name = "LLP";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "OS";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "Math";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//
//	struct String hashed_table_name3 = inner_string_create("group");
//
//	g_number = 0;
//	rn3 = create_data_row_node("group_number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name1, &rn3);
//
//	g_number = 2;
//	rn3 = create_data_row_node("group_number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name1, &rn3);
//
//	g_number = 1;
//	rn3 = create_data_row_node("group_number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name1, &rn3);
//
//
//	//struct Result_Set* rs = single_table_select(f_handle,
//	//	hashed_table_name2,
//	//	NULL,
//	//	-1, // -1 => all cols
//	//	NULL,
//	//	10);
//
//	//printf("COURSE RESULT SET:\n");
//	//printf(" rs.rows_num: %d\n", rs->rows_num);
//	//printf(" rs.whole_table: %d\n", rs->whole_table);
//	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
//	//printf("ROWS:\n");
//	//for (uint32_t i = 0; i < rs->rows_num; i++)
//	//{
//	//	print_table_row(rs->row_pointers[i]);
//	//}
//
//
//	//rs = single_table_select(f_handle,
//	//	hashed_table_name1,
//	//	NULL,
//	//	-1, // -1 => all cols
//	//	NULL,
//	//	10);
//
//	//printf("STUDENT RESULT SET:\n");
//	//printf(" rs.rows_num: %d\n", rs->rows_num);
//	//printf(" rs.whole_table: %d\n", rs->whole_table);
//	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
//	//printf("ROWS:\n");
//	//for (uint32_t i = 0; i < rs->rows_num; i++)
//	//{
//	//	print_table_row(rs->row_pointers[i]);
//	//}
//
//
//
//
//	uint32_t num_of_cols = -1;
//	struct Condition* condit = NULL;
//	struct String* col_nams = NULL;
//	struct Table_Chain_Result_Set* jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
//		&condit, &num_of_cols, &col_nams, 5);
//	printf("As single select\n");
//	print_joined_table_rows(jrs);
//	delete_table(f_handle, hashed_table_name2);
//	jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
//		&condit, &num_of_cols, &col_nams, 5);
//	print_joined_table_rows(jrs);
//	printf("%d rnum\n", delete_rows(f_handle, hashed_table_name1, NULL));
//	jrs = table_chain_select(f_handle, 1, &hashed_table_name1, NULL,
//		&condit, &num_of_cols, &col_nams, 5);
//	print_joined_table_rows(jrs);
//
//}
//
//
//
//void test_func3(struct File_Handle* f_handle) {
//
//	struct Table_Schema schema = table_schema_init();
//	table_schema_expand(&schema, "name", STRING);
//	table_schema_expand(&schema, "year", INT);
//	table_create(f_handle, "student", schema);
//
//	table_create(f_handle, "course", schema);
//
//
//	struct String hashed_table_name1 = inner_string_create("student");
//
//	char* name = "Lis";
//	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
//	int32_t year = 1;
//	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "Adam";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "Rose";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//
//	struct String hashed_table_name2 = inner_string_create("course");
//
//	name = "LLP";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "OS";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "Math";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//
//	//struct Result_Set* rs = single_table_select(f_handle,
//	//	hashed_table_name2,
//	//	NULL,
//	//	-1, // -1 => all cols
//	//	NULL,
//	//	10);
//
//	//printf("COURSE RESULT SET:\n");
//	//printf(" rs.rows_num: %d\n", rs->rows_num);
//	//printf(" rs.whole_table: %d\n", rs->whole_table);
//	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
//	//printf("ROWS:\n");
//	//for (uint32_t i = 0; i < rs->rows_num; i++)
//	//{
//	//	print_table_row(rs->row_pointers[i]);
//	//}
//
//
//	//rs = single_table_select(f_handle,
//	//	hashed_table_name1,
//	//	NULL,
//	//	-1, // -1 => all cols
//	//	NULL,
//	//	10);
//
//	//printf("STUDENT RESULT SET:\n");
//	//printf(" rs.rows_num: %d\n", rs->rows_num);
//	//printf(" rs.whole_table: %d\n", rs->whole_table);
//	//printf(" rs.next_table_row_offset: %d\n", rs->next_table_row_offset);
//	//printf("ROWS:\n");
//	//for (uint32_t i = 0; i < rs->rows_num; i++)
//	//{
//	//	print_table_row(rs->row_pointers[i]);
//	//}
//
//	struct String str_arr[2];
//	str_arr[0] = hashed_table_name1; // student
//	str_arr[1] = hashed_table_name2; //course
//
//	struct Join_Condition jc = (struct Join_Condition){
//		.current_table_column_name = inner_string_create("year"),
//		.related_table_column_name = inner_string_create("year"),
//		.related_table_index = 0
//	};
//
//	struct Joined_Table jt = (struct Joined_Table){
//		.number_of_joined_tables = 2,
//		.table_names = str_arr,
//		.join_conditions = &jc
//	};
//
//	struct Condition* single_tab_cond[2];
//	single_tab_cond[0] = NULL;
//	single_tab_cond[1] = NULL;
//
//	uint32_t number_of_cols[2];
//	number_of_cols[0] = 1;
//	number_of_cols[1] = -1;
//
//	struct String* col_names[2];
//	struct String col1_name = inner_string_create("name");
//	col_names[0] = &col1_name;
//	col_names[1] = NULL;
//
//
//	struct Table_Chain_Result_Set* jrs = table_chain_select(f_handle, jt.number_of_joined_tables, jt.table_names, jt.join_conditions,
//		single_tab_cond, number_of_cols, col_names, 3);
//
//	print_joined_table_rows(jrs);
//
//	jrs = table_chain_get_next(f_handle, jrs, 3);
//
//	print_joined_table_rows(jrs);
//
//	uint32_t num_of_cols = 1;
//	struct Condition* condit = NULL;
//	struct String* col_nams = NULL;
//	jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
//		&condit, &num_of_cols, col_names, 5);
//	printf("As single select\n");
//	print_joined_table_rows(jrs);
//
//}
//
//
//void test_func4(struct File_Handle* f_handle) {
//
//	struct Table_Schema schema = table_schema_init();
//	table_schema_expand(&schema, "name", STRING);
//	table_schema_expand(&schema, "year", INT);
//	table_create(f_handle, "student", schema);
//
//	table_create(f_handle, "course", schema);
//
//
//	struct String hashed_table_name1 = inner_string_create("student");
//
//	char* name = "Lis";
//	struct Data_Row_Node rn = create_data_row_node("name", STRING, &name);
//	int32_t year = 1;
//	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//
//	struct String hashed_table_name2 = inner_string_create("course");
//
//	name = "LLP";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "Adam";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "OS";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 2;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	name = "Rose";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name1, &rn);
//
//	name = "Math";
//	rn = create_data_row_node("name", STRING, &name);
//	year = 1;
//	rn2 = create_data_row_node("year", INT, &year);
//	rn.next_node = &rn2;
//	insert_row(f_handle, hashed_table_name2, &rn);
//
//	struct File_Table_Schema_Metadata tab_info = get_table_schema_data(f_handle, hashed_table_name2);
//	printf("TAB INFO: %s\n", tab_info.table_name.value);
//	for (uint32_t i = 0; i < tab_info.columns_number; i++)
//	{
//		printf("%s\n", tab_info.columns_data[i].column_name.value);
//	}
//
//	//normalize_db_file(f_handle);
//}