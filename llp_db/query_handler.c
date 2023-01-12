#include "query_handler.h"
#include "file_handler.h"
#include "printers.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define DEFAULT_CRITICAL_GAP_RATE 0.2
#define DEFAULT_CRITICAL_GAP_SIZE 4096

struct Table_Schema table_schema_init() {
	void* column_metadata_array = malloc(sizeof(struct Column_Info_Block) * 10);
	return (struct Table_Schema) {
			.number_of_columns = 0,
			.column_array_size = 10,
			.column_info = (struct Column_Info_Block*) column_metadata_array
	};
}

int8_t table_schema_expand(struct Table_Schema* schema, char* column_name, enum DB_Data_Type data_type) {

	struct String hashed_column_name = inner_string_create(column_name);

	struct Column_Info_Block new_column = (struct Column_Info_Block){
		.column_name = hashed_column_name,
		.column_type = data_type
	};

	for (int32_t i = 0; i < schema->number_of_columns; i++) {

		if (schema->column_info->column_name.hash == hashed_column_name.hash) {

			if (strcmp((schema->column_info)[i].column_name.value, hashed_column_name.value) == 0) {
				return -1; // уже есть такая колонка
			}
		}
	}

	if (schema->number_of_columns == schema->column_array_size) {
		int32_t new_size = schema->column_array_size + schema->column_array_size / 2;
		schema->column_info = realloc(schema->column_info, sizeof(struct Column_Info_Block) * new_size);
		schema->column_array_size = new_size;
	}

	schema->column_info[schema->number_of_columns] = new_column;
	schema->number_of_columns++;
	return 0;
}


struct File_Handle* file_open_or_create(char* filename) {
	struct File_Handle* file_handle = open_or_create_db_file(filename, DEFAULT_CRITICAL_GAP_RATE, DEFAULT_CRITICAL_GAP_SIZE);
	// errors handle
	return file_handle;
}

struct File_Handle* file_open_or_create_with_gap_rate(char* filename, float critical_gap_rate) {
	struct File_Handle* file_handle = open_or_create_db_file(filename, critical_gap_rate, DEFAULT_CRITICAL_GAP_SIZE);
	// errors handle
	return file_handle;
}

struct File_Handle* file_open_or_create_with_gap_sz(char* filename, uint32_t critical_gap_sz) {
	struct File_Handle* file_handle = open_or_create_db_file(filename, DEFAULT_CRITICAL_GAP_RATE, critical_gap_sz);
	// errors handle
	return file_handle;
}

struct File_Handle* file_open_or_create_with_gap_rate_and_sz(char* filename, float critical_gap_rate, uint32_t critical_gap_sz) {
	struct File_Handle* file_handle = open_or_create_db_file(filename, critical_gap_rate, critical_gap_sz);
	// errors handle
	return file_handle;
}

void file_close(struct File_Handle* f_handle, uint8_t normalize) {
	close_or_normalize_db_file(f_handle, normalize);
}

int8_t table_create(struct File_Handle* f_handle, char* table_name, struct Table_Schema schema) {
	
	struct String hashed_tab_name = inner_string_create(table_name);
	create_table(f_handle, hashed_tab_name, schema);
}

int8_t table_delete(struct File_Handle* f_handle, char* table_name, enum Normalization normalization) {
	struct String hashed_tab_name = inner_string_create(table_name);
	delete_table(f_handle, hashed_tab_name, normalization);
}

int32_t process_insert(struct File_Handle* f_handle, struct Insert insert_command) {
	return insert_row(f_handle, insert_command.table_name, insert_command.new_data);
}

int32_t process_update(struct File_Handle* f_handle, struct Update update_command, enum Normalization normalization) {
	return update_rows(f_handle, update_command.table_name, update_command.condition, update_command.new_data, normalization);
}

int32_t process_delete(struct File_Handle* f_handle, struct Delete delete_command, enum Normalization normalization) {
	return delete_rows(f_handle, delete_command.table_name, delete_command.condition, normalization);
}

struct Table_Chain_Result_Set* process_select(struct File_Handle* f_handle, struct Select select_command) {
	
	return process_select_with_row_num(f_handle, select_command, DEFAULT_ROW_NUM_IN_SELECT_QUERY);
}

struct Table_Chain_Result_Set* result_set_get_next(struct File_Handle* f_handle, struct Table_Chain_Result_Set* result_set) {

	return table_chain_get_next(f_handle, result_set, result_set->rows_num);
}

struct Table_Chain_Result_Set* process_select_with_row_num(struct File_Handle* f_handle, struct Select select_command, uint32_t max_row_num) {
	
	int32_t number_of_joined_tables = 0;
	struct String* table_names;
	struct Join_Condition* join_conditions;
	struct Condition** conditions_on_single_tables;
	uint32_t* number_of_columns_from_each_table;
	struct String** column_names;
	if (select_command.is_single_table_select) {
		number_of_joined_tables = 1;
		struct Single_Table_Select sts = select_command.query_details.single_table_select;
		table_names = &sts.table_name;
		join_conditions = NULL;
		conditions_on_single_tables = &sts.condition;
		number_of_columns_from_each_table = &sts.number_of_columns;
		column_names = &sts.column_names;
	}
	else {
		
		struct Joined_Table_Select jts = select_command.query_details.joined_table_select;
		table_names = jts.joined_table.table_names;
		number_of_joined_tables = jts.joined_table.number_of_joined_tables;
		join_conditions = jts.joined_table.join_conditions;
		conditions_on_single_tables = jts.conditions;
		number_of_columns_from_each_table = jts.number_of_columns_from_each_table;
		column_names = jts.column_names;
	}

	return table_chain_select(f_handle, 
		number_of_joined_tables, 
		table_names, join_conditions, 
		conditions_on_single_tables, 
		number_of_columns_from_each_table, 
		column_names, 
		max_row_num);
}

/*for testing*/
uint32_t get_file_sz(struct File_Handle* f_handle) {
	return find_file_end(f_handle);
}


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