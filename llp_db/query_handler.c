#include "query_handler.h"
#include "file_handler.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

uint32_t hash(char* string, uint32_t st_len) {
	return 0;
}

struct Table_Schema table_schema_init() {
	void* column_metadata_array = malloc(sizeof(struct Column_Info_Block) * 10);
	return (struct Table_Schema) {
			.number_of_columns = 0,
			.column_array_size = 10,
			.column_info = (struct Column_Info_Block*) column_metadata_array
	};
}

struct String inner_string_create(char* str) {
	int32_t str_length = strlen(str);

	return (struct String) {
		.hash = hash(str, str_length),
			.length = str_length,
			.value = str
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


struct Condition create_simple_condition(char* column_name, struct Schema_Internals_Value val, enum Condition_Relation relation) {
	
	struct Simple_Condition simp_cond = {
		.column_name = inner_string_create(column_name),
		.relation = relation,
		.right_part = val
	};
	union Condition_Union cond_union = {
		.simple_condition = simp_cond
	};
	return (struct Condition) {
		.is_simple = 1,
		.condition = cond_union
	};
}

struct Condition create_complex_condition(struct Condition* left, struct Condition* right, enum Condition_Chain_Relation relation) {

	struct Complex_Condition complex_cond = (struct Complex_Condition){
		.left = left,
		.relation = relation,
		.right = right
	};
	union Condition_Union cond_union = {
		.complex_condition = complex_cond
	};
	return (struct Condition) {
		.is_simple = 0,
		.condition = cond_union
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


void print_table_row(struct Data_Row_Node* dr) {
	printf("\t-ROW:\n");
	while (dr != NULL) {
		if (dr->value.data_type == STRING) {
			printf("string %s\n", dr->value.value.db_string.value);
		}
		if (dr->value.data_type == INT) {
			printf("int %d\n", dr->value.value.db_integer);
		}
		if (dr->value.data_type == FLOAT) {
			printf("float %.3f\n", dr->value.value.db_float);
		}
		if (dr->value.data_type == BOOL) {
			printf("bool %d\n", dr->value.value.db_boolean);
		}
		dr = dr->next_node;
	}
}


void test_func(struct File_Handle* f_handle) {
	struct String hashed_table_name = inner_string_create("tab2");
	char* s1 = "Lis";
	struct Data_Row_Node rn = create_data_row_node("col1", STRING, &s1);
	char* s2 = "Lisuudhue11111";
	struct Data_Row_Node rn2 = create_data_row_node("col2", STRING, &s2);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name, &rn);

	//struct String hashed_table_name = inner_string_create("tab2");
	s1 = "kis";
	rn = create_data_row_node("col1", STRING, &s1);
	s2 = "Lisuudhue11111";
	rn2 = create_data_row_node("col2", STRING, &s2);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name, &rn);

	//struct String hashed_table_name = inner_string_create("tab2");
	s1 = "kis";
	rn = create_data_row_node("col1", STRING, &s1);
	s2 = "Kisuudhue";
	rn2 = create_data_row_node("col2", STRING, &s2);
	rn.next_node = &rn2;
	insert_row(f_handle, hashed_table_name, &rn);

	s2 = "Lisuudhue11111";
	//(union Data) {
	//	.db_string = inner_string_create(s2)
	//};
	struct Schema_Internals_Value val = (struct Schema_Internals_Value){
		.data_type = STRING,
		.value = (union Data) {
			.db_string = inner_string_create(s2)
		}
	};
	struct Condition con = create_simple_condition("col2", val, EQUALS);
	
	struct Result_Set rs = single_table_select(f_handle,
		hashed_table_name,
		&con,
		-1, // -1 => all cols
		NULL,
		13);

	printf("RESULT SET:\n");
	printf(" rs.rows_num: %d\n", rs.rows_num);
	printf(" rs.whole_table: %d\n", rs.whole_table);
	printf(" rs.next_table_row_offset: %d\n", rs.next_table_row_offset);
	printf("ROWS:\n");
	for (uint32_t i = 0; i < rs.rows_num; i++)
	{
		print_table_row(rs.row_pointers[i]);
	}


	con = create_simple_condition("col1", val, EQUALS);

	rs = single_table_select(f_handle,
		hashed_table_name,
		&con,
		-1, // -1 => all cols
		NULL,
		13);

	printf("RESULT SET2:\n");
	printf(" rs.rows_num: %d\n", rs.rows_num);
	printf(" rs.whole_table: %d\n", rs.whole_table);
	printf(" rs.next_table_row_offset: %d\n", rs.next_table_row_offset);
	printf("ROWS:\n");
	for (uint32_t i = 0; i < rs.rows_num; i++)
	{
		print_table_row(rs.row_pointers[i]);
	}

	/*char* s3 = "i";
	rn2 = create_data_row_node("col2", STRING, &s3);

	int32_t upd_rs = update_rows(f_handle, hashed_table_name, &con, &rn2);
	printf("updated_rows %d\n", upd_rs);*/
	//delete_rows(f_handle, hashed_table_name, &con);
}