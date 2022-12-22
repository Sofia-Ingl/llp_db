#include "query_handler.h"
#include "printers.h"
#include "file_handler.h"
#include <time.h>  

void create_student_table(struct File_Handle* f_handle) {
	struct Table_Schema stud_schema = table_schema_init();
	
	table_schema_expand(&stud_schema, "name", STRING);
	table_schema_expand(&stud_schema, "group_number", STRING);
	table_schema_expand(&stud_schema, "actual", BOOL);

	table_create(f_handle, "student", stud_schema);
}


void insert_into_student_table(struct File_Handle* f_handle) {
	

	char* name = "Inglikova";
	char* group = "P33312";
	enum Boolean actual = TRUE;


	struct Data_Row_Node rn0 = create_data_row_node("name", STRING, &name);
	struct Data_Row_Node rn1 = create_data_row_node("group_number", STRING, &group);
	rn0.next_node = &rn1;
	struct Data_Row_Node rn2 = create_data_row_node("actual", BOOL, &actual);
	rn1.next_node = &rn2;

	struct Insert ins = (struct Insert){
			.table_name = inner_string_create("student"),
			.new_data = &rn0
	};
	process_insert(f_handle, ins);

	name = "Erehinsky";
	group = "P33312";
	actual = TRUE;

	rn0 = create_data_row_node("name", STRING, &name);
	rn1 = create_data_row_node("group_number", STRING, &group);
	rn0.next_node = &rn1;
	rn2 = create_data_row_node("actual", BOOL, &actual);
	rn1.next_node = &rn2;

	ins.new_data = &rn0;
	process_insert(f_handle, ins);


	name = "Kiyko";
	group = "P3233";
	actual = FALSE;

	rn0 = create_data_row_node("name", STRING, &name);
	rn1 = create_data_row_node("group_number", STRING, &group);
	rn0.next_node = &rn1;
	rn2 = create_data_row_node("actual", BOOL, &actual);
	rn1.next_node = &rn2;

	ins.new_data = &rn0;
	process_insert(f_handle, ins);

}


void create_group_table(struct File_Handle* f_handle) {
	struct Table_Schema schema = table_schema_init();

	table_schema_expand(&schema, "number", STRING);
	table_schema_expand(&schema, "year", INT);
	table_schema_expand(&schema, "curriculum_code", STRING);

	table_create(f_handle, "group", schema);
}


void insert_into_group_table(struct File_Handle* f_handle) {

	char* number = "P33312";
	int32_t year = 3;
	char* curriculum_code = "SEaCE_1";

	struct Data_Row_Node rn0 = create_data_row_node("number", STRING, &number);
	struct Data_Row_Node rn1 = create_data_row_node("year", INT, &year);
	rn0.next_node = &rn1;
	struct Data_Row_Node rn2 = create_data_row_node("curriculum_code", STRING, &curriculum_code);
	rn1.next_node = &rn2;

	struct Insert ins = (struct Insert){
			.table_name = inner_string_create("group"),
			.new_data = &rn0
	};
	process_insert(f_handle, ins);

	number = "P3233";
	year = 2;
	curriculum_code = "SEaCE_1";

	rn0 = create_data_row_node("number", STRING, &number);
	rn1 = create_data_row_node("year", INT, &year);
	rn0.next_node = &rn1;
	rn2 = create_data_row_node("curriculum_code", STRING, &curriculum_code);
	rn1.next_node = &rn2;

	ins.new_data = &rn0;
	process_insert(f_handle, ins);


}

void create_curriculum_table(struct File_Handle* f_handle) {
	struct Table_Schema schema = table_schema_init();

	table_schema_expand(&schema, "code", STRING);
	table_schema_expand(&schema, "name", STRING);

	table_create(f_handle, "curriculum", schema);
}

void insert_into_curriculum_table(struct File_Handle* f_handle) {

	char* code = "SEaCE_1";
	char* name = "Software engineering and... - 1";

	struct Data_Row_Node rn0 = create_data_row_node("code", STRING, &code);
	struct Data_Row_Node rn1 = create_data_row_node("name", STRING, &name);
	rn0.next_node = &rn1;
	
	struct Insert ins = (struct Insert){
			.table_name = inner_string_create("curriculum"),
			.new_data = &rn0
	};
	process_insert(f_handle, ins);

}


void create_subject_table(struct File_Handle* f_handle) {
	struct Table_Schema schema = table_schema_init();

	table_schema_expand(&schema, "code", STRING);
	table_schema_expand(&schema, "name", STRING);

	table_create(f_handle, "subject", schema);
}

void insert_into_subject_table(struct File_Handle* f_handle) {

	char* code = "LLP";
	char* name = "Low lvl programming";

	struct Data_Row_Node rn0 = create_data_row_node("code", STRING, &code);
	struct Data_Row_Node rn1 = create_data_row_node("name", STRING, &name);
	rn0.next_node = &rn1;

	struct Insert ins = (struct Insert){
			.table_name = inner_string_create("subject"),
			.new_data = &rn0
	};
	process_insert(f_handle, ins);

	code = "OS";
	name = "Operation systems";

	rn0 = create_data_row_node("code", STRING, &code);
	rn1 = create_data_row_node("name", STRING, &name);
	rn0.next_node = &rn1;

	ins.new_data = &rn0;
	process_insert(f_handle, ins);


	code = "WEB";
	name = "Web programming";

	rn0 = create_data_row_node("code", STRING, &code);
	rn1 = create_data_row_node("name", STRING, &name);
	rn0.next_node = &rn1;

	ins.new_data = &rn0;
	process_insert(f_handle, ins);

}

void create_curriculum_subject_relation_table(struct File_Handle* f_handle) {
	struct Table_Schema schema = table_schema_init();

	table_schema_expand(&schema, "curriculum_code", STRING);
	table_schema_expand(&schema, "subject_code", STRING);
	table_schema_expand(&schema, "year", INT);

	table_create(f_handle, "cur_sub_relation", schema);
}


void insert_into_curriculum_subject_relation_table(struct File_Handle* f_handle) {

	char* curriculum_code = "SEaCE_1";
	char* subject_code = "LLP";
	int32_t year = 3;

	struct Data_Row_Node rn0 = create_data_row_node("curriculum_code", STRING, &curriculum_code);
	struct Data_Row_Node rn1 = create_data_row_node("subject_code", STRING, &subject_code);
	rn0.next_node = &rn1;
	struct Data_Row_Node rn2 = create_data_row_node("year", INT, &year);
	rn1.next_node = &rn2;

	struct Insert ins = (struct Insert){
			.table_name = inner_string_create("cur_sub_relation"),
			.new_data = &rn0
	};
	process_insert(f_handle, ins);



	curriculum_code = "SEaCE_1";
	subject_code = "OS";
	year = 3;

	rn0 = create_data_row_node("curriculum_code", STRING, &curriculum_code);
	rn1 = create_data_row_node("subject_code", STRING, &subject_code);
	rn0.next_node = &rn1;
	rn2 = create_data_row_node("year", INT, &year);
	rn1.next_node = &rn2;

	ins.new_data = &rn0;
	process_insert(f_handle, ins);



	curriculum_code = "SEaCE_1";
	subject_code = "WEB";
	year = 2;

	rn0 = create_data_row_node("curriculum_code", STRING, &curriculum_code);
	rn1 = create_data_row_node("subject_code", STRING, &subject_code);
	rn0.next_node = &rn1;
	rn2 = create_data_row_node("year", INT, &year);
	rn1.next_node = &rn2;

	ins.new_data = &rn0;
	process_insert(f_handle, ins);

}



void prepare_short_test_schema(struct File_Handle* fh) {
	
	create_student_table(fh);
	create_group_table(fh);
	create_curriculum_table(fh);
	create_subject_table(fh);
	create_curriculum_subject_relation_table(fh);

	insert_into_student_table(fh);
	insert_into_group_table(fh);
	insert_into_curriculum_table(fh);
	insert_into_subject_table(fh);
	insert_into_curriculum_subject_relation_table(fh);

}


void test_select_on_short_test_schema(struct File_Handle* fh) {

	printf("---task---\n");
	
	printf("select all data about actual students having LLP in their curriculum:\n");
	
	printf("---sql---\n");
	
	printf("select * from student\n");
	printf("join group on student.group_number = group.number\n");
	printf("join curriculum on curriculum.code = group.curriculum_code\n");
	printf("join cur_sub_relation on curriculum.code = cur_sub_relation.curriculum_code\n");
	printf("join subject on subject.code = cur_sub_relation.subject_code\n");
	printf("where (subject.code == 'LLP')\n");
	printf("and (student.actual == TRUE)\n");

	char* char_table_names[5];
	char_table_names[0] = "student";
	char_table_names[1] = "group";
	char_table_names[2] = "curriculum";
	char_table_names[3] = "cur_sub_relation";
	char_table_names[4] = "subject";

	struct String table_names[5];
	for (size_t i = 0; i < 5; i++)
	{
		table_names[i] = inner_string_create(char_table_names[i]);
	}

	/*table_names[0] = inner_string_create("student");
	table_names[1] = inner_string_create("group");
	table_names[2] = inner_string_create("curriculum");
	table_names[3] = inner_string_create("cur_sub_relation");
	table_names[4] = inner_string_create("subject");*/

	struct Join_Condition join_conditions[4];
	
	//join group on student.group_number = group.number
	join_conditions[0].current_table_column_name = inner_string_create("number");
	join_conditions[0].related_table_column_name = inner_string_create("group_number");
	join_conditions[0].related_table_index = 0;
	
	//join curriculum on curriculum.code = group.curriculum_code
	join_conditions[1].current_table_column_name = inner_string_create("code");
	join_conditions[1].related_table_column_name = inner_string_create("curriculum_code");
	join_conditions[1].related_table_index = 1;

	//join cur_sub_relation on curriculum.code = cur_sub_relation.curriculum_code
	join_conditions[2].current_table_column_name = inner_string_create("curriculum_code");
	join_conditions[2].related_table_column_name = inner_string_create("code");
	join_conditions[2].related_table_index = 2;

	//join subject on subject.code = cur_sub_relation.subject_code
	join_conditions[3].current_table_column_name = inner_string_create("code");
	join_conditions[3].related_table_column_name = inner_string_create("subject_code");
	join_conditions[3].related_table_index = 3;

	
	struct Joined_Table jt = (struct Joined_Table){
		.number_of_joined_tables = 5,
		.table_names = table_names,
		.join_conditions = join_conditions
	};


	uint32_t number_of_columns_from_each_table[5];
	struct String* column_names[5];
	struct Condition* conditions[5];

	for (size_t i = 0; i < 5; i++)
	{
		number_of_columns_from_each_table[i] = -1; // all columns
		column_names[i] = NULL;
		conditions[i] = NULL;
	}

	//where (subject.name == 'LLP')
	struct Schema_Internals_Value val5;
	val5.data_type = STRING;
	char* target_subj = "LLP";
	val5.value.db_string = inner_string_create(target_subj);
	struct Condition cond_5 = create_simple_condition("code", val5, EQUALS);
	conditions[4] = &cond_5;

	//and (student.actual == TRUE)
	struct Schema_Internals_Value val1;
	val1.data_type = BOOL;
	val1.value.db_boolean = TRUE;
	struct Condition cond_1 = create_simple_condition("actual", val1, EQUALS);
	conditions[0] = &cond_1;


	struct Joined_Table_Select jts = (struct Joined_Table_Select){
		.joined_table = jt,
		.column_names = column_names,
		.conditions = conditions,
		.number_of_columns_from_each_table = number_of_columns_from_each_table
	};

	union Select_Union su = (union Select_Union){
		.joined_table_select = jts
	};

	struct Select sel = (struct Select){
		.is_single_table_select = 0,
		.query_details = su
	};

	struct Table_Chain_Result_Set* rs = process_select_with_row_num(fh, sel, 1);
	print_joined_table_rows(rs);
	while (rs->probably_has_next == 1) {
		rs = result_set_get_next(fh, rs);
		print_joined_table_rows(rs);
	}

}


void process_student_inserts_from_text_format() {

}




void create_int_table(struct File_Handle* f_handle) {
	struct Table_Schema schema = table_schema_init();

	table_schema_expand(&schema, "col1", INT);

	table_create(f_handle, "tab1", schema);
}


//struct Single_Table_Select {
//	struct String table_name;
//	uint32_t number_of_columns; // -1 == all cols
//	struct String* column_names;
//	struct Condition* condition;
//};
//
//union Select_Union {
//	struct Single_Table_Select single_table_select;
//	struct Joined_Table_Select joined_table_select;
//};
//
//struct Select {
//	uint8_t is_single_table_select;
//	union Select_Union query_details;
//};

void asymptotics_testing(struct File_Handle* f_handle) {
	printf("ASYMPTESTING!\n");
	create_int_table(f_handle);
	struct Insert ins = (struct Insert){
			.table_name = inner_string_create("tab1"),
			.new_data = NULL
	};
	struct Schema_Internals_Value val;
	val.data_type = INT;
	val.value.db_integer = 1;
	struct Condition cond = create_simple_condition("col1", val, EQUALS);
	struct Single_Table_Select ssel = (struct Single_Table_Select) {
		.table_name = inner_string_create("tab1"),
		.number_of_columns = -1,
		.column_names = NULL,
		.condition = &cond
	};
	union Select_Union su = (union Select_Union){
		.single_table_select = ssel
	};

	struct Select sel = (struct Select){
		.is_single_table_select = 1,
		.query_details = su
	};
	for (size_t i = 0; i < 10; i++)
	{
		clock_t begin = clock();
		for (size_t i = 0; i < 10000; i++)
		{
			int val = i % 10;
			struct Data_Row_Node rn0 = create_data_row_node("col1", INT, &val);
			ins.new_data = &rn0;
			process_insert(f_handle, ins);
		}
		clock_t end = clock();
		//printf("10000 elems insert time: %f\n", (double)(end - begin)/CLOCKS_PER_SEC);
		begin = clock();
		struct Table_Chain_Result_Set* rs = process_select_with_row_num(f_handle, sel, 500);
		while (rs->probably_has_next == 1) {
			rs = result_set_get_next(f_handle, rs);
		}
		free(rs);
		end = clock();
		//printf("select time: %f\n", (double)(end - begin) / CLOCKS_PER_SEC);
	}
	
	
}






//struct Joined_Table {
//	int32_t number_of_joined_tables;
//	struct String* table_names;
//	struct Join_Condition* join_conditions; // length = number_of_joined_tables - 1
//};
//
//
//struct Joined_Table_Select {
//	//int8_t all_columns;
//	struct Joined_Table joined_table;
//	uint32_t* number_of_columns_from_each_table;
//	struct String** column_names; // array with len == joined_tables_num
//	struct Condition** conditions; // array with len == joined_tables_num
//};




//void create_useless_table(struct File_Handle* f_handle) {
//	struct Table_Schema schema = table_schema_init();
//
//	table_schema_expand(&schema, "a", INT);
//	table_schema_expand(&schema, "b", FLOAT);
//
//	table_create(f_handle, "useless", schema);
//}
//
//
//void insert_into_useless_table(struct File_Handle* f_handle) {
//
//	int32_t a = 0;
//	struct Data_Row_Node rn0 = create_data_row_node("a", INT, &a);
//	float b = 1.6;
//	struct Data_Row_Node rn1 = create_data_row_node("b", FLOAT, &b);
//	rn0.next_node = &rn1;
//
//	struct Insert ins = (struct Insert){
//		.table_name = inner_string_create("useless"),
//		.new_data = &rn0
//	};
//
//	process_insert(f_handle, ins);
//
//	struct Single_Table_Select sts = (struct Single_Table_Select){
//		.column_names = NULL,
//		.condition = NULL,
//		.number_of_columns = -1,
//		.table_name = inner_string_create("useless")
//	};
//
//	union Select_Union su = (union Select_Union){
//		.single_table_select = &sts
//	};
//
//	struct Select sel = (struct Select){
//		.is_single_table_select = 1,
//		.query_details = su
//	};
//
//	struct Table_Chain_Result_Set* rs = process_select_with_row_num(f_handle, sel, 10);
//	print_joined_table_rows(rs);
//	free_table_chain_result_set_with_all_fields(rs);
//}
//
//void delete_useless_table(struct File_Handle* f_handle) {
//
//	table_delete(f_handle, "useless");
//
//}



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
//	rn3 = create_data_row_node("number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name3, &rn3);
//
//	g_number = 2;
//	rn3 = create_data_row_node("number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name3, &rn3);
//
//	g_number = 1;
//	rn3 = create_data_row_node("number", INT, &g_number);
//	insert_row(f_handle, hashed_table_name3, &rn3);
//
//
//	uint32_t num_of_cols = -1;
//	struct Condition* condit = NULL;
//	struct String* col_nams = NULL;
//	struct Table_Chain_Result_Set* jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
//		&condit, &num_of_cols, &col_nams, 5);
//	printf("As single select\n");
//	print_joined_table_rows(jrs);
//	delete_rows(f_handle, hashed_table_name2, NULL, 1);
//	jrs = table_chain_select(f_handle, 1, &hashed_table_name2, NULL,
//		&condit, &num_of_cols, &col_nams, 5);
//	print_joined_table_rows(jrs);
//	printf("%d rnum\n", delete_rows(f_handle, hashed_table_name1, NULL, 1));
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