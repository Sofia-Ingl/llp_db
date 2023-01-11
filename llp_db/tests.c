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
	char* jval1 = "number";
	char* jval2 = "group_number";
	join_conditions[0].current_table_column_name = inner_string_create(jval1);
	join_conditions[0].related_table_column_name = inner_string_create(jval2);
	join_conditions[0].related_table_index = 0;
	
	//join curriculum on curriculum.code = group.curriculum_code
	char* jval3 = "code";
	char* jval4 = "curriculum_code";
	join_conditions[1].current_table_column_name = inner_string_create(jval3);
	join_conditions[1].related_table_column_name = inner_string_create(jval4);
	join_conditions[1].related_table_index = 1;

	//join cur_sub_relation on curriculum.code = cur_sub_relation.curriculum_code
	join_conditions[2].current_table_column_name = inner_string_create(jval4);
	join_conditions[2].related_table_column_name = inner_string_create(jval3);
	join_conditions[2].related_table_index = 2;

	//join subject on subject.code = cur_sub_relation.subject_code
	char* jval5 = "subject_code";
	join_conditions[3].current_table_column_name = inner_string_create(jval3);
	join_conditions[3].related_table_column_name = inner_string_create(jval5);
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
	char* cval1 = "code";
	struct Condition cond_5 = create_simple_condition(cval1, val5, EQUALS);
	conditions[4] = &cond_5;

	//and (student.actual == TRUE)
	struct Schema_Internals_Value val1;
	val1.data_type = BOOL;
	val1.value.db_boolean = TRUE;
	char* cval2 = "actual";
	struct Condition cond_1 = create_simple_condition(cval2, val1, EQUALS);
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
	while (rs != NULL) {
		print_joined_table_rows(rs);
		rs = result_set_get_next(fh, rs);
	}

}


void process_student_inserts_from_text_format() {

}




//void create_int_tables(struct File_Handle* f_handle) {
//	struct Table_Schema schema = table_schema_init();
//
//	table_schema_expand(&schema, "col1", INT);
//
//	table_create(f_handle, "tab1", schema);
//
//	table_schema_expand(&schema, "col1", INT);
//
//	table_create(f_handle, "tab2", schema);
//}


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

	struct Table_Schema schema = table_schema_init();

	table_schema_expand(&schema, "col1", INT);

	table_create(f_handle, "tab1", schema);

	table_schema_expand(&schema, "col1", INT);

	table_create(f_handle, "tab2", schema);

	/*INSERTS*/

	struct Insert ins = (struct Insert){
			.table_name = inner_string_create("tab1"),
			.new_data = NULL
	};

	struct Insert ins2 = (struct Insert){
			.table_name = inner_string_create("tab2"),
			.new_data = NULL
	};

	/*SINGLE TABLE SELECT*/

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

	/*DELETE*/

	struct Schema_Internals_Value del_val;
	del_val.data_type = INT;
	del_val.value.db_integer = 2;
	struct Condition del_cond = create_simple_condition("col1", del_val, EQUALS);
	struct Delete del = (struct Delete) {
		.table_name = inner_string_create("tab1"),
		.condition = &del_cond
	};

	/*UPDATE*/

	struct Schema_Internals_Value upd_val;
	upd_val.data_type = INT;
	upd_val.value.db_integer = 3;
	struct Condition upd_cond = create_simple_condition("col1", upd_val, EQUALS);
	int32_t nv = 5;
	struct Data_Row_Node new_val = create_data_row_node("col1", INT, &nv);
	struct Update upd = (struct Update){
		.table_name = inner_string_create("tab1"),
		.new_data = &new_val,
		.condition = &upd_cond
	};

	/*JOIN SELECT*/

	struct String table_names[2];
	char* str1 = "tab1";
	char* str2 = "tab2";
	table_names[0] = inner_string_create(str1);
	table_names[1] = inner_string_create(str2);
	

	struct Join_Condition join_conditions[1];
	char* str3 = "col1";
	join_conditions[0].current_table_column_name = inner_string_create(str3);
	join_conditions[0].related_table_column_name = inner_string_create(str3);
	join_conditions[0].related_table_index = 0;

	struct Joined_Table jt = (struct Joined_Table){
		.number_of_joined_tables = 2,
		.table_names = table_names,
		.join_conditions = join_conditions
	};


	uint32_t number_of_columns_from_each_table[2];
	struct String* column_names[2];
	struct Condition* conditions[2];

	for (size_t i = 0; i < 2; i++)
	{
		number_of_columns_from_each_table[i] = -1; // all columns
		column_names[i] = NULL;
		conditions[i] = NULL;
	}


	struct Joined_Table_Select jts = (struct Joined_Table_Select){
		.joined_table = jt,
		.column_names = column_names,
		.conditions = conditions,
		.number_of_columns_from_each_table = number_of_columns_from_each_table
	};

	union Select_Union join_su = (union Select_Union){
		.joined_table_select = jts
	};

	struct Select join_sel = (struct Select){
		.is_single_table_select = 0,
		.query_details = join_su
	};

	/*TEST CYCLE*/

	double insert_time[10];
	double single_table_select_time[10];
	double multiple2_table_select_time[10];
	double delete_time[10];
	double update_time[10];

	printf("running tests...\n");

	for (size_t i = 0; i < 10; i++)
	{
		/*INSERT*/
		clock_t begin = clock();
		for (size_t j = 0; j < 10000; j++)
		{
			int val = j % 10;
			struct Data_Row_Node rn0 = create_data_row_node("col1", INT, &val);
			ins.new_data = &rn0;
			process_insert(f_handle, ins);
		}
		clock_t end = clock();
		insert_time[i] = (double)(end - begin) / CLOCKS_PER_SEC;

		/*SINGLE TAB SELECT*/
		begin = clock();
		struct Table_Chain_Result_Set* rs = process_select_with_row_num(f_handle, sel, 500);
		
		while (rs != NULL) {
				
			rs = result_set_get_next(f_handle, rs);
			
		}
		end = clock();
		single_table_select_time[i] = (double)(end - begin) / CLOCKS_PER_SEC;

		/*DELETE*/
		begin = clock();
		int del_num = process_delete(f_handle, del, 0);
		end = clock();
		delete_time[i] = (double)(end - begin) / CLOCKS_PER_SEC;

		/*UPDATE*/
		begin = clock();
		int upd_num = process_update(f_handle, upd, 0);
		end = clock();
		update_time[i] = (double)(end - begin) / CLOCKS_PER_SEC;
		
		/*JOIN TAB SELECT*/
		for (size_t j = 0; j < 10; j++)
		{
			int val = j % 10;
			struct Data_Row_Node rn0 = create_data_row_node("col1", INT, &val);
			ins2.new_data = &rn0;
			process_insert(f_handle, ins2);
		}

		begin = clock();
		struct Table_Chain_Result_Set* join_rs = process_select_with_row_num(f_handle, join_sel, 500);
		while (join_rs != NULL) {

			join_rs = result_set_get_next(f_handle, join_rs);

		}
		end = clock();
		multiple2_table_select_time[i] = (double)(end - begin) / CLOCKS_PER_SEC;

	}
	
	printf("Inserts\n");
	for (size_t i = 0; i < 10; i++)
	{
		printf("%f\n", insert_time[i]);
	}
	printf("Single table selects\n");
	for (size_t i = 0; i < 10; i++)
	{
		printf("%f\n", single_table_select_time[i]);
	}
	printf("Delets\n");
	for (size_t i = 0; i < 10; i++)
	{
		printf("%f\n", delete_time[i]);
	}
	printf("Updates\n");
	for (size_t i = 0; i < 10; i++)
	{
		printf("%f\n", update_time[i]);
	}
	printf("Join table selects\n");
	for (size_t i = 0; i < 10; i++)
	{
		printf("%f\n", multiple2_table_select_time[i]);
	}
}


