#ifndef DB_TESTS
#define DB_TESTS

#include "query_handler.h"

void prepare_short_test_schema(struct File_Handle* fh);

void test_select_on_short_test_schema(struct File_Handle* fh);

void asymptotics_testing(struct File_Handle* f_handle);

void size_testing(struct File_Handle* f_handle);

//void test_func2(struct File_Handle* f_handle);

//void create_student_table(struct File_Handle* f_handle);
//
//void insert_into_student_table(struct File_Handle* f_handle);
//
//void create_group_table(struct File_Handle* f_handle);
//
//void insert_into_group_table(struct File_Handle* f_handle);
//
//void create_curriculum_table(struct File_Handle* f_handle);
//
//void insert_into_curriculum_table(struct File_Handle* f_handle);
//
//void create_subject_table(struct File_Handle* f_handle);
//
//void insert_into_subject_table(struct File_Handle* f_handle);
//
//void create_curriculum_subject_relation_table(struct File_Handle* f_handle);
//
//void insert_into_curriculum_subject_relation_table(struct File_Handle* f_handle);

#endif