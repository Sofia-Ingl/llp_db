#ifndef DB_TESTS
#define DB_TESTS

#include "query_handler.h"

void prepare_short_test_schema(struct File_Handle* fh);

void test_select_on_short_test_schema(struct File_Handle* fh);

void asymptotics_testing(struct File_Handle* f_handle);

void size_testing(struct File_Handle* f_handle);

#endif