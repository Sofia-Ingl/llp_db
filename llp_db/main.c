#include <stdio.h>

#include "query_handler.h"

int main() {
	struct File_Handle* fh = file_open_or_create("hell2");

	struct Table_Schema schema = table_schema_init();
	table_schema_expand(&schema, "col1", STRING);
	table_schema_expand(&schema, "col2", STRING);
	table_create(fh, "tab11", schema);

	table_create(fh, "tab22", schema);

	table_create(fh, "tab22", schema);

	table_create(fh, "tab33", schema);

	//table_delete(fh, "tab11");
	test_func(fh);
	//table_delete(fh, "tab22");
	//test_func(fh);
	char c = getchar();
	return 0;
}