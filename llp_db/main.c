#include <stdio.h>

#include "query_handler.h"

int main() {
	struct File_Handle* fh = file_open_or_create("hell2");


	//test_func1(fh);

	char c = getchar();
	return 0;
}