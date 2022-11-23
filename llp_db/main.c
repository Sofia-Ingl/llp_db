#include <stdio.h>
#include <string.h>

#include "query_handler.h"
#include "tests.h"

int main(int argc, char* argv[]) {

	if (argc < 2) {

		printf("Wrong prog usage! Pls enter target db file name\n");
		printf("./app <filename> <norm>\n");

	}
	else {

		uint32_t normalize = 0;
		if (argc > 2) {
			if (strcmp(argv[2], "norm") == 0) {
				normalize = 1;
			}
			else {
				printf("Wrong prog usage! Undefined param\n");
			}
		}

		struct File_Handle* fh = file_open_or_create(argv[1]);

		//prepare_short_test_schema(fh);
		//test_select_on_short_test_schema(fh);

		//test_func2(fh);

		/*create_student_table(fh);
		create_group_table(fh);
		create_curriculum_table(fh);
		create_subject_table(fh);
		create_curriculum_subject_relation_table(fh);

		insert_into_student_table(fh);
		insert_into_group_table(fh);
		insert_into_curriculum_table(fh);
		insert_into_subject_table(fh);
		insert_into_curriculum_subject_relation_table(fh);*/

		file_close(fh, normalize);
	}
	


	//test_func1(fh);

	char c = getchar();
	return 0;
}