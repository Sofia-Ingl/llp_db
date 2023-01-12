#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "query_handler.h"
#include "tests.h"

int main(int argc, char* argv[]) {

	char* error_msg = "Wrong prog usage! Use this pattern:\n ./app <filename> <-n> <-s gap_sz> <-r gap_rate>\n";
	
	if (argc < 2) {

		printf("%s", error_msg);

	}
	else {

		char* filename = argv[1];
		uint32_t normalize = 0;
		int32_t gap_sz = -1;
		float gap_rate = 0;
		uint8_t gap_set = 0;

		if (argc > 2) {
			for (size_t i = 2; i < argc; i++)
			{
				if (strcmp(argv[i], "-n") == 0) {
					normalize = 1;
				}
				if (strcmp(argv[i], "-s") == 0) {
					
					if ((i + 1) >= argc) {
						printf("%s", error_msg);
						printf("Gap sz will stay default\n");
					}
					else {
						gap_sz = atoi(argv[i + 1]);
					}
				}

				if (strcmp(argv[i], "-r") == 0) {
					if ((i + 1) >= argc) {
						printf("%s", error_msg);
						printf("Gap rate will stay default\n");
					}
					else {
						gap_rate = atof(argv[i + 1]);
						gap_set = 1;
					}
				}
			}
			
		}

		

		struct File_Handle* fh;
		if (gap_set) {
			if (gap_sz != -1) {
				fh = file_open_or_create_with_gap_rate_and_sz(filename, gap_rate, gap_sz);
			}
			else {
				fh = file_open_or_create_with_gap_rate(filename, gap_rate);
			}
		}
		else {
			if (gap_sz != -1) {
				fh = file_open_or_create_with_gap_sz(filename, gap_sz);
			}
			else {
				fh = file_open_or_create(filename);
			}
		}


		prepare_short_test_schema(fh);
		test_select_on_short_test_schema(fh);
		file_close(fh, normalize);

		char* asymp_filename = "asymp";
		struct File_Handle* asymp_fh = file_open_or_create(asymp_filename);
		asymptotics_testing(asymp_fh);
		file_close(asymp_fh, 0);

		char* sizetest_filename = "sizetest";
		struct File_Handle* sizetest_fh = file_open_or_create_with_gap_rate_and_sz(sizetest_filename, 0.05, 2048);
		size_testing(fh);
		file_close(sizetest_fh, 0);

		

		
		
	}

	return 0;
}