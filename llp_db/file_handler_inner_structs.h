#ifndef FH_INNER_STRUCTS
#define FH_INNER_STRUCTS

#include "file_structs.h"
#include <stdio.h>

struct File_Handle {
	FILE* file;
};

struct Table_Handle {
	uint8_t exists;
	uint32_t table_metadata_offset;
	struct Table_Header table_header;
};

#endif