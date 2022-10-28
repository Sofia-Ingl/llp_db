#ifndef COMMON_STRUCTS
#define COMMON_STRUCTS

#include <stdint.h>

struct String {
	uint32_t hash;
	uint16_t length;
	char* value;
};

enum DB_Data_Type {
	INT,
	FLOAT,
	BOOL,
	STRING
};

enum Boolean {
	FALSE,
	TRUE
};

#endif