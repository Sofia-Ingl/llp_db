#ifndef CONDITION_FILTERS
#define CONDITION_FILTERS

#include "common_structs.h"

#include <stdint.h>
#include <string.h>
#include <stdio.h>



uint8_t apply_filter(void* table_metadata_buffer, void* row_buffer, struct Condition* condition);

#endif