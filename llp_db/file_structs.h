#ifndef FILE_STRUCTS
#define FILE_STRUCTS

#include "common_structs.h"

#define DB_FILE_SIGNATURE 0x1917
#define DB_MAX_ROW_SIZE 4096
#define DB_MAX_TABLE_METADATA_SIZE 4096

//struct __attribute__((packed)) String_Metadata {
struct String_Metadata {
	uint32_t hash;
	uint16_t length;
};


//struct __attribute__((packed)) File_Header {
struct File_Header {
	uint16_t signature;
	uint16_t tables_number;
	//uint16_t gaps_number;
	uint32_t gap_sz;
	uint32_t first_gap_offset;
	uint32_t last_gap_offset;
	uint32_t first_table_offset;
	uint32_t last_table_offset;
};


/* | Table_Header | table_name_str | Column_Header 1| column_name_str_1 | ...*/
//struct __attribute__((packed)) Table_Header {
struct Table_Header {
	uint16_t columns_number;
	uint16_t table_metadata_size; // with column metadata
	uint32_t first_row_offset;
	uint32_t last_row_offset;
	uint32_t next_table_header_offset;
	uint32_t prev_table_header_offset;
	struct String_Metadata table_name_metadata;
};

//struct __attribute__((packed)) Column_Header {
struct Column_Header {
	enum DB_Data_Type data_type;
	struct String_Metadata column_name_metadata;
};

//struct __attribute__((packed)) Row_Header {
struct Row_Header {
	uint32_t next_row_header_offset;
	uint32_t prev_row_header_offset;
	uint16_t row_size;
};

//struct __attribute__((packed)) Gap_Header {
struct Gap_Header {
	uint32_t next_gap_header_offset;
	uint32_t prev_gap_header_offset;
	uint16_t gap_size;
};

#endif