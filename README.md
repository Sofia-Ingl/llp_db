# llp_db

## Стуктуры данных

### Представление данных

Примитивные элементы данных:

```
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

union Data {
	struct String db_string;
	enum Boolean db_boolean;
	int32_t db_integer;
	float db_float;
};

struct Schema_Internals_Value {
	enum DB_Data_Type data_type;
	union Data value;
};

struct Data_Row_Node {
	struct String column_name;
	struct Schema_Internals_Value value;
	struct Data_Row_Node* next_node;
};
```

Условия:

```
enum Condition_Relation {
	EQUALS,
	LESS,
	BIGGER,
	NOT_EQUALS
};

enum Condition_Chain_Relation {
	AND,
	OR
};

struct Simple_Condition {
	struct String column_name;
	enum Condition_Relation relation;
	struct Schema_Internals_Value right_part;
};

struct Condition;

struct Complex_Condition {
	struct Condition* left;
	struct Condition* right;
	enum Condition_Chain_Relation relation;
};

union Condition_Union {
	struct Complex_Condition complex_condition;
	struct Simple_Condition simple_condition;
};

struct Condition {
	uint8_t is_simple;
	union Condition_Union condition;
};
```

Склеенные таблицы для селект запросов:

```
struct Join_Condition {
	uint32_t related_table_index;
	struct String related_table_column_name;
	struct String current_table_column_name;
};

struct Joined_Table {
	int32_t number_of_joined_tables;
	struct String* table_names;
	struct Join_Condition* join_conditions; // length = number_of_joined_tables - 1
};
```

### Запросы

Структуры и функции для работы с файлом:

```
struct File_Handle {
	FILE* file;
	char* filename;
	float critical_gap_rate;
	uint32_t critical_gap_sz;
};

struct File_Handle* file_open_or_create(char* filename);
struct File_Handle* file_open_or_create_with_gap_rate(char* filename, float critical_gap_rate);
struct File_Handle* file_open_or_create_with_gap_sz(char* filename, uint32_t critical_gap_sz);
struct File_Handle* file_open_or_create_with_gap_rate_and_sz(char* filename, float critical_gap_rate, uint32_t critical_gap_sz);
void file_close(struct File_Handle* f_handle, uint8_t normalize);
```

Структуры и функции для работы со схемой данных:

```
struct Table_Schema {
	uint32_t number_of_columns;
	uint32_t column_array_size; // to calc if there is free space or not
	struct Column_Info_Block* column_info;
};

struct Table_Schema table_schema_init();
int8_t table_schema_expand(struct Table_Schema* schema, char* column_name, enum DB_Data_Type data_type);
int8_t table_create(struct File_Handle* f_handle, char* table_name, struct Table_Schema schema);
int8_t table_delete(struct File_Handle* f_handle, char* table_name, enum Normalization normalization);
```

Структуры и функции запросов, работающие с элементами данных:

```
struct Joined_Table_Select {
	struct Joined_Table joined_table;
	uint32_t* number_of_columns_from_each_table;
	struct String** column_names; 
	struct Condition** conditions; 
};

struct Single_Table_Select {
	struct String table_name;
	uint32_t number_of_columns; 
	struct String* column_names;
	struct Condition* condition;
};

union Select_Union {
	struct Single_Table_Select single_table_select;
	struct Joined_Table_Select joined_table_select;
};

struct Select {
	uint8_t is_single_table_select;
	union Select_Union query_details;
};

struct Update {
	struct String table_name;
	struct Data_Row_Node* new_data;
	struct Condition* condition;
};

struct Delete {
	struct String table_name;
	struct Condition* condition;
};

struct Insert {
	struct String table_name;
	struct Data_Row_Node* new_data; //one row
};

int32_t process_insert(struct File_Handle* f_handle, struct Insert insert_command);
int32_t process_update(struct File_Handle* f_handle, struct Update update_command, enum Normalization normalization);
int32_t process_delete(struct File_Handle* f_handle, struct Delete delete_command, enum Normalization normalization);
struct Table_Chain_Result_Set* process_select_with_row_num(struct File_Handle* f_handle, struct Select select_command, uint32_t max_row_num);
struct Table_Chain_Result_Set* process_select(struct File_Handle* f_handle, struct Select select_command);
struct Table_Chain_Result_Set* result_set_get_next(struct File_Handle* f_handle, struct Table_Chain_Result_Set* result_set);

```

Результат селекта:

```
struct Table_Chain_Result_Set {

	uint32_t rows_num; 
	uint8_t probably_has_next;
	int32_t number_of_joined_tables;
	struct String* table_names;
	struct Join_Condition* join_conditions;
	uint32_t* cursor_offsets;
	struct Condition** conditions_on_single_tables;
	struct Table_Handle* tab_handles;
	void** table_metadata_buffers;
	struct Table_Row_Lists_Bunch* rows_chain;
	int32_t* number_of_selected_columns; 
	struct String** column_names;

};
```

Узел древовидной структуры, в которой хранятся результаты селекта:

```
struct Table_Row_Lists_Bunch {
	uint32_t local_rows_num;
	void* row_lists_buffer;
	uint32_t* row_starts_in_buffer;
	struct Table_Row_Lists_Bunch** row_tails;
};
```

## Результаты тестирования

### Производительность

Insert
  
![Insert](https://github.com/Sofia-Ingl/llp_db/blob/master/graphs/insert.jpg)

Update

![Update](https://github.com/Sofia-Ingl/llp_db/blob/master/graphs/update.jpg)

Delete

![Delete](https://github.com/Sofia-Ingl/llp_db/blob/master/graphs/delete.jpg)

Single table select
 
![Select](https://github.com/Sofia-Ingl/llp_db/blob/master/graphs/single_tab_select.jpg)

Join select
 
![Select](https://github.com/Sofia-Ingl/llp_db/blob/master/graphs/join_select1.jpg)
![Select](https://github.com/Sofia-Ingl/llp_db/blob/master/graphs/join_select2.jpg)

### Размер файла 
Сначала десятикратное добавление 10тыс записей, затем удаление 10тыс на каждой итерации с помощью подбора специфических условий.

![File size](https://github.com/Sofia-Ingl/llp_db/blob/master/graphs/file_sz.jpg)
