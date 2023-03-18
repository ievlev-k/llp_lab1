//
// Created by KuPuK on 20.02.2023.
//

#ifndef LLP1_FILE_H
#define LLP1_FILE_H

#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>
#include "database_include.h"
#include "data_types.h"
enum data_type;
struct database_header;
struct column;
struct table_struct;
struct query_params;

struct table_header;
struct row;
struct page_header;
struct schema;

enum file_status  {
    OK = 0,
    ERROR
};

bool is_relation_present(FILE *file, const size_t length, const char* name, struct table_header* relation_header);

enum file_status file_update_last_page(FILE *file, uint32_t old_number, uint32_t new);
enum file_status file_open(FILE **file, const char *const name, const char *const mode);
enum file_status file_close(FILE *file);

enum file_status page_header_write_real(FILE *file, struct database_header* database_header, struct table_header* relation_header);

enum file_status relation_header_save_changes(FILE *file, struct table_header* relation_header);
enum file_status relation_header_read(FILE *file, const char *const  relation_name, struct table_header* relation_header, size_t relation_count);

enum file_status relation_page_write(FILE *file, struct page_header* page_header, struct schema* schema);

enum file_status relation_read_columns(FILE *file, struct table_struct* relation);

enum file_status row_write_to_page(FILE *file, uint32_t page_number, struct row* row);

enum file_status database_update_last_page(FILE *file, struct database_header* database_header, uint32_t new);
enum file_status database_header_save_changes(FILE *file, struct database_header* database_header);
enum file_status database_header_read(FILE *file, struct database_header* database_header);
enum file_status database_save_to_file(FILE *file, struct database_header* database_header, struct page_header* page_header_real);

void row_remove(struct table_struct *relation, uint32_t ptr, uint32_t page_number);

void select_execute(FILE *file, struct table_struct *relation, uint32_t offset, uint16_t column_size, void *value,
                    enum data_type type, int32_t row_count, bool show_output);
void update_execute(FILE *file, struct table_struct *relation, struct query_params *first, struct query_params *second,
                    void **values, bool show_output);
void delete_execute(FILE *file, struct table_struct* relation, struct query_params* query, void* value);

void update_query(char *begin, void *value, struct query_params *query, struct table_struct *relation, uint32_t ptr,
                  uint32_t page_number, bool show_output);



long int database_get_size(FILE* file);
uint32_t query_join_attempt(FILE *file, struct table_struct* left, struct table_struct* right, struct query_params* left_query, struct query_params* right_query, char* left_row);
void query_join(FILE *file, struct table_struct* left, struct table_struct* right, struct query_params* left_query, struct query_params* right_query);

void query_join_output(char* begin_left, char* begin_right, struct table_struct* left, struct table_struct* right, uint32_t left_offset, uint32_t right_offset);

#endif //LLP1_FILE_H
