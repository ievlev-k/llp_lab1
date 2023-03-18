#ifndef LLP1_INCLUDE_DATABASE_H
#define LLP1_INCLUDE_DATABASE_H

#include <inttypes.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdbool.h>
#include "table_include.h"
#include "schema.h"


#include "../struct/database_struct.h"
#include "../struct/query.h"


#include "file.h"

#define MAX_RELATION_NAME_LENGTH 20

struct schema;
struct table_header;

enum query_types;

bool is_enough_space(struct page_header* page_header, uint32_t required);


bool is_enough_space(struct page_header* page_header, uint32_t required);

struct page_header* page_create(struct database_header* database_header, struct table_header* relation_header);
struct page_header* page_add_real(struct database_header* database_header);
struct page_header* page_add(struct table_header* relation_header, struct database_header* database_header);

struct database* db_get(const char *const file, const enum database_state state);
struct database* db_create_in_file(const char *const file);
struct database* db_get_from_file(const char *const file);

void db_close(struct database* database);

struct table_struct* table_create(struct schema* schema, const char* relation_name, struct database*  database);
struct table_struct* table_get(const char *const relation_name, struct database*  database);

void relation_close(struct table_struct* relation);

struct query* query_make(enum query_types operation, struct table_struct* relations, char* columns[], void* vals[], int32_t cnt);
struct query_join* query_join_make(struct table_struct* left, struct table_struct* right, char* left_column, char* right_column);

void query_execute(struct query *query, bool show_output);
void query_join_execute(struct query_join* query);

void query_close(struct query* query);
void query_join_close(struct query_join* query);

#endif