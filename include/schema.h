//
// Created by KuPuK on 20.02.2023.
//

#ifndef LLP1_SCHEMA_H
#define LLP1_SCHEMA_H

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>

#include "../struct/table_struct.h"
#include "../struct/query.h"
#include "file.h"



#define MAX_NAME_LENGTH 20

struct query;


struct column* column_create(const char* name, enum data_type content_type);
struct column* column_create_varchar(const char* name, enum data_type content_type, uint16_t size);

void column_list_delete(struct column* list);
void column_add(struct schema* schema, const char* name, enum data_type content_type);
void column_add_varchar(struct schema* schema, const char* name, enum data_type content_type, uint16_t size);

int32_t column_length_varchar(const struct column* list, const size_t length, const char* name);



struct schema* schema_create();
struct schema* schema_add_column(struct schema* schema, const char* name, enum data_type content_type);
struct schema* schema_add_column_varchar(struct schema* schema, const char* name, enum data_type content_type, uint16_t size);

void schema_close(struct schema* schema);




#endif //LLP1_RELATION_H
