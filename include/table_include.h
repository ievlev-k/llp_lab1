//
// Created by KuPuK on 18.03.2023.
//


#ifndef LLP1_TABLE_INCLUDE_H
#define LLP1_TABLE_INCLUDE_H

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>

#include "../struct/table_struct.h"
#include "../struct/query.h"
#include "file.h"

#define MAX_NAME_LENGTH 20


void attribute_add(struct row* row, char* name, enum data_type content_type, void* value);

int32_t column_get_offset(const struct column *list, char *name, const size_t length);

struct row* row_create(struct table_struct* relation);


void row_close(struct row* row);
void row_insert(struct row* row);
void row_select(struct query *query, bool show_output);
void row_update(struct query *query, bool show_output);
void row_delete(struct query *query, bool show_output);


void integer_add(struct row* row, int32_t value, uint32_t offset);
void boolean_add(struct row* row, bool value, uint32_t offset);
void varchar_add(struct row* row, char* value, uint32_t offset, uint32_t length);
void double_add(struct row* row, double value, uint32_t offset);
#endif //LLP1_TABLE_INCLUDE_H
