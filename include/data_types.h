//
// Created by KuPuK on 18.03.2023.
//

#ifndef LLP1_DATA_TYPES_H
#define LLP1_DATA_TYPES_H


#include <stdbool.h>
#include <inttypes.h>
#include "../struct/database_struct.h"
#include "../struct/table_struct.h"
#include "file.h"

void integer_update(char* row_ptr, void* value, uint32_t offset);
void boolean_update(char* row_ptr, void* value, uint32_t offset);
void varchar_update(char* row_ptr, void* value, uint32_t offset, uint16_t column_size);
void double_update(char* row_ptr, void* value, uint32_t offset);

void integer_output(char* begin, uint32_t offset);
void boolean_output(char* begin, uint32_t offset);
void varchar_output(char* begin, uint32_t offset);
void double_output(char* begin, uint32_t offset);
void data_output(char* begin, struct column* columns, uint16_t length);

bool integer_compare(char* row_ptr, void* value, uint32_t offset);
bool boolean_compare(char* row_ptr, void* value, uint32_t offset);
bool varchar_compare(char* row_ptr, void* value, uint32_t offset, uint16_t column_size);
bool double_compare(char* row_ptr, void* value, uint32_t offset);


#endif //LLP1_DATA_TYPES_H
