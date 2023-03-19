
#ifndef LLP1_TABLE_H
#define LLP1_TABLE_H
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>

#include "database.h"

enum data_type {
    INTEGER = 0,
    BOOLEAN,
    DOUBLE,
    VARCHAR
};

struct column {
    char name[20];
    enum data_type data_type;
    struct column* next;
    uint32_t size;
};

struct query_params {
    char name[20];
    enum data_type data_type;
    uint16_t size;
    uint32_t offset;
};


struct schema {
    struct column* start;
    struct column* end;
    uint64_t length;
    uint16_t count;
};

struct table_header {
    bool is_available;

    char name[20];

    struct database* database;
    struct table* table;
    struct schema schema;

    uint32_t page_number_first;
    uint32_t page_number_last;
    uint32_t page_count;
    uint32_t real_number;
};

struct table {
    struct table_header* table_header;
    struct schema* schema;
};

struct row_header {
    bool is_available;
};

struct row {
    struct row_header* row_header;
    struct table* table;
    void** data;
};


#endif //LLP1_TABLE_H
