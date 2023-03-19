
#ifndef LLP1_DATABASE_H
#define LLP1_DATABASE_H
#include <inttypes.h>
#include <stdio.h>
#include <stdbool.h>





#define MAX_DATABASE_NAME_LENGTH 50
#define DEFAULT_PAGE_SIZE_BYTES 4096
#define MAX_TABLE_NAME_LENGTH 20

//struct schema;
//struct table_header;

//enum query_types;

enum database_state {
    SAVED_IN_FILE = 0,
    NEW
};

struct page_header {
    bool is_dirty;
    char table_name[MAX_TABLE_NAME_LENGTH];
    uint16_t remaining_space;
    uint32_t page_number;
    uint32_t write_ptr;
    uint32_t real_number;
    uint32_t next_page_number;
};

struct database_header {
    char name[MAX_DATABASE_NAME_LENGTH];
    struct database* database;
    uint32_t table_count;
    uint32_t page_count;
    uint32_t page_size;
    uint32_t last_page_number;
};

struct database {
    struct database_header* database_header;
    FILE* source_file;
};

#endif //LLP1_DATABASE_H
