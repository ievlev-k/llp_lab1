//
// Created by KuPuK on 14.03.2023.
//

#ifndef LLP1_QUERY_H
#define LLP1_QUERY_H
#include <inttypes.h>
#include "table_struct.h"
enum query_types {
    SELECT = 0,
    UPDATE,
    DELETE,
};

struct query {
    char** name;
    enum query_types operation;
    int32_t number;
    void** value;
    struct table_struct* relation;
};

struct query_join {
    struct table_struct* left;
    struct table_struct* right;
    char* left_column;
    char* right_column;
};

#endif //LLP1_QUERY_H
