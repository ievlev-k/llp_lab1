
#include "include/data_types.h"

bool integer_compare(char* row_ptr, void* value, uint32_t offset) {
    int32_t comparing = (int32_t) *(row_ptr + offset);
    int32_t expecting = *((int32_t*) value);
    if (comparing == expecting) {
        return true;
    }
    return false;
}

bool boolean_compare(char* row_ptr, void* value, uint32_t offset) {
    bool comparing = (bool) *(row_ptr + offset);
    bool expecting = *((bool*) value);
    if (comparing == expecting) {
        return true;
    }
    return false;
}


bool double_compare(char* row_ptr, void* value, uint32_t offset) {
    double* comparing = (double*) (row_ptr + offset);
    double expecting = *((double*) value);
    if (*comparing == expecting) {
        return true;
    }
    return false;
}

bool varchar_compare(char* row_ptr, void* value, uint32_t offset, uint16_t column_size) {
    char* comparing = (char*) (row_ptr + offset);
    char* expecting = *((char**) value);
    if (strcmp(comparing, expecting) == 0) {
        return true;
    }
    return false;
}

void integer_update(char* row_ptr, void* value, uint32_t offset) {
    int32_t* original = (int32_t*) (row_ptr + offset);
    int32_t expecting = *((int32_t*) value);
    *original = expecting;
}

void boolean_update(char* row_ptr, void* value, uint32_t offset) {
    bool* original = (bool*) (row_ptr + offset);
    bool expecting = *((bool*) value);
    *original = expecting;
}

void double_update(char* row_ptr, void* value, uint32_t offset) {
    double* original = (double*) (row_ptr + offset);
    double expecting = *((double*) value);
    *original = expecting;
}

void varchar_update(char* row_ptr, void* value, uint32_t offset, uint16_t column_size) {
    char* original = (char*) row_ptr + offset;
    char* expecting = *((char**) value);
    strcpy(original, expecting);
}

void integer_output(char* begin, uint32_t offset) {
    int32_t* value = (int32_t*) (begin + offset);
    printf("%" PRId32 " ", *value);
}

void boolean_output(char* begin, uint32_t offset) {
    bool* value = (bool*) (begin + offset);
    printf("%s ", *value ? "T" : "F");
}

void double_output(char* begin, uint32_t offset) {
    double* value = (double*) (begin + offset);
    printf("%f ", *value);
}

void varchar_output(char* begin, uint32_t offset) {
    char* value = (char*) (begin + offset);
    printf("%s ", value);
}

void data_output(char* begin, struct column* columns, uint16_t length) {
    uint16_t cursor = 0;

    for (size_t i = 0; i < length; i++) {
        switch (columns[i].data_type) {
            case INTEGER:
                integer_output(begin, cursor);
                break;
            case BOOLEAN:
                boolean_output(begin, cursor);
                break;
            case VARCHAR:
                varchar_output(begin, cursor);
                break;
            case DOUBLE:
                double_output(begin, cursor);
                break;
        }
        cursor += columns[i].size;
    }

    printf("\n");
}

