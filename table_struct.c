//
// Created by KuPuK on 20.02.2023.
//
#include "include/table_include.h"
int32_t column_length_varchar(const struct column* list, const size_t length, const char* name) {
    int32_t idx = 0;
    if (list) {
        while (idx < length) {
            if (strcmp(list[idx].name, name) == 0)
                return list[idx].size;
            idx++;
        }
        return -1;
    }
    else return -1;
}


void attribute_add(struct row* row, char* name, enum data_type content_type, void* value) {
    relation_read_columns(row->relation->relation_header->database->source_file, row->relation); //нужно для таблиц созданных до
    int32_t offset = column_get_offset(row->relation->schema->start, name, row->relation->schema->count);
    if (offset != -1) {
        switch (content_type) {
            case INTEGER:
                integer_add(row, *((int32_t *) value), offset);
                break;
            case BOOLEAN:
                boolean_add(row, *((bool *) value), offset);
                break;
            case VARCHAR:
                varchar_add(row, *((char **) value), offset,
                            column_length_varchar(row->relation->schema->start, row->relation->schema->count,
                                                  name));
                break;
            case DOUBLE:
                double_add(row, *((double *) value), offset);
                break;
        }
    } else {
        printf("Attribute does not exist: %s\n", name);
    }
}

int32_t column_get_offset(const struct column *list, char *name, const size_t length) {
    int32_t idx = 0;
    uint32_t offset = 0;
    if (list) {
        while (idx < length) {
            uint32_t bytes = list[idx].size;
            if (strcmp(list[idx].name, name) == 0) return offset;
            idx++;
            offset += bytes;
        }
        return -1;
    }
    else return -1;
}

struct row* row_create(struct table_struct* relation) {
    struct row* new = malloc(sizeof(struct row));
    struct row_header* row_header = malloc(sizeof(struct row_header));
    row_header->is_available = true;
    new->relation = relation;
    new->data = malloc(relation->schema->length);
    new->row_header = row_header;
    return new;
}

void row_close(struct row* row) {
    free(row->data);
    free(row->row_header);
    free(row);
}

void row_insert(struct row* row) {
    uint32_t page_number = row->relation->relation_header->page_number_last;
    enum file_status result = row_write_to_page(row->relation->relation_header->database->source_file,
                                                page_number, row);
    if (result != OK) {
        printf("Could not insert row");
    }
}

void row_select(struct query *query, bool show_output) {
    bool is_column_present = false;
    enum data_type data_type;
    char name[MAX_NAME_LENGTH];
    uint16_t size;

    for (size_t i = 0; i < query->relation->schema->count; i++) {
        if (strcmp(query->relation->schema->start[i].name, query->name[0]) == 0) {
            is_column_present = true;
            strncpy(name, query->relation->schema->start[i].name, MAX_NAME_LENGTH);
            size = query->relation->schema->start[i].size;
            data_type = query->relation->schema->start[i].data_type;
            break;
        }
    }
    if (is_column_present) {
        uint32_t offset = column_get_offset(query->relation->schema->start,
                                            query->name[0], query->relation->schema->count);
        select_execute(query->relation->relation_header->database->source_file, query->relation, offset, size,
                       query->value[0], data_type, query->number, show_output);
    } else printf("Attribute for this query does not exist\n");
}

void row_update(struct query *query, bool show_output) {
    bool is_column_present_one = false;
    bool is_column_present_two = false;
    char name_one[MAX_NAME_LENGTH];
    char name_two[MAX_NAME_LENGTH];
    enum data_type type_one;
    enum data_type type_two;
    uint16_t size_one = 0;
    uint16_t size_two = 0;

    for (size_t i = 0; i < query->relation->schema->count; i++) {
        if (strcmp(query->relation->schema->start[i].name, query->name[0]) == 0) {
            is_column_present_one = true;
            type_one = query->relation->schema->start[i].data_type;
            strncpy(name_one, query->relation->schema->start[i].name, MAX_NAME_LENGTH);
            size_one = query->relation->schema->start[i].size;
        } else if (strcmp(query->relation->schema->start[i].name, query->name[1]) == 0) {
            is_column_present_two = true;
            type_two = query->relation->schema->start[i].data_type;
            strncpy(name_two, query->relation->schema->start[i].name, MAX_NAME_LENGTH);
            size_two = query->relation->schema->start[i].size;
        }
        if (is_column_present_one && is_column_present_two) {
            break;
        }
    }

    if (is_column_present_one && is_column_present_two) {
        uint32_t offset_one = column_get_offset(query->relation->schema->start,
                                                name_one, query->relation->schema->count);
        uint32_t offset_two = column_get_offset(query->relation->schema->start,
                                                name_two, query->relation->schema->count);
        struct query_params* query_one = malloc(sizeof(struct query_params));
        struct query_params* query_two = malloc(sizeof(struct query_params));

        query_one->size = size_one;
        query_one->data_type = type_one;
        query_one->offset = offset_one;

        strncpy(query_one->name, "", MAX_NAME_LENGTH);
        strncpy(query_one->name, name_one, MAX_NAME_LENGTH);

        query_two->size = size_two;
        query_two->data_type = type_two;
        query_two->offset = offset_two;

        strncpy(query_two->name, "", MAX_NAME_LENGTH);
        strncpy(query_two->name, name_two, MAX_NAME_LENGTH);

        update_execute(query->relation->relation_header->database->source_file, query->relation, query_one,
                       query_two, query->value, show_output);

        free(query_one);
        free(query_two);

    } else printf("Attribute is not present\n");
}

void row_delete(struct query *query, bool show_output) {
    bool is_column_present = false;
    enum data_type data;
    char name[MAX_NAME_LENGTH];
    uint16_t column_size;

    for (size_t i = 0; i < query->relation->schema->count; i++) {
        if (strcmp(query->relation->schema->start[i].name, query->name[0]) == 0) {
            is_column_present = true;
            column_size = query->relation->schema->start[i].size;
            data = query->relation->schema->start[i].data_type;
            strncpy(name, query->relation->schema->start[i].name, MAX_NAME_LENGTH);
            break;
        }
    }

    if (is_column_present) {
        struct query_params* query_params = malloc(sizeof(struct query_params));

        uint32_t offset = column_get_offset(query->relation->schema->start, name, query->relation->schema->count);

        query_params->size = column_size;
        query_params->data_type = data;
        query_params->offset = offset;

        strncpy(query_params->name, "", MAX_NAME_LENGTH);
        strncpy(query_params->name, name, MAX_NAME_LENGTH);

        delete_execute(query->relation->relation_header->database->source_file, query->relation, query_params, query->value[0]);

        free(query_params);
    } else printf("Attribute is not present\n");
}

void integer_add(struct row* row, int32_t value, uint32_t offset) {
    char* ptr = (char*) row->data + offset;
    *((int32_t*) (ptr)) = value;
}

void boolean_add(struct row* row, bool value, uint32_t offset) {
    char* ptr = (char*) row->data + offset;
    *((bool*) (ptr)) = value;
}

void double_add(struct row* row, double value, uint32_t offset) {
    char* ptr = (char*) row->data + offset;
    *((double*) (ptr)) = value;
}

void varchar_add(struct row* row, char* value, uint32_t offset, uint32_t length) {
    if (length != -1) {
        char* ptr = (char*) row->data + offset;
        strncpy(ptr, "", length);
        strncpy(ptr, value, strlen(value));
    }
}

