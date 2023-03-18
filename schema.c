
#include "include/schema.h"

struct schema* schema_create() {
    struct schema* new = malloc(sizeof(struct schema));
    new->count = 0;
    new->length = 0;
    new->start = NULL;
    new->end = NULL;
    return new;
}


void column_add(struct schema* schema, const char* name, enum data_type content_type) {
    struct column* new = column_create(name, content_type);
    if (schema->end)
        schema->end->next = new;
    else schema->start = new;
    schema->end = new;
}

void column_add_varchar(struct schema* schema, const char* name, enum data_type content_type, uint16_t size) {
    struct column* new = column_create_varchar(name, content_type, size);
    if (schema->end)
        schema->end->next = new;
    else schema->start = new;
    schema->end = new;
}




void column_list_delete(struct column* list) {
    struct column* current = list;
    struct column* next;
    while (current) {
        next = current->next;
        free(current);
        current = next;
    }
}

void schema_close(struct schema* schema) {
    column_list_delete(schema->start);
    free(schema);
}

uint32_t column_presence_count(const struct column* list, const size_t length, const char* name) {
    int32_t idx = 0;
    const struct column* current = list;
    if (list) {
        while (idx < length) {
            if (strcmp(current->name, name) == 0) return current->size;
            idx++;
            current=current->next;
        }
        return 0;
    }
    else return 0;
}


struct schema* schema_add_column(struct schema* schema, const char* name, enum data_type content_type) {
    if (column_presence_count(schema->start, schema->count, name) == 0) {
        column_add(schema, name, content_type);
        schema->count++;
        schema->length += schema->end->size;
        return schema;
    } else {
        printf("Can't add two attributes with the same name");
        return schema;
    }
}

struct schema* schema_add_column_varchar(struct schema* schema, const char* name, enum data_type content_type, uint16_t size) {
    if (column_presence_count(schema->start, schema->count, name) == 0) {
        column_add_varchar(schema, name, content_type, size);
        schema->count++;
        schema->length += schema->end->size;
        return schema;
    } else {
        printf("Can't add two attributes with the same name");
        return schema;
    }
}

struct column* column_create(const char* name, enum data_type content_type ) {
    struct column* new = malloc(sizeof (struct column));
    if (new == NULL || content_type == VARCHAR) {
        return NULL;
    }
    strncpy(new->name, "", MAX_NAME_LENGTH);
    strncpy(new->name, name, strlen(name));
    new->data_type = content_type;
    switch (content_type) {
        case INTEGER:
            new->size = sizeof(int32_t);
            break;
        case BOOLEAN:
            new->size = sizeof(bool);
            break;
        case DOUBLE:
            new->size = sizeof(double);
            break;
    }
    new->next = NULL;
    return new;
}


struct column* column_create_varchar(const char* name, enum data_type content_type, uint16_t size) {
    if (content_type != VARCHAR)
        return NULL;

    struct column* new = malloc(sizeof (struct column));
    if (!new) {
        return NULL;
    }
    strncpy(new->name, "", MAX_NAME_LENGTH);
    strncpy(new->name, name, strlen(name));

    new->next = NULL;
    new->size = sizeof(char) * size;
    new->data_type = content_type;
    return new;
}



struct column* column_delete(struct column* current, const char* name, struct schema* schema) {
    struct column* column;
    if (!current)
        return NULL;
    else if (strcmp(current->name, name) == 0) {
        column = current->next;
        free(current);
        return column;
    } else {
        current->next = column_delete(current->next, name, schema);
        if (current->next == NULL) {
            schema->end = current;
        }
        return current;
    }
}
