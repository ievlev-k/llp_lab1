
#include "include/file.h"



enum file_status file_open(FILE **file, const char *const name, const char *const mode) {
    *file = fopen(name, mode);
    if (*file == NULL) {
        return ERROR;
    }
    else return OK;
}

enum file_status file_close(FILE *file) {
    if (fclose(file) == 0) {
        return OK;
    }
    else return ERROR;
}

enum file_status file_update_last_page(FILE *file, uint32_t old_number, uint32_t new) {
    struct page_header* page_header = malloc(sizeof(struct page_header));

    fseek(file, (old_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
    fread(page_header, sizeof(struct page_header), 1, file);
    page_header->next_page_number = new;

    fseek(file, (old_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
    if (fwrite(page_header, sizeof(struct page_header), 1, file) == 1) {
        free(page_header);
        return OK;
    }
    else {
        free(page_header);
        return ERROR;
    }
}

long int database_get_size(FILE* file) {
    fseek(file, 0, SEEK_END);
    return ftell(file);
}

enum file_status database_save_to_file(FILE *file, struct database_header* database_header, struct page_header* page_header_real) {
    fseek(file, 0, SEEK_SET);
    if (fwrite(database_header, sizeof(struct database_header), 1, file) == 1) {
        page_header_real->remaining_space -= sizeof(struct database_header) + sizeof(struct page_header);

        page_header_real->write_ptr += sizeof(struct database_header) + sizeof(struct page_header);

        if (fwrite(page_header_real, sizeof(struct page_header), 1, file) == 1) {
            return OK;
        }
    }
    return ERROR;
}

enum file_status database_header_save_changes(FILE *file, struct database_header* database_header) {
    fseek(file, 0, SEEK_SET);
    if (fwrite(database_header, sizeof(struct database_header), 1, file) == 1) {
        return OK;
    } else return ERROR;
}

enum file_status database_header_read(FILE *file, struct database_header* database_header) {
    fseek(file, 0, SEEK_SET);
    if (fread(database_header, sizeof(struct database_header), 1, file) == 1) {
        return OK;
    } else return ERROR;
}

enum file_status database_update_last_page(FILE *file, struct database_header* database_header, uint32_t new) {
    uint32_t last_page_number = database_header->last_page_number;
    struct page_header* page_header = malloc(sizeof(struct page_header));

    if (database_header->last_page_number == 1) {
        fseek(file, sizeof(struct database_header), SEEK_SET);
    } else fseek(file, (database_header->last_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

    fread(page_header, sizeof(struct page_header), 1, file);

    page_header->next_page_number = new;

    if (last_page_number == 1) {
        fseek(file, sizeof(struct database_header), SEEK_SET);
    } else fseek(file, (last_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

    if (fwrite(page_header, sizeof(struct page_header), 1, file) == 1) {
        free(page_header);
        return OK;
    } else {
        free(page_header);
        return ERROR;
    }
}

enum file_status page_header_write_real(FILE *file, struct database_header* database_header, struct table_header* table_header) {

    struct page_header* new = NULL;

    if (database_header->last_page_number == 1) {
        fseek(file, sizeof(struct database_header), SEEK_SET);
    } else fseek(file, (database_header->last_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

    struct page_header* real_page = malloc(sizeof(struct page_header));

    fread(real_page, sizeof(struct page_header), 1, file);
    if (!is_enough_space(real_page, sizeof(struct table_header))) {
        new = page_add_real(database_header);
        real_page = new;
        if (database_header_save_changes(file, database_header) != OK) {
            return ERROR;
        }
        fseek(file, (real_page->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

        real_page->remaining_space -= sizeof(struct page_header);

        real_page->write_ptr += sizeof(struct page_header);

        if (fwrite(real_page, sizeof(struct page_header), 1, file) != 1) {
            return ERROR;
        }
    }
    fseek(file, real_page->write_ptr, SEEK_SET);
    if (fwrite(table_header, sizeof(struct table_header), 1, file) == 1) {
        real_page->remaining_space -= sizeof(struct table_header);
        real_page->write_ptr += sizeof(struct table_header);

        if (real_page->page_number == 1)  {
            fseek(file, sizeof(struct database_header), SEEK_SET);
        } else fseek(file, (real_page->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

        if (fwrite(real_page, sizeof(struct page_header), 1, file) == 1) {
            if (new) {
                free(new);
            }
            //free(real_page);
            return OK;
        }
    }
    if (new) {
        free(new);
    }
    free(real_page);
    return ERROR;
}



void query_join_output(char* begin_left, char* begin_right, struct table* left, struct table* right, uint32_t left_offset, uint32_t right_offset) {
    uint16_t offset_one = 0;
    uint16_t offset_two = 0;
    for (size_t i = 0; i < left->schema->count; i++) {
        if (offset_one != left_offset) {
            switch (left->schema->start[i].data_type) {
                case INTEGER:
                    integer_output(begin_left, offset_one);
                    break;
                case BOOLEAN:
                    boolean_output(begin_left, offset_one);
                    break;
                case DOUBLE:
                    double_output(begin_left, offset_one);
                    break;
                case VARCHAR:
                    varchar_output(begin_left, offset_one);
                    break;
            }
        } else {
            for (size_t j = 0; j < right->schema->count; j++) {
                if (offset_two != right_offset) {
                    switch (right->schema->start[j].data_type) {
                        case INTEGER:
                            integer_output(begin_right, offset_two);
                            break;
                        case BOOLEAN:
                            boolean_output(begin_right, offset_two);
                            break;
                        case DOUBLE:
                            double_output(begin_right, offset_two);
                            break;
                        case VARCHAR:
                            varchar_output(begin_right, offset_two);
                            break;
                    }
                }
                offset_two += right->schema->start[j].size;
            }
        }
        offset_one += left->schema->start[i].size;
    }
    printf("\n");
}


bool integer_query_join_compare(char* left_row, char* right_row, struct query_params* left_query, struct query_params* right_query, struct table* left, struct table* right) {
    int32_t value_one = (int32_t) *(left_row + left_query->offset);
    int32_t value_two = (int32_t) *(right_row + right_query->offset);
    if (value_one == value_two) {
        query_join_output(left_row, right_row, left, right, left_query->offset, right_query->offset);
        return true;
    } else return false;
}

bool boolean_query_join_compare(char* left_row, char* right_row, struct query_params* left_query, struct query_params* right_query, struct table* left, struct table* right) {
    bool value_one = (bool) *(left_row + left_query->offset);
    bool value_two = (bool) *(right_row + right_query->offset);
    if (value_one == value_two) {
        query_join_output(left_row, right_row, left, right, left_query->offset, right_query->offset);
        return true;
    } else return false;
}

bool double_query_join_compare(char* left_row, char* right_row, struct query_params* left_query, struct query_params* right_query, struct table* left, struct table* right) {
    double value_one = (double) *(left_row + left_query->offset);
    double value_two = (double) *(right_row + right_query->offset);
    if (value_one == value_two) {
        query_join_output(left_row, right_row, left, right, left_query->offset, right_query->offset);
        return true;
    } else return false;
}


bool varchar_query_join_compare(char* left_row, char* right_row, struct query_params* left_query, struct query_params* right_query, struct table* left, struct table* right) {
    char* value_one = (char*) (left_row + left_query->offset);
    char* value_two = (char*) (right_row + right_query->offset);
    if (strcmp(value_one, value_two) == 0) {
        query_join_output(left_row, right_row, left, right, left_query->offset, right_query->offset);
        return true;
    } else return false;
}

uint32_t query_join_attempt(FILE *file, struct table* left, struct table* right, struct query_params* left_query, struct query_params* right_query, char* left_row) {
    struct row_header* row_header =  malloc(sizeof(struct row_header));

    uint32_t ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * right->schema->count;

    char* row_ptr = malloc(right->schema->length);

    struct page_header* page_header = malloc(sizeof(struct page_header));

    fseek(file, (right->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
    fread(page_header, sizeof(struct page_header), 1, file);
    fseek(file, (right->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * right->schema->count, SEEK_SET); //передвинулись на начало строк

    while (ptr != page_header->write_ptr) {
        fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr, SEEK_SET);
        fread(row_header, sizeof(struct row_header), 1, file);
        if (row_header->is_available) {
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr + sizeof(struct row_header), SEEK_SET);
            fread(row_ptr, right->schema->length, 1, file);
            switch (right_query->data_type) {
                case INTEGER:
                    if (integer_query_join_compare(left_row, row_ptr, left_query, right_query, left, right)) {
                        return 1;
                    }
                    break;
                case BOOLEAN:
                    if (boolean_query_join_compare(left_row, row_ptr, left_query, right_query, left, right)) {
                        return 1;
                    }
                    break;
                case DOUBLE:
                    if (double_query_join_compare(left_row, row_ptr, left_query, right_query, left, right)) {
                        return 1;
                    }
                    break;
                case VARCHAR:
                    if (varchar_query_join_compare(left_row, row_ptr, left_query, right_query, left, right)) {
                        return 1;
                    }
                    break;
            }
        }

        ptr += sizeof(struct row_header) + right->schema->length;

        if (page_header->next_page_number != 0 && ptr == page_header->write_ptr) {
            ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * right->schema->count;
            fseek(file, (page_header->next_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
            fread(page_header, sizeof(struct page_header), 1, file);
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * right->schema->count, SEEK_SET);
        }
    }
    free(row_ptr);
    free(row_header);
    free(page_header);
    return 0;
}

void query_join(FILE *file, struct table* left, struct table* right, struct query_params* left_query, struct query_params* right_query) {
    uint32_t result_count = 0;
    uint32_t ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * left->schema->count;

    struct row_header* row_header =  malloc(sizeof(struct row_header));
    char* row_ptr = malloc(left->schema->length);

    struct page_header* page_header = malloc(sizeof(struct page_header));

    fseek(file, (left->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
    fread(page_header, sizeof(struct page_header), 1, file);
    fseek(file, (left->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * left->schema->count, SEEK_SET); //передвинулись на начало строк

    while (ptr != page_header->write_ptr) {
        fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr, SEEK_SET);
        fread(row_header, sizeof(struct row_header), 1, file);
        if (row_header->is_available) {
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr + sizeof(struct row_header), SEEK_SET);
            fread(row_ptr, left->schema->length, 1, file);
            result_count += query_join_attempt(file, left, right, left_query, right_query, row_ptr);
        }

        ptr += sizeof(struct row_header) + left->schema->length;

        if (page_header->next_page_number != 0 && ptr == page_header->write_ptr) {
            ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * left->schema->count;
            fseek(file, (page_header->next_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
            fread(page_header, sizeof(struct page_header), 1, file);
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * left->schema->count, SEEK_SET);
        }
    }
    free(row_ptr);
    free(row_header);
    free(page_header);
    printf("Joined %d rows\n", result_count);

}

enum file_status table_header_save_changes(FILE *file, struct table_header* table_header) {
    fseek(file, sizeof(struct database_header) + sizeof(struct page_header) + (table_header->real_number - 1) * sizeof(struct table_header), SEEK_SET);
    if (fwrite(table_header, sizeof(struct table_header), 1, file) == 1) {
        return OK;
    } else return ERROR;
}



enum file_status table_page_write(FILE *file, struct page_header* page_header, struct schema* schema) {
    fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

    uint16_t size = schema->count * sizeof(struct column);

    page_header->remaining_space -= sizeof(struct page_header) + sizeof(uint16_t) + size;
    page_header->write_ptr += sizeof(struct page_header) + sizeof(uint16_t) + size;

    if (fwrite(page_header, sizeof(struct page_header), 1, file) != 1) {
        return ERROR;
    }

    struct column* columns = malloc(size);

    struct column* current = schema->start;

    for (size_t i = 0; i < schema->count; i++){
        columns[i] = *current;
        current = current->next;
    }

    fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header), SEEK_SET);
    if (fwrite(&size, sizeof(uint16_t), 1, file) != 1) {
        return ERROR;
    }
    fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t), SEEK_SET);
    if (fwrite(columns, size, 1, file) != 1) {
        return ERROR;
    }
    free(columns);
    free(page_header);
    return OK;
}

enum file_status table_header_read(FILE *file, const char *const table_name, struct table_header* table_header, size_t table_count) {
    if (is_table_present(file, table_count, table_name, table_header)) {
        return OK;
    } else return ERROR;
}


enum file_status table_read_columns(FILE *file, struct table* table) {
    struct column* columns = malloc(sizeof(struct column) * table->table_header->schema.count);
    uint16_t* size = malloc(sizeof(uint16_t));

    fseek(file, (table->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header), SEEK_SET);
    fread(size, sizeof(uint16_t), 1, file);
    fread(columns, *size, 1, file);

    table->schema->start = columns;
    table->schema->count = table->table_header->schema.count;
    table->schema->length = table->table_header->schema.length;
    table->schema->end = NULL;

    free(size);

    return OK;
}

enum file_status row_write_to_page(FILE *file, uint32_t page_number, struct row* row) {

    struct page_header* new = NULL;
    uint32_t length = row->table->table_header->schema.length;
    uint32_t size = sizeof(struct row_header) + length;
    fseek(file, (page_number-1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
    struct page_header* page_header = malloc(sizeof(struct page_header));
    if (fread(page_header, sizeof(struct page_header), 1, file) == 1) {

        if (!is_enough_space(page_header, size)) {

            new = page_add(row->table->table_header, row->table->table_header->database->database_header);

            fseek(file, (page_number-1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header), SEEK_SET);
            uint16_t columns_size;
            fread(&columns_size, sizeof(uint16_t), 1, file);

            struct column* columns = malloc(columns_size);

            fseek(file, (page_number-1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t), SEEK_SET);
            fread(columns, columns_size, 1, file);

            page_number = new->page_number;
            page_header = new;

            page_header->remaining_space -= sizeof(struct page_header) + sizeof(uint16_t) + columns_size;
            page_header->write_ptr += sizeof(struct page_header) + sizeof(uint16_t) + columns_size;

            fseek(file, (page_number-1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

            fwrite(page_header, sizeof(struct page_header), 1, file);
            fwrite(&columns_size, sizeof(uint16_t), 1, file);
            fwrite(columns, columns_size, 1, file);

            free(columns);
        }

        fseek(file, (page_number-1) * DEFAULT_PAGE_SIZE_BYTES + page_header->write_ptr, SEEK_SET);

        if (fwrite(row->row_header, sizeof(struct row_header), 1, file) == 1) {
            fseek(file, (page_number-1) * DEFAULT_PAGE_SIZE_BYTES + page_header->write_ptr + sizeof(struct row_header), SEEK_SET);
            if (fwrite(row->data, length, 1, file) == 1) {

                page_header->write_ptr += sizeof(struct row_header) + length;
                page_header->remaining_space -= sizeof(struct row_header) + length;

                fseek(file, (page_number-1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

                if (fwrite(page_header, sizeof(struct page_header), 1, file) != 1) {
                    if (new) free(new);
                    free(page_header);
                    return ERROR;
                }
                if (new) {
                    free(new);
                }

                return OK;
            }
        }
        if (new) {
            free(new);
        }
        free(page_header);
        return ERROR;
    } else {
        if (new) {
            free(new);
        }
        free(page_header);
        return ERROR;
    }
}


void row_remove(struct table *table, uint32_t ptr, uint32_t page_number) {
    struct row_header row_header =  {
            false
    };

    fseek(table->table_header->database->source_file, (page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr, SEEK_SET);

    fwrite(&row_header, sizeof(struct row_header), 1, table->table_header->database->source_file);
}


void update_query(char *begin, void *value, struct query_params *query, struct table *table, uint32_t ptr,
                  uint32_t page_number, bool show_output) {
    switch (query->data_type) {
        case INTEGER:
            integer_update(begin, value, query->offset);
            break;
        case BOOLEAN:
            boolean_update(begin, value, query->offset);
            break;
        case VARCHAR:
            varchar_update(begin, value, query->offset, query->size);
            break;
        case DOUBLE:
            double_update(begin, value, query->offset);
            break;
    }

    fseek(table->table_header->database->source_file, (page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr, SEEK_SET);

    fwrite(begin, table->schema->length, 1, table->table_header->database->source_file);

    if (show_output)
        data_output(begin, table->schema->start, table->schema->count);
}

void select_execute(FILE *file, struct table *table, uint32_t offset, uint16_t column_size, void *value,
                    enum data_type type, int32_t row_count, bool show_output) {
    uint32_t result_count = 0;
    uint32_t ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count;

    char* row_ptr = malloc(table->schema->length);
    struct row_header* row_header = malloc(sizeof(struct row_header));

    struct page_header* page_header = malloc(sizeof(struct page_header));

    fseek(file, (table->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

    fread(page_header, sizeof(struct page_header), 1, file);

    fseek(file, (table->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count, SEEK_SET); //передвинулись на начало строк

    while (ptr != page_header->write_ptr) {
        fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr, SEEK_SET);
        fread(row_header, sizeof(struct row_header), 1, file);
        if (row_header->is_available) {
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr + sizeof(struct row_header), SEEK_SET);
            fread(row_ptr, table->schema->length, 1, file);
            switch (type) {
                case INTEGER:
                    if (integer_compare(row_ptr, value, offset)) {
                        if (show_output)
                            data_output(row_ptr, table->schema->start, table->schema->count);
                        result_count++;
                    }
                    break;
                case BOOLEAN:
                    if (boolean_compare(row_ptr, value, offset)) {
                        if (show_output)
                            data_output(row_ptr, table->schema->start, table->schema->count);
                        result_count++;
                    }
                    break;
                case DOUBLE:
                    if (double_compare(row_ptr, value, offset)) {
                        if (show_output)
                            data_output(row_ptr, table->schema->start, table->schema->count);
                        result_count++;
                    }
                    break;
                case VARCHAR:
                    if (varchar_compare(row_ptr, value, offset, column_size)) {
                        if (show_output)
                            data_output(row_ptr, table->schema->start, table->schema->count);
                        result_count++;
                    }
                    break;
            }
        }

        ptr += table->schema->length + sizeof(struct row_header);

        if (page_header->next_page_number != 0 && ptr == page_header->write_ptr) {
            ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count;
            fseek(file, (page_header->next_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
            fread(page_header, sizeof(struct page_header), 1, file);
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count, SEEK_SET);
        }

    }
    free(row_header);
    free(row_ptr);
    free(page_header);

    printf("Selected %d rows\n", result_count);

}

void update_execute(FILE *file, struct table *table, struct query_params *first, struct query_params *second,
                    void **values, bool show_output) {
    uint32_t result_count = 0;
    uint32_t current = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count;

    struct row_header* row_header = malloc(sizeof(struct row_header));

    char* row_ptr = malloc(table->schema->length);

    struct page_header* page_header = malloc(sizeof(struct page_header));

    fseek(file, (table->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);

    fread(page_header, sizeof(struct page_header), 1, file);

    fseek(file, (table->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count, SEEK_SET); //передвинулись на начало строк

    while (current < page_header->write_ptr) {

        fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + current, SEEK_SET);
        fread(row_header, sizeof(struct row_header), 1, file);

        if (row_header->is_available) {

            uint32_t current_ptr = current + sizeof(struct row_header);

            fread(row_ptr, table->schema->length, 1, file);

            switch (first->data_type) {
                case INTEGER:
                    if (integer_compare(row_ptr, values[0], first->offset)) {
                        update_query(row_ptr, values[1], second, table, current_ptr, page_header->page_number, show_output);
                        result_count++;
                    }
                    break;
                case BOOLEAN:
                    if (boolean_compare(row_ptr, values[0], first->offset)) {
                        update_query(row_ptr, values[1], second, table, current_ptr, page_header->page_number, show_output);
                        result_count++;
                    }
                    break;
                case DOUBLE:
                    if (double_compare(row_ptr, values[0], first->offset)) {
                        update_query(row_ptr, values[1], second, table, current_ptr, page_header->page_number, show_output);
                        result_count++;
                    }
                    break;
                case VARCHAR:
                    if (varchar_compare(row_ptr, values[0], first->offset, first->size)) {
                        update_query(row_ptr, values[1], second, table, current_ptr, page_header->page_number, show_output);
                        result_count++;
                    }
                    break;
            }
        }

        current += table->schema->length + sizeof(struct row_header);

        if (current == page_header->write_ptr != 0 && page_header->next_page_number) {
            current = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count;
            fseek(file, (page_header->next_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
            fread(page_header, sizeof(struct page_header), 1, file);
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count, SEEK_SET);
        }

    }
    free(row_header);
    free(row_ptr);
    free(page_header);

    printf("Updated %d rows\n", result_count);
}


void delete_execute(FILE *file, struct table* table, struct query_params* query, void* value) {
    uint32_t result_count = 0;
    uint32_t ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count;

    struct row_header* row_header =  malloc(sizeof(struct row_header));

    struct page_header* page_header = malloc(sizeof(struct page_header));

    char* row_ptr = malloc(table->schema->length);

    fseek(file, (table->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
    fread(page_header, sizeof(struct page_header), 1, file);

    fseek(file, (table->table_header->page_number_first - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count, SEEK_SET); //передвинулись на начало строк

    while (ptr != page_header->write_ptr) {

        fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr, SEEK_SET);
        fread(row_header, sizeof(struct row_header), 1, file);

        if (row_header->is_available) {
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + ptr + sizeof(struct row_header), SEEK_SET);
            fread(row_ptr, table->schema->length, 1, file);

            switch (query->data_type) {
                case INTEGER:
                    if (integer_compare(row_ptr, value, query->offset)) {
                        row_remove(table, ptr, page_header->page_number);
                        result_count++;
                    }
                    break;
                case BOOLEAN:
                    if (boolean_compare(row_ptr, value, query->offset)) {
                        row_remove(table, ptr, page_header->page_number);
                        result_count++;
                    }
                    break;
                case DOUBLE:
                    if (double_compare(row_ptr, value, query->offset)) {
                        row_remove(table, ptr, page_header->page_number);
                        result_count++;
                    }
                    break;
                case VARCHAR:
                    if (varchar_compare(row_ptr, value, query->offset, query->size)) {
                        row_remove(table, ptr, page_header->page_number);
                        result_count++;
                    }
                    break;
            }
        }

        ptr += table->schema->length + sizeof(struct row_header);

        if (page_header->next_page_number != 0 && ptr == page_header->write_ptr) {
            ptr = sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count;
            fseek(file, (page_header->next_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
            fread(page_header, sizeof(struct page_header), 1, file);
            fseek(file, (page_header->page_number - 1) * DEFAULT_PAGE_SIZE_BYTES + sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->schema->count, SEEK_SET);
        }
    }
    free(row_header);
    free(page_header);
    free(row_ptr);
    printf("Deleted %d rows\n", result_count);
}

bool is_table_present(FILE *file, const size_t length, const char* name, struct table_header* table_header) {

    uint32_t idx = 0;
    uint32_t ptr = sizeof(struct database_header) + sizeof(struct page_header);

    struct page_header* page_header = malloc(sizeof(struct page_header));

    fseek(file, sizeof(struct database_header), SEEK_SET);

    fread(page_header, sizeof(struct page_header), 1, file);

    if (length > 0) {

        fseek(file, sizeof(struct database_header)+sizeof(struct page_header), SEEK_SET);

        while (idx < length) {

            fread(table_header, sizeof(struct table_header), 1, file);
            if (table_header->is_available && strcmp(table_header->name, name) == 0) {
                free(page_header);
                return true;
            }

            idx++;

            ptr += sizeof(struct table_header);

            if (page_header->next_page_number != 0 && ptr == page_header->write_ptr) {
                ptr = 0;
                fseek(file, (page_header->next_page_number - 1) * DEFAULT_PAGE_SIZE_BYTES, SEEK_SET);
                fread(page_header, sizeof(struct page_header), 1, file);
            }

        }

        free(page_header);
        return false;

    } else {
        free(page_header);
        return false;
    }
}

