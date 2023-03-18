#include <stdio.h>
#include <time.h>


#include "include/schema.h"






void size_test() {
    printf("Select with big data amount operation test:\n");

    struct database* test_database = db_get("select_size.bin", NEW);

    struct schema* schema_one = schema_create();
    schema_one = schema_add_column_varchar(schema_one, "varchar", VARCHAR, 20);
    schema_one = schema_add_column(schema_one, "integer", INTEGER);
    schema_one = schema_add_column(schema_one, "boolean", BOOLEAN);
    schema_one = schema_add_column(schema_one, "double", DOUBLE);

    struct table_struct* relation_one = table_create(schema_one, "test", test_database);

    struct row* row_one = row_create(relation_one);

    char* strings[9] = {"Abc", "aBc", "abC", "ABc", "aBC", "AbC", "ABC", "ABCD", "abcd"};
    int32_t integers[9] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
    bool booleans[2] = {true, false};
    double doubles[9] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};

    char* query_string = "abc";
    char* column[1] = {"varchar"};
    void* value[1] = {&query_string};
    struct query* select_query = query_make(SELECT, relation_one, column, value, -1);

    clock_t begin;
    clock_t end;
    double elapsed_time;

    for (size_t i = 0; i < 9; i++) {
        for (size_t j = 0; j < 10000; j++) {
            attribute_add(row_one, "integer", INTEGER, (void *) &integers[i % 9]);
            attribute_add(row_one, "varchar", VARCHAR, (void *) &strings[i % 9]);
            attribute_add(row_one, "boolean", BOOLEAN, (void *) &booleans[i % 2]);
            attribute_add(row_one, "double", DOUBLE, (void *) &doubles[i % 9]);
            row_insert(row_one);
        }
        query_string = strings[i % 10];
        begin = clock();
        query_execute(select_query, false);
        end = clock();
        elapsed_time = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Select from %d rows took %f seconds\n", 1000*(i+1), elapsed_time);
    }

    query_close(select_query);
    row_close(row_one);
    relation_close(relation_one);
    schema_close(schema_one);
    db_close(test_database);
}

void save_test() {
    printf("Load existing DB test:\n");

    struct database* test_database = db_get("save.bin", NEW);

    struct schema* schema_one = schema_create();
    schema_one = schema_add_column_varchar(schema_one, "varchar", VARCHAR, 20);
    schema_one = schema_add_column(schema_one, "integer", INTEGER);
    schema_one = schema_add_column(schema_one, "boolean", BOOLEAN);
    schema_one = schema_add_column(schema_one, "double", DOUBLE);

    struct table_struct* relation_one = table_create(schema_one, "test", test_database);

    struct row* row_one = row_create(relation_one);

    row_close(row_one);
    relation_close(relation_one);
    schema_close(schema_one);
    db_close(test_database);

    struct database* opened_database = db_get("save.bin", SAVED_IN_FILE);

    relation_one = table_get("test", opened_database);

    row_one = row_create(relation_one);

    char* strings[9] = {"Abc", "aBc", "abC", "ABc", "aBC", "AbC", "ABC", "ABCD", "abcd"};
    int32_t integers[9] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
    bool booleans[2] = {true, false};
    double doubles[9] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};

    char* query_string = "abc";
    char* column[1] = {"varchar"};
    void* value[1] = {&query_string};
    struct query* select_query = query_make(SELECT, relation_one, column, value, -1);

    for (size_t i = 0; i < 9; i++) {
        for (size_t j = 0; j < 1000; j++) {
            attribute_add(row_one, "integer", INTEGER, (void *) &integers[i % 9]);
            attribute_add(row_one, "varchar", VARCHAR, (void *) &strings[i % 9]);
            attribute_add(row_one, "boolean", BOOLEAN, (void *) &booleans[i % 2]);
            attribute_add(row_one, "double", DOUBLE, (void *) &doubles[i % 9]);
            row_insert(row_one);
        }
        query_string = strings[i % 10];
        query_execute(select_query, false);
    }

    relation_close(relation_one);
    db_close(opened_database);
}

void test_insert() {

    printf("Insert operation test:\n");
    double elapsed_time;
    struct database* test_database = db_get("insert.bin", NEW);

    struct schema* schema_one = schema_create();
    schema_one = schema_add_column_varchar(schema_one, "varchar", VARCHAR, 20);
    schema_one = schema_add_column(schema_one, "integer", INTEGER);
    schema_one = schema_add_column(schema_one, "boolean", BOOLEAN);
    schema_one = schema_add_column(schema_one, "double", DOUBLE);

    struct table_struct* relation_one = table_create(schema_one, "test", test_database);

    struct row* row_one = row_create(relation_one);

    uint32_t test_int = 100;
    char* test_char = "Test";
    bool test_boolean = true;
    double test_double = 100.01;

    attribute_add(row_one, "varchar", VARCHAR, (void *) &test_char);
    attribute_add(row_one, "integer", INTEGER, (void *) &test_int);
    attribute_add(row_one, "boolean", BOOLEAN, (void *) &test_boolean);
    attribute_add(row_one, "double", INTEGER, (void *) &test_double);

    clock_t begin;
    clock_t end;

    for (size_t i = 1; i < 10; i++) {
        begin = clock();
        for (size_t j=0; j<1000; j++) {
            row_insert(row_one);
        }
        end = clock();
        elapsed_time = (double) (end - begin) / CLOCKS_PER_SEC;
        long int file_size = database_get_size(test_database->source_file);
        printf("Insertion of %d rows took %f seconds\n", 1000, elapsed_time);
        printf("File size is %d bytes\n", file_size);
    }

    row_close(row_one);
    relation_close(relation_one);
    schema_close(schema_one);
    db_close(test_database);
}

void test_select() {
    printf("Select operation test:\n");

    struct database* test_database = db_get("select.bin", NEW);

    struct schema* schema_one = schema_create();
    schema_one = schema_add_column_varchar(schema_one, "varchar", VARCHAR, 20);
    schema_one = schema_add_column(schema_one, "integer", INTEGER);
    schema_one = schema_add_column(schema_one, "boolean", BOOLEAN);
    schema_one = schema_add_column(schema_one, "double", DOUBLE);

    struct table_struct* relation_one = table_create(schema_one, "test", test_database);

    struct row* row_one = row_create(relation_one);

    char* strings[9] = {"Abc", "aBc", "abC", "ABc", "aBC", "AbC", "ABC", "ABCD", "abcd"};
    int32_t integers[9] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
    bool booleans[2] = {true, false};
    double doubles[9] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};

    char* query_string = "abc";
    char* column[1] = {"varchar"};
    void* value[1] = {&query_string};
    struct query* select_query = query_make(SELECT, relation_one, column, value, -1);

    clock_t begin;
    clock_t end;
    double elapsed_time;

    for (size_t i = 0; i < 9; i++) {
        for (size_t j = 0; j < 1000; j++) {
            attribute_add(row_one, "integer", INTEGER, (void *) &integers[i % 9]);
            attribute_add(row_one, "varchar", VARCHAR, (void *) &strings[i % 9]);
            attribute_add(row_one, "boolean", BOOLEAN, (void *) &booleans[i % 2]);
            attribute_add(row_one, "double", DOUBLE, (void *) &doubles[i % 9]);
            row_insert(row_one);
        }
        query_string = strings[i % 10];
        begin = clock();
        query_execute(select_query, false);
        end = clock();
        elapsed_time = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Select from %d rows took %f seconds\n", 1000*(i+1), elapsed_time);
    }

    query_close(select_query);
    row_close(row_one);
    relation_close(relation_one);
    schema_close(schema_one);
    db_close(test_database);
}

void test_update() {
    printf("Update operation test:\n");

    struct database* test_database = db_get("update.bin", NEW);

    struct schema* schema_one = schema_create();
    schema_one = schema_add_column_varchar(schema_one, "varchar", VARCHAR, 20);
    schema_one = schema_add_column(schema_one, "integer", INTEGER);
    schema_one = schema_add_column(schema_one, "boolean", BOOLEAN);
    schema_one = schema_add_column(schema_one, "double", DOUBLE);

    struct table_struct* relation_one = table_create(schema_one, "test", test_database);

    struct row* row_one = row_create(relation_one);

    char* strings[9] = {"Abc", "aBc", "abC", "ABc", "aBC", "AbC", "ABC", "ABCD", "abcd"};
    int32_t integers[9] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
    bool booleans[2] = {true, false};
    double doubles[9] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};

    char* query_string = "ABC";
    int32_t query_int = 0;
    char* columns[2] = {"varchar", "integer"};
    void* values[2] = {&query_string, &query_int};

    struct query* update_query = query_make(UPDATE, relation_one, columns, values, -1);

    clock_t begin;
    clock_t end;
    double elapsed_time;

    for (size_t i = 0; i < 9; i++) {
        for (size_t j = 0; j < 1000; j++) {
            attribute_add(row_one, "varchar", VARCHAR, (void *) &strings[i % 9]);
            attribute_add(row_one, "integer", INTEGER, (void *) &integers[i % 9]);
            attribute_add(row_one, "boolean", BOOLEAN, (void *) &booleans[i % 2]);
            attribute_add(row_one, "double", INTEGER, (void *) &doubles[i % 9]);
            row_insert(row_one);
        }
        query_string = strings[i % 10];
        query_int = integers[(i + 2) % 9];

        begin = clock();
        query_execute(update_query, false);
        end = clock();

        elapsed_time = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Update with %d rows took %f seconds\n", 1000*(i+1), elapsed_time);
    }

    query_close(update_query);
    row_close(row_one);
    relation_close(relation_one);
    schema_close(schema_one);
    db_close(test_database);
}

void test_delete() {
    printf("Delete operation test:\n");

    struct database* test_database = db_get("delete.bin", NEW);

    struct schema* schema_one = schema_create();
    schema_one = schema_add_column_varchar(schema_one, "varchar", VARCHAR, 20);
    schema_one = schema_add_column(schema_one, "integer", INTEGER);
    schema_one = schema_add_column(schema_one, "boolean", BOOLEAN);
    schema_one = schema_add_column(schema_one, "double", DOUBLE);

    struct table_struct* relation_one = table_create(schema_one, "test", test_database);

    struct row* row_one = row_create(relation_one);

    char* strings[9] = {"Abc", "aBc", "abC", "ABc", "aBC", "AbC", "ABC", "ABCD", "abcd"};
    int32_t integers[9] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
    bool booleans[2] = {true, false};
    double doubles[9] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};

    char* query_string = "abc";
    char* column[1] = {"varchar"};
    void* value[1] = {&query_string};
    struct query* delete_query = query_make(DELETE, relation_one, column, value, -1);

    clock_t begin;
    clock_t end;
    double elapsed_time;

    for (size_t i = 0; i < 9; i++) {
        for (size_t j = 0; j < 1000; j++) {
            attribute_add(row_one, "integer", INTEGER, (void *) &integers[i % 9]);
            attribute_add(row_one, "varchar", VARCHAR, (void *) &strings[i % 9]);
            attribute_add(row_one, "boolean", BOOLEAN, (void *) &booleans[i % 2]);
            attribute_add(row_one, "double", DOUBLE, (void *) &doubles[i % 9]);
            row_insert(row_one);
        }
        query_string = strings[i % 10];
        begin = clock();
        query_execute(delete_query, true);
        end = clock();
        elapsed_time = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Deletion from %d rows took %f seconds\n", 1000*(i+1), elapsed_time);
    }

    query_close(delete_query);
    row_close(row_one);
    relation_close(relation_one);
    schema_close(schema_one);
    db_close(test_database);
}

void test_join() {
    printf("Select with join operation test:\n");
    struct schema* schema_one = schema_create();
    schema_one = schema_add_column(schema_one, "id", INTEGER);
    schema_one = schema_add_column_varchar(schema_one, "varchar", VARCHAR, 20);
    schema_one = schema_add_column(schema_one, "foreign_id", INTEGER);

    struct schema* schema_two = schema_create();
    schema_two = schema_add_column(schema_two, "id", INTEGER);
    schema_two = schema_add_column_varchar(schema_two, "varchar2", VARCHAR, 20);

    struct database* test_database = db_get("db.bin", NEW);

    struct table_struct* relation_one = table_create(schema_one, "first", test_database);
    struct table_struct* relation_two = table_create(schema_two, "second", test_database);

    struct row* row_one = row_create(relation_one);

    char* placeholder = "placeholder";
    uint32_t foreign_key = 1;

    uint32_t id = 1;

    attribute_add(row_one, "varchar", VARCHAR, (void*) &placeholder);
    attribute_add(row_one, "foreign_id", INTEGER, (void*) &foreign_key);
    for (size_t i = 0; i < 10; i++) {
        attribute_add(row_one, "id", INTEGER, (void*) &id);
        row_insert(row_one);
        id++;
    }

    char* placeholder2 = "placeholder but better";
    foreign_key = 2;
    attribute_add(row_one, "foreign_id", INTEGER, (void*) &foreign_key);
    attribute_add(row_one, "varchar", VARCHAR, (void*) &placeholder2);
    for (size_t i = 0; i < 10; i++){
        attribute_add(row_one, "id", INTEGER, (void*) &id);
        row_insert(row_one);
        id++;
    }

    struct row* row_two = row_create(relation_two);
    uint32_t id_two = 1;
    char* varchar2 = "foreign1";
    attribute_add(row_two, "id", INTEGER, (void*) &id_two);
    attribute_add(row_two, "varchar2", VARCHAR, (void*) &varchar2);
    row_insert(row_two);

    id_two = 2;
    varchar2 = "foreign2";
    attribute_add(row_two, "id", INTEGER, (void*) &id_two);
    attribute_add(row_two, "varchar2", VARCHAR, (void*) &varchar2);
    row_insert(row_two);

    struct query_join* select_query_7 = query_join_make(relation_one, relation_two, "foreign_id", "id");
    query_join_execute(select_query_7);

    query_join_close(select_query_7);
    row_close(row_one);
    row_close(row_two);
    relation_close(relation_one);
    relation_close(relation_two);
    schema_close(schema_one);
    schema_close(schema_two);
    db_close(test_database);
}


int main() {
    size_test();
    test_insert();
    test_select();
    test_update();
    test_delete();
    test_join();
    save_test();
    return 0;
}


