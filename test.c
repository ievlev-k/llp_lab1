#include "include/test.h"
#include <time.h>
#include <stdio.h>

#include <locale.h>


void size_test( ){

    struct database* db = db_get("db5.db", NEW);
    struct schema* test_schema = schema_create();



    test_schema = schema_add_column(test_schema,"id", INTEGER);
    test_schema = schema_add_column_varchar(test_schema, "name", VARCHAR, 20);
    test_schema = schema_add_column(test_schema, "is_free", BOOLEAN);
    test_schema = schema_add_column(test_schema, "height", DOUBLE);
    test_schema = schema_add_column(test_schema, "post_id", INTEGER);

    struct table* table = table_create(test_schema, "test", db );

    struct row* row_test = row_create(table);
    char* names[5] ={"Petya", "Petr", "Petrucha", "Petrosyan", "Petych"};
    bool is_frees[2] = {true, false};
    double height[4] = {180.1, 175.5, 192.6, 168.1};
    int32_t post_id[6] = {1,2,3,4,5,6};


    clock_t  start;
    clock_t finish;
    for (size_t i = 1; i < 21; i++){
        start = clock();
        for (size_t j = 0 ; j < 1000; j ++){
            attribute_add(row_test, "id", INTEGER, (void *)&j);
            attribute_add(row_test, "name",VARCHAR,(void *) &names[i%5]);
            attribute_add(row_test, "is_free", BOOLEAN, (void * ) &is_frees[i%2]);
            attribute_add(row_test, "height", DOUBLE, (void *) &height[i%4]);
            attribute_add(row_test, "post_id", INTEGER, (void *) & post_id[i%6]);
            row_insert(row_test);
        }
        finish = clock();
        long int file_size =  database_get_size(db->source_file);
        double work_time = (double ) (finish - start)/CLOCKS_PER_SEC;
        printf("For %d rows file size is %d bytes\n",1000 *(i) ,file_size);



    }

    row_close(row_test);
    table_close(table);
    schema_close(test_schema);
    db_close(db);


}



void delete_test(){
    struct database* db1 = db_get("db3.db", NEW);
    struct schema* test_schema = schema_create();



    test_schema = schema_add_column(test_schema,"id", INTEGER);
    test_schema = schema_add_column_varchar(test_schema, "name", VARCHAR, 20);
    test_schema = schema_add_column(test_schema, "is_free", BOOLEAN);
    test_schema = schema_add_column(test_schema, "height", DOUBLE);
    test_schema = schema_add_column(test_schema, "post_id", INTEGER);

    struct table* table = table_create(test_schema, "test", db1 );

    struct row* row_test = row_create(table);
    char* names[5] = {"Petya", "Petr", "Petrucha", "Petrosyan", "Petych"};
    bool is_frees[2] = {true, false};
    double height[4] = {180.1, 175.5, 192.6, 168.1};
    int32_t post_id[6] = {1,2,3,4,5,6};

    char* query_val = "Petya";
    char* column_name[1] = {"name"};
    void* value[1] = {&query_val};


    struct query* query = query_make(DELETE, table, column_name, value, -1);
    clock_t  start;
    clock_t finish;
    for (size_t i = 0; i < 20; i++){

        for (size_t j = 0 ; j < 1000; j ++){
            attribute_add(row_test, "id", INTEGER, (void *)&j);
            attribute_add(row_test, "name",VARCHAR,(void *) &names[i%5]);
            attribute_add(row_test, "is_free", BOOLEAN, (void * ) &is_frees[i%2]);
            attribute_add(row_test, "height", DOUBLE, (void *) &height[i%4]);
            attribute_add(row_test, "post_id", INTEGER, (void *) & post_id[i%6]);
            row_insert(row_test);
        }
        start = clock();
        query_execute(query, false);
        finish = clock();


        double work_time = (double ) (finish - start)/CLOCKS_PER_SEC;

        printf("DELETE from %d rows took %f sec\n", 1000*(i+1), work_time);


    }
    query_close(query);
    db_close(db1);
    row_close(row_test);
    table_close(table);
    schema_close(test_schema);

}

void update_test(){
    struct database* db1 = db_get("db2.db", NEW);
    struct schema* test_schema = schema_create();



    test_schema = schema_add_column(test_schema,"id", INTEGER);
    test_schema = schema_add_column_varchar(test_schema, "name", VARCHAR, 20);
    test_schema = schema_add_column(test_schema, "is_free", BOOLEAN);
    test_schema = schema_add_column(test_schema, "height", DOUBLE);
    test_schema = schema_add_column(test_schema, "post_id", INTEGER);

    struct table* table = table_create(test_schema, "test", db1 );

    struct row* row_test = row_create(table);
    char* names[5] = {"Petya", "Petr", "Petrucha", "Petrosyan", "Petych"};
    bool is_frees[2] = {true, false};
    double height[4] = {180.1, 175.5, 192.6, 168.1};
    int32_t post_id[6] = {1,2,3,4,5,6};


    char* name = "Petya";
    double double_set = 12.52;
    char* column_name[2] = {"name", "height"};
    void* value[2] = {&name, &double_set};


    struct query* query = query_make(UPDATE, table, column_name, value, -1);
    clock_t  start;
    clock_t finish;
    for (size_t i = 0; i < 11; i++){

        for (size_t j = 0 ; j < 10000; j ++){
            attribute_add(row_test, "id", INTEGER, (void *)&j);
            attribute_add(row_test, "name",VARCHAR,(void *) &names[i%5]);
            attribute_add(row_test, "is_free", BOOLEAN, (void * ) &is_frees[i%2]);
            attribute_add(row_test, "height", DOUBLE, (void *) &height[i%4]);
            attribute_add(row_test, "post_id", INTEGER, (void *) & post_id[i%6]);
            row_insert(row_test);
        }
        start = clock();
        query_execute(query, false);
        finish = clock();


        double work_time = (double ) (finish - start)/CLOCKS_PER_SEC;

        printf("UPDATE from %d rows took %f sec\n", 1000*(i+1), work_time);


    }
    db_close(db1);
    row_close(row_test);
    table_close(table);
    schema_close(test_schema);
}


void insert_test( ){

    struct database* db = db_get("db.db", NEW);
    struct schema* test_schema = schema_create();

    test_schema = schema_add_column(test_schema,"id", INTEGER);
    test_schema = schema_add_column_varchar(test_schema, "name", VARCHAR, 20);
    test_schema = schema_add_column(test_schema, "is_free", BOOLEAN);
    test_schema = schema_add_column(test_schema, "height", DOUBLE);
    test_schema = schema_add_column(test_schema, "post_id", INTEGER);

    struct table* table = table_create(test_schema, "test", db );

    struct row* row_test = row_create(table);
    char* names[5] = {"Петя", "Петр", "Петруха", "Петросян", "Петух"};
    bool is_frees[2] = {true, false};
    double height[4] = {180.1, 175.5, 192.6, 168.1};
    int32_t post_id[6] = {1,2,3,4,5,6};


    clock_t  start;
    clock_t finish;
    for (size_t i = 1; i < 21; i++){
        start = clock();
        for (size_t j = 0 ; j < 1000; j ++){
            attribute_add(row_test, "id", INTEGER, (void *)&j);
            attribute_add(row_test, "name",VARCHAR,(void *) &names[i%5]);
            attribute_add(row_test, "is_free", BOOLEAN, (void * ) &is_frees[i%2]);
            attribute_add(row_test, "height", DOUBLE, (void *) &height[i%4]);
            attribute_add(row_test, "post_id", INTEGER, (void *) & post_id[i%6]);
            row_insert(row_test);
        }
        finish = clock();
        double work_time = (double ) (finish - start)/CLOCKS_PER_SEC;

        printf("INSERT %d row in %f sec\n", 1000*(i), work_time);


    }

    row_close(row_test);
    table_close(table);
    schema_close(test_schema);
    db_close(db);


}

void select_test(){
    struct database* db1 = db_get("db1.db", NEW);
    struct schema* test_schema = schema_create();



    test_schema = schema_add_column(test_schema,"id", INTEGER);
    test_schema = schema_add_column_varchar(test_schema, "name", VARCHAR, 20);
    test_schema = schema_add_column(test_schema, "is_free", BOOLEAN);
    test_schema = schema_add_column(test_schema, "height", DOUBLE);
    test_schema = schema_add_column(test_schema, "post_id", INTEGER);

    struct table* table = table_create(test_schema, "test", db1 );

    struct row* row_test = row_create(table);
    char* names[5] = {"Petya", "Petr", "Petrucha", "Petrosyan", "Petych"};
    bool is_frees[2] = {true, false};
    double height[4] = {180.1, 175.5, 192.6, 168.1};
    int32_t post_id[6] = {1,2,3,4,5,6};

    char* query_val = "Petya";
    char* column_name[1] = {"name"};
    void* value[1] = {&query_val};


    struct query* query = query_make(SELECT, table, column_name, value, -1);
    clock_t  start;
    clock_t finish;
    for (size_t i = 0; i < 20; i++){

        for (size_t j = 0 ; j < 1000; j ++){
            attribute_add(row_test, "id", INTEGER, (void *)&j);
            attribute_add(row_test, "name",VARCHAR,(void *) &names[i%5]);
            attribute_add(row_test, "is_free", BOOLEAN, (void * ) &is_frees[i%2]);
            attribute_add(row_test, "height", DOUBLE, (void *) &height[i%4]);
            attribute_add(row_test, "post_id", INTEGER, (void *) & post_id[i%6]);
            row_insert(row_test);
        }
        start = clock();
        query_execute(query, false);
        finish = clock();


        double work_time = (double ) (finish - start)/CLOCKS_PER_SEC;

        printf("SELECT from %d rows took %f sec\n", 1000*(i+1), work_time);


    }
    query_close(query);
    db_close(db1);
    row_close(row_test);
    table_close(table);
    schema_close(test_schema);

}


void join_test(){
    struct database* db = db_get("db5.db", NEW);
    struct schema* test_schema = schema_create();




    test_schema = schema_add_column_varchar(test_schema, "name", VARCHAR, 20);
    test_schema = schema_add_column(test_schema, "is_free", BOOLEAN);
    test_schema = schema_add_column(test_schema, "height", DOUBLE);
    test_schema = schema_add_column(test_schema, "post_id", INTEGER);

    struct table* table = table_create(test_schema, "test1", db );

    struct row* row_test = row_create(table);
    char* names[5] = {"Petya", "Petr", "Petrucha", "Petrosyan", "Petych"};
    bool is_frees[2] = {true, false};
    double height[4] = {180.1, 175.5, 192.6, 168.1};
    int32_t post_id[6] = {1,2,3,4,5,6};






    struct schema* test_schema_name_post = schema_create();

    test_schema_name_post  = schema_add_column( test_schema_name_post,"id", INTEGER);
    test_schema_name_post  = schema_add_column_varchar( test_schema_name_post, "name_post", VARCHAR, 20);


    struct table* table_post = table_create(test_schema_name_post , "test_post", db );

    char* posts[6] = {"job1","job2","job3","job4","job5","job6"};
    struct row* row_test1 = row_create(table_post);
    for (int i = 1; i <= 6; ++i) {
        attribute_add(row_test1, "id", INTEGER, (void * )&i);
        attribute_add(row_test1, "name_post", VARCHAR, (void*) &posts[i - 1] );
        row_insert(row_test1);
    }

    struct query_join* join = query_join_make(table, table_post, "post_id", "id");
    clock_t  start;
    clock_t finish;
    for (size_t j=0; j<11; j++) {
        for (size_t i=0; i<100 * j; i++) {
            attribute_add(row_test, "name",VARCHAR,(void *) &names[i%5]);
            attribute_add(row_test, "is_free", BOOLEAN, (void * ) &is_frees[i%2]);
            attribute_add(row_test, "height", DOUBLE, (void *) &height[i%4]);
            attribute_add(row_test, "post_id", INTEGER, (void *) & post_id[i%6]);
            row_insert(row_test);
        }


        start = clock();
        query_join_execute(join);
        finish = clock();
        double work_time = (double ) (finish - start)/CLOCKS_PER_SEC;
        printf("JOIN from %d rows took %f sec\n", 1000*(j+1), work_time);

    }





    query_join_close(join);

    row_close(row_test);
    table_close(table);
    schema_close(test_schema);
    row_close(row_test1);
    table_close(table_post);
    schema_close(test_schema);
    db_close(db);


}