cmake_minimum_required(VERSION 3.21)
project(llp_lab1 C)

set(CMAKE_C_STANDARD 99)



add_executable(llp_lab1 main.c include/database_include.h include/file.h include/schema.h database_struct.c file.c table_struct.c schema.c struct/database_struct.h struct/table_struct.h struct/query.h include/table_include.h include/test.h include/data_types.h data_types.c test.c)
