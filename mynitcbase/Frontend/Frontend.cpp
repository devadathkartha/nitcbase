#include "Frontend.h"

#include <cstring>
#include <iostream>

// Frontend/Frontend.cpp

int Frontend::create_table(char relname[ATTR_SIZE], int no_attrs, 
                            char attributes[][ATTR_SIZE], int type_attrs[]) {
    return Schema::createRel(relname, no_attrs, attributes, type_attrs);
}

int Frontend::drop_table(char relname[ATTR_SIZE]) {
    return Schema::deleteRel(relname);
}

int Frontend::open_table(char relname[ATTR_SIZE]) {
  return Schema::openRel(relname);
}

int Frontend::close_table(char relname[ATTR_SIZE]) {
  return Schema::closeRel(relname);
}

int Frontend::alter_table_rename(char relname_from[ATTR_SIZE], char relname_to[ATTR_SIZE]) {
  return Schema::renameRel(relname_from, relname_to);
}

int Frontend::alter_table_rename_column(char relname[ATTR_SIZE], char attrname_from[ATTR_SIZE],
                                        char attrname_to[ATTR_SIZE]) {
  return Schema::renameAttr(relname, attrname_from, attrname_to);
}

int Frontend::create_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::createIndex
  return SUCCESS;
}

int Frontend::drop_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::dropIndex
  return SUCCESS;
}

int Frontend::insert_into_table_values(char relname[ATTR_SIZE], int attr_count, char attr_values[][ATTR_SIZE]) {
  return Algebra::insert(relname, attr_count, attr_values);
}

int Frontend::select_from_table(char relname_source[ATTR_SIZE],
                                char relname_target[ATTR_SIZE]) {

    // Direct call to Algebra::project (copy relation version)
    return Algebra::project(relname_source, relname_target);
}

int Frontend::select_attrlist_from_table(char relname_source[ATTR_SIZE],
                                         char relname_target[ATTR_SIZE],
                                         int attr_count,
                                         char attr_list[][ATTR_SIZE]) {

    // Direct call to Algebra::project (specific attributes version)
    return Algebra::project(relname_source, relname_target,
                            attr_count, attr_list);
}

int Frontend::select_from_table_where(char relname_source[ATTR_SIZE],
                                      char relname_target[ATTR_SIZE],
                                      char attribute[ATTR_SIZE],
                                      int op, char value[ATTR_SIZE]) {

    // Direct call to Algebra::select
    return Algebra::select(relname_source, relname_target,
                           attribute, op, value);
}

int Frontend::select_attrlist_from_table_where(
    char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
    int attr_count, char attr_list[][ATTR_SIZE],
    char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE]) {

    // Step 1: Select matching rows from source into .temp
    // .temp will have all columns of source, only rows matching condition
    int ret = Algebra::select(relname_source, TEMP, attribute, op, value);

    if (ret != SUCCESS) {
        return ret;
    }

    // Step 2: Open .temp so Algebra::project() can read from it
    int tempRelId = OpenRelTable::openRel((char*)TEMP);

    if (tempRelId < 0) {
        // Failed to open .temp — clean up and return error
        Schema::deleteRel(TEMP);
        return tempRelId;
    }

    // Step 3: Project only the requested columns from .temp into target
    ret = Algebra::project(TEMP, relname_target, attr_count, attr_list);

    // Step 4: Close and delete .temp regardless of project() result
    OpenRelTable::closeRel(tempRelId);
    Schema::deleteRel(TEMP);

    // Step 5: Return result of project() — SUCCESS or error
    return ret;
}

int Frontend::select_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                     char relname_target[ATTR_SIZE],
                                     char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE]) {
  // Algebra::join
  return SUCCESS;
}

int Frontend::select_attrlist_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                              char relname_target[ATTR_SIZE],
                                              char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE],
                                              int attr_count, char attr_list[][ATTR_SIZE]) {
  // Algebra::join + project
  return SUCCESS;
}

int Frontend::custom_function(int argc, char argv[][ATTR_SIZE]) {
  // argc gives the size of the argv array
  // argv stores every token delimited by space and comma

  // implement whatever you desire

  return SUCCESS;
}