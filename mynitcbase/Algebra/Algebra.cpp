#include "Algebra.h"

#include <cstring>
#include <cstdio>
#include <cstdlib>

// Returns true if a string can be parsed as a floating point number
bool isNumber(char *str) {
  int len;
  float ignore;
  /*
    sscanf reads data from a string. 
    %f tries to read a float.
    %n gets the number of characters successfully read so far.
  */
  int ret = sscanf(str, "%f %n", &ignore, &len);
  
  // If it successfully read 1 float, and the number of characters read 
  // exactly matches the length of the string, then it's a valid number!
  return ret == 1 && len == strlen(str);
}

// Algebra/Algebra.cpp

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE],
                    char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {

    /*** STEP 1: Validate source relation ***/

    // get the rel-id of the source relation
    int srcRelId = OpenRelTable::getRelId(srcRel);

    // if source relation is not open, return error
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    /*** STEP 2: Validate attribute exists and get its type ***/

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

    // if attribute doesn't exist in source relation
    if (ret != SUCCESS) {
        return E_ATTRNOTEXIST;
    }

    /*** STEP 3: Convert strVal to appropriate Attribute type ***/

    Attribute attrVal;
    int type = attrCatEntry.attrType;

    if (type == NUMBER) {

        // check if the string can be parsed as a number
        if (isNumber(strVal)) {
            // convert string to double using atof()
            attrVal.nVal = atof(strVal);
        } else {
            // string is not a valid number but attribute is numeric
            return E_ATTRTYPEMISMATCH;
        }

    } else if (type == STRING) {
        // copy string value directly into attrVal
        strncpy(attrVal.sVal, strVal, ATTR_SIZE);
    }

    /*** STEP 4: Get source relation's schema info ***/

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    // number of attributes in source relation
    int src_nAttrs = relCatEntry.numAttrs;

    /*** STEP 5: Build attribute name and type arrays for createRel() ***/

    // these will hold the schema of the target relation
    // (same schema as source relation)
    char attr_names[src_nAttrs][ATTR_SIZE];
    int  attr_types[src_nAttrs];

    // fill arrays by reading each attribute's catalog entry
    for (int i = 0; i < src_nAttrs; i++) {

        AttrCatEntry ithAttrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &ithAttrCatEntry);

        // copy attribute name
        strncpy(attr_names[i], ithAttrCatEntry.attrName, ATTR_SIZE);

        // copy attribute type (NUMBER or STRING)
        attr_types[i] = ithAttrCatEntry.attrType;
    }

    /*** STEP 6: Create the target relation ***/

    ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);

    if (ret != SUCCESS) {
        // could be E_RELEXIST, E_DISKFULL, etc.
        return ret;
    }

    /*** STEP 7: Open the target relation ***/

    int targetRelId = OpenRelTable::openRel(targetRel);

    if (targetRelId < 0) {
        // opening failed — clean up by deleting the relation we just created
        Schema::deleteRel(targetRel);
        return targetRelId;  // return the error code from openRel
    }

    /*** STEP 8: Reset both search indexes before starting the loop ***/

    // reset relation cache search index (used by linearSearch)
    RelCacheTable::resetSearchIndex(srcRelId);

    // reset attribute cache search index (used by bPlusSearch)
    // this ensures B+ search starts from the root of the tree
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    /*** STEP 9: Search and insert loop ***/

    // buffer to hold each found record
    Attribute record[src_nAttrs];

    // keep searching until no more matching records
    while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) {

        // insert the found record into the target relation
        ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS) {
            // insert failed (e.g., disk full)
            // clean up: close and delete the partially filled target relation
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    /*** STEP 10: Close the target relation ***/

    Schema::closeRel(targetRel);

    return SUCCESS;
}
// Algebra/Algebra.cpp

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {

    // RELATIONCAT and ATTRIBUTECAT cannot be inserted into by the user
    // These are system catalog relations — protected from direct manipulation
    if (strcmp(relName, RELCAT_RELNAME) == 0 || 
        strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    // Get the relation's relId from the Open Relation Table
    // The relation must already be open for insertion to work
    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    // Get the relation's catalog entry from the cache
    // We need numAttrs to validate the input
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    // Check if number of attributes provided matches the relation's schema
    if (relCatEntry.numAttrs != nAttrs) {
        return E_NATTRMISMATCH;
    }

    // This array will hold the converted attribute values
    // record[][] is strings, but we need union Attribute for storage
    union Attribute recordValues[nAttrs];

    // Convert each string value to the correct type
    for (int i = 0; i < nAttrs; i++) {

        // Get the attribute catalog entry for the i-th attribute
        // This tells us whether the attribute is NUMBER or STRING
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);

        int type = attrCatEntry.attrType;

        if (type == NUMBER) {
            // Check if the string can actually be converted to a number
            // isNumber() returns true if the string is a valid number
            if (isNumber(record[i])) {
                // atof() converts string to double
                // e.g. "300" → 300.0, "3.14" → 3.14
                recordValues[i].nVal = atof(record[i]);
            } else {
                // User passed a string where a number was expected
                // e.g. INSERT INTO Locations VALUES (elhc, abc)
                //                                          ↑ not a number
                return E_ATTRTYPEMISMATCH;
            }
        } else if (type == STRING) {
            // Simply copy the string value into sVal
            // e.g. "elhc" → recordValues[i].sVal = "elhc"
            strcpy(recordValues[i].sVal, record[i]);
        }
    }

    // All validation passed, forward to Block Access Layer
    int retVal = BlockAccess::insert(relId, recordValues);

    return retVal;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

    // Step 1: Get srcRel's rel-id
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    // Step 2: Get source relation's catalog entry
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
    int numAttrs = relCatEntry.numAttrs;

    // Step 3: Collect attribute names and types from source
    char attrNames[numAttrs][ATTR_SIZE];
    int  attrTypes[numAttrs];

    for (int i = 0; i < numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        strcpy(attrNames[i], attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
    }

    // Step 4: Create the target relation with same schema
    int ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
    if (ret != SUCCESS) {
        return ret;    // E_RELEXIST or E_DISKFULL
    }

    // Step 5: Open the target relation
    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    // Step 6: Reset search index before starting the copy
    RelCacheTable::resetSearchIndex(srcRelId);

    // Step 7: Copy every record from source to target
    Attribute record[numAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS) {

        ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Step 8: Close the target relation
    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE],
                     int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {

    // Step 1: Get srcRel's rel-id
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    // Step 2: Get source relation's number of attributes
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
    int src_nAttrs = relCatEntry.numAttrs;

    // Step 3: Build attr_offset[] and attr_types[] for target attributes
    int attr_offset[tar_nAttrs];
    int attr_types[tar_nAttrs];

    for (int i = 0; i < tar_nAttrs; i++) {
        AttrCatEntry attrCatEntry;
        int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatEntry);

        if (ret != SUCCESS) {
            return E_ATTRNOTEXIST;
        }

        attr_offset[i] = attrCatEntry.offset;
        attr_types[i]  = attrCatEntry.attrType;
    }

    // Step 4: Create the target relation with only the requested attributes
    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
    if (ret != SUCCESS) {
        return ret;    // E_RELEXIST or E_DISKFULL
    }

    // Step 5: Open the target relation
    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    // Step 6: Reset search index before starting projection
    RelCacheTable::resetSearchIndex(srcRelId);

    // Step 7: Read each full record, project it, insert into target
    Attribute record[src_nAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS) {

        // Build the projected record using attr_offset[]
        Attribute proj_record[tar_nAttrs];

        for (int i = 0; i < tar_nAttrs; i++) {
            proj_record[i] = record[attr_offset[i]];
        }

        // Insert the projected record into target relation
        ret = BlockAccess::insert(targetRelId, proj_record);

        if (ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Step 8: Close the target relation
    Schema::closeRel(targetRel);

    return SUCCESS;
}