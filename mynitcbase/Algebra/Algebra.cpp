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

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  
  // 1. Get the relation ID of the source table
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTOPEN) {
    return E_RELNOTOPEN;
  }

  // 2. Get the metadata for the column we are filtering on (e.g., "Marks")
  AttrCatEntry attrCatEntry;
  int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
  if (ret != SUCCESS) {
    return ret; // Returns E_ATTRNOTEXIST if the column doesn't exist
  }

  // 3. Convert the user's string value into a proper `union Attribute`
  int type = attrCatEntry.attrType;
  Attribute attrVal;
  
  if (type == NUMBER) {
    // If the catalog says this column is a NUMBER, check if the user's string is a valid number
    if (isNumber(strVal)) {
      attrVal.nVal = atof(strVal); // atof converts string to float
    } else {
      return E_ATTRTYPEMISMATCH; // Error: User typed something like "Marks >= five"
    }
  } else if (type == STRING) {
    // If it's a STRING, just copy it directly
    strcpy(attrVal.sVal, strVal);
  }

  /*** Selecting records from the source relation ***/

  // 1. Reset the search bookmark to start from the very beginning
  RelCacheTable::resetSearchIndex(srcRelId);

  // 2. Get the relation catalog entry so we know how many columns to print
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

  /************************
  NOTE: The following code prints directly to the console. 
  In later stages, NITCbase requires us to save the output into a new TARGET table instead.
  For Stage 4, direct console output is perfect.
  ************************/

  // 3. Print the Header Row (Column Names)
  printf("|");
  for (int i = 0; i < relCatEntry.numAttrs; ++i) {
    AttrCatEntry attrCatCatEntry;
    // Get the column metadata at offset 'i'
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatCatEntry);
    printf(" %s |", attrCatCatEntry.attrName);
  }
  printf("\n");

  // 4. The Infinite Search Loop
  while (true) {
    
    // Ask our Block Access engine for the next matching record
    RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

    // Did we find a match?
    if (searchRes.block != -1 && searchRes.slot != -1) {

      // Yes! Let's fetch the actual record data from that exact block and slot
      RecBuffer recBuffer(searchRes.block);
      union Attribute record[relCatEntry.numAttrs];
      recBuffer.getRecord(record, searchRes.slot);

      // Print the values of this record in the same format as the header
      printf("|");
      for (int i = 0; i < relCatEntry.numAttrs; ++i) {
        AttrCatEntry attrCatCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatCatEntry);

        // Check if we should print a string or a number
        if (attrCatCatEntry.attrType == NUMBER) {
          // We cast to int here because NITCbase commonly uses round numbers for schema info
          printf(" %d |", (int)record[i].nVal); 
        } else {
          printf(" %s |", record[i].sVal);
        }
      }
      printf("\n");

    } else {
      // No more records match the condition. We are totally finished!
      break;
    }
  }

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