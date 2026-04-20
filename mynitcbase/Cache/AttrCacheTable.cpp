#include "AttrCacheTable.h"

#include <cstring>

// Allocate memory for the 12 linked list heads
AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

/* Converts an attribute catalog record to an AttrCatEntry struct */
void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],
                                          AttrCatEntry* attrCatEntry) {
                                              
  // 1. Copy the string values (Table Name and Column Name)
  strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
  strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);

  // 2. Copy the integer values (Type, PrimaryFlag, RootBlock, Offset)
  attrCatEntry->attrType = (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
  attrCatEntry->primaryFlag = (int)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
  attrCatEntry->rootBlock = (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
  attrCatEntry->offset = (int)record[ATTRCAT_OFFSET_INDEX].nVal;
}

/* Returns the attrOffset-th attribute for the relation corresponding to relId */
int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
  
  // 1. Sanity Check: Is the relId valid?
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // 2. Is this table open? (If the head of the linked list is null, it's not)
  if (attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // 3. Traverse the linked list
  for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
      
      // Did we find the exact column we are looking for?
      if (entry->attrCatEntry.offset == attrOffset) {
          
          // Copy the cached struct into the caller's variable
          *attrCatBuf = entry->attrCatEntry;
          return SUCCESS;
      }
  }

  // 4. If we reach the end of the list and didn't find it
  return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf) {

  // Check that relId is valid and corresponds to an open relation
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }
  if (attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // Iterate over the entries in the attribute cache linked list 
  // and set attrCatBuf to the entry that matches attrName
  for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
    if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
      *attrCatBuf = entry->attrCatEntry;
      return SUCCESS;
    }
  }

  // No attribute with name attrName found for the relation
  return E_ATTRNOTEXIST;
}

// Cache/AttrCacheTable.cpp

// ===== getSearchIndex (attrName version) =====
// Retrieves the current searchIndex for the named attribute

int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE],
                                    IndexId *searchIndex) {

    // Step 1: validate relId bounds
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Step 2: check if this relation is actually open
    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    // Step 3: traverse the linked list of attributes for this relation
    AttrCacheEntry *curr = attrCache[relId];
    while (curr != nullptr) {

        // compare attribute name (use strncmp or strcmp)
        if (strcmp(curr->attrCatEntry.attrName, attrName) == 0) {

            // found the attribute — copy its searchIndex to output
            *searchIndex = curr->searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    // attribute with given name not found in this relation
    return E_ATTRNOTEXIST;
}


// ===== getSearchIndex (attrOffset version) =====
// Retrieves the current searchIndex for the attribute at given offset

int AttrCacheTable::getSearchIndex(int relId, int attrOffset,
                                    IndexId *searchIndex) {

    // Step 1: validate relId bounds
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Step 2: check if this relation is actually open
    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    // Step 3: traverse the linked list to find the attribute at given offset
    AttrCacheEntry *curr = attrCache[relId];
    while (curr != nullptr) {

        if (curr->attrCatEntry.offset == attrOffset) {

            // found the attribute — copy its searchIndex to output
            *searchIndex = curr->searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}


// ===== setSearchIndex (attrName version) =====
// Updates the searchIndex for the named attribute

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE],
                                    IndexId *searchIndex) {

    // Step 1: validate relId bounds
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Step 2: check if this relation is actually open
    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    // Step 3: traverse the linked list to find the attribute
    AttrCacheEntry *curr = attrCache[relId];
    while (curr != nullptr) {

        if (strcmp(curr->attrCatEntry.attrName, attrName) == 0) {

            // found the attribute — update its searchIndex from input
            curr->searchIndex = *searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}


// ===== setSearchIndex (attrOffset version) =====
// Updates the searchIndex for the attribute at given offset

int AttrCacheTable::setSearchIndex(int relId, int attrOffset,
                                    IndexId *searchIndex) {

    // Step 1: validate relId bounds
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Step 2: check if this relation is actually open
    if (attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    // Step 3: traverse the linked list to find the attribute
    AttrCacheEntry *curr = attrCache[relId];
    while (curr != nullptr) {

        if (curr->attrCatEntry.offset == attrOffset) {

            // found the attribute — update its searchIndex from input
            curr->searchIndex = *searchIndex;
            return SUCCESS;
        }
        curr = curr->next;
    }

    return E_ATTRNOTEXIST;
}


// ===== resetSearchIndex (attrName version) =====
// Resets the searchIndex to {-1, -1} for the named attribute
// This forces bPlusSearch() to start from the root next time

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE]) {

    // create an IndexId with sentinel value {-1, -1}
    IndexId resetValue = {-1, -1};

    // delegate to setSearchIndex — reuse all the validation logic
    return AttrCacheTable::setSearchIndex(relId, attrName, &resetValue);
}


// ===== resetSearchIndex (attrOffset version) =====
// Resets the searchIndex to {-1, -1} for the attribute at given offset

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset) {

    // create an IndexId with sentinel value {-1, -1}
    IndexId resetValue = {-1, -1};

    // delegate to setSearchIndex
    return AttrCacheTable::setSearchIndex(relId, attrOffset, &resetValue);
}