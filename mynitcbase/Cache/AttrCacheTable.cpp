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