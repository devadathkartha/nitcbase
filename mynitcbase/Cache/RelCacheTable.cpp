#include "RelCacheTable.h"

#include <cstring>

// Allocate memory for the 12 pointers
RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];

/* Converts a relation catalog record to a RelCatEntry struct */
void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],
                                        RelCatEntry* relCatEntry) {
                                            
  // 1. Copy the string value (Table Name)
  strcpy(relCatEntry->relName, record[RELCAT_REL_NAME_INDEX].sVal);
  
  // 2. Copy the integer values
  // We cast the floats (.nVal) to integers because metadata like "number of attributes" is always a whole number.
  relCatEntry->numAttrs = (int)record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
  relCatEntry->numRecs = (int)record[RELCAT_NO_RECORDS_INDEX].nVal;
  relCatEntry->firstBlk = (int)record[RELCAT_FIRST_BLOCK_INDEX].nVal;
  relCatEntry->lastBlk = (int)record[RELCAT_LAST_BLOCK_INDEX].nVal;
  relCatEntry->numSlotsPerBlk = (int)record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
}

/* Get the relation catalog entry for the relation with rel-id `relId` */
int RelCacheTable::getRelCatEntry(int relId, RelCatEntry* relCatBuf) {
  // 1. Sanity Check: Is the relId between 0 and 11?
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // 2. Is this table actually open? 
  // If the pointer is null, there is no cheat sheet here!
  if (relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // 3. Success! Copy the cached struct into the caller's variable.
  // We use `->relCatEntry` because relCache stores a larger wrapper struct (RelCacheEntry) 
  // which contains the relCatEntry alongside other runtime info (like exact block/slot location).
  *relCatBuf = relCache[relId]->relCatEntry;

  return SUCCESS;
}