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


/* Returns the searchIndex for the relation corresponding to `relId` */
int RelCacheTable::getSearchIndex(int relId, RecId* searchIndex) {
  // Check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // Check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
  if (relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // Copy the searchIndex field of the Relation Cache entry 
  // corresponding to input relId to the searchIndex variable.
  *searchIndex = relCache[relId]->searchIndex;
  
  return SUCCESS;
}

/* Sets the searchIndex for the relation corresponding to relId */
int RelCacheTable::setSearchIndex(int relId, RecId* searchIndex) {
  // Check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // Check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
  if (relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // Update the searchIndex value in the relCache for the relId 
  // to the searchIndex argument
  relCache[relId]->searchIndex = *searchIndex;

  return SUCCESS;
}

/* Resets the search index to {-1, -1} */
int RelCacheTable::resetSearchIndex(int relId) {
  // Use setSearchIndex to set the search index to {-1, -1}
  RecId resetIndex = {-1, -1};
  return setSearchIndex(relId, &resetIndex);
}

// Cache/RelCacheTable.cpp

int RelCacheTable::setRelCatEntry(int relId, RelCatEntry *relCatBuf) {

    // Check if relId is within valid range [0, MAX_OPEN-1]
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Check if this slot in the cache is actually in use
    // A free slot means no relation is open at this relId
    if (relCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    // Copy the input RelCatEntry into the cache
    // This updates the in-memory copy of the relation's catalog entry
    relCache[relId]->relCatEntry = *relCatBuf;

    // Mark this cache entry as dirty
    // This tells closeRel() that it needs to write this back to disk
    relCache[relId]->dirty = true;

    return SUCCESS;
}

void RelCacheTable::relCatEntryToRecord(RelCatEntry *relCatEntry, 
                                         union Attribute record[RELCAT_NO_ATTRS]) {
    // RELATIONCAT has 6 attributes, in this exact order:
    // [0] RelName      → string
    // [1] #Attributes  → number
    // [2] #Records     → number
    // [3] FirstBlock   → number
    // [4] LastBlock    → number
    // [5] #Slots       → number

    // Copy relation name into record[0] as a string
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, relCatEntry->relName);

    // Copy number of attributes into record[1] as a number
    record[RELCAT_NO_ATTRIBUTES_INDEX].nVal = (double)relCatEntry->numAttrs;

    // Copy number of records into record[2] as a number
    record[RELCAT_NO_RECORDS_INDEX].nVal = (double)relCatEntry->numRecs;

    // Copy first block number into record[3] as a number
    record[RELCAT_FIRST_BLOCK_INDEX].nVal = (double)relCatEntry->firstBlk;

    // Copy last block number into record[4] as a number
    record[RELCAT_LAST_BLOCK_INDEX].nVal = (double)relCatEntry->lastBlk;

    // Copy slots per block into record[5] as a number
    record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = (double)relCatEntry->numSlotsPerBlk;
}