#include "OpenRelTable.h"
#include <cstring>
#include <cstdlib>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

  // Initialize relCache and attrCache with nullptr, all slots free
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
  }

  /************ Setting up Relation Cache entries ************/

  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  Attribute attrCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(attrCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

  struct RelCacheEntry attrCacheEntry_Rel;
  RelCacheTable::recordToRelCatEntry(attrCatRecord, &attrCacheEntry_Rel.relCatEntry);
  attrCacheEntry_Rel.recId.block = RELCAT_BLOCK;
  attrCacheEntry_Rel.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = attrCacheEntry_Rel;

  /************ Setting up Attribute Cache entries ************/

  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrRecord[ATTRCAT_NO_ATTRS];

  // RELCAT attributes: slots 0-5
  struct AttrCacheEntry *head = nullptr;
  struct AttrCacheEntry *current = nullptr;

  for (int i = 0; i <= 5; i++) {
    attrCatBlock.getRecord(attrRecord, i);

    struct AttrCacheEntry *newEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrRecord, &newEntry->attrCatEntry);
    newEntry->recId.block = ATTRCAT_BLOCK;
    newEntry->recId.slot = i;
    newEntry->next = nullptr;

    if (head == nullptr) {
      head = newEntry;
      current = newEntry;
    } else {
      current->next = newEntry;
      current = newEntry;
    }
  }
  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  // ATTRCAT attributes: slots 6-11
  head = nullptr;
  current = nullptr;

  for (int i = 6; i <= 11; i++) {
    attrCatBlock.getRecord(attrRecord, i);

    struct AttrCacheEntry *newEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrRecord, &newEntry->attrCatEntry);
    newEntry->recId.block = ATTRCAT_BLOCK;
    newEntry->recId.slot = i;
    newEntry->next = nullptr;

    if (head == nullptr) {
      head = newEntry;
      current = newEntry;
    } else {
      current->next = newEntry;
      current = newEntry;
    }
  }
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;

  /************ Setting up tableMetaInfo entries ************/

  tableMetaInfo[RELCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);

  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}

OpenRelTable::~OpenRelTable() {

  // Step 1: close all user-opened relations (index 2 onwards)
  for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i);
    }
  }

  // Step 2: manually free RELCAT (index 0) and ATTRCAT (index 1)
  for (int i = 0; i <= 1; i++) {

    // free relCache entryCLOSE TABLE Students;
    free(RelCacheTable::relCache[i]);
    RelCacheTable::relCache[i] = nullptr;

    // free attrCache linked list
    AttrCacheEntry *curr = AttrCacheTable::attrCache[i];
    while (curr != nullptr) {
      AttrCacheEntry *next = curr->next;
      free(curr);
      curr = next;
    }
    AttrCacheTable::attrCache[i] = nullptr;
  }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  // Check for RELCAT
  if (strcmp(relName, RELCAT_RELNAME) == 0) {
    return RELCAT_RELID;
  }

  // Check for ATTRCAT
  if (strcmp(relName, ATTRCAT_RELNAME) == 0) {
    return ATTRCAT_RELID;
  }

  // Search tableMetaInfo for any occupied slot with matching relName
  for (int i = 2; i < MAX_OPEN; i++) {
    if (!tableMetaInfo[i].free && strcmp(tableMetaInfo[i].relName, relName) == 0) {
      return i;
    }
  }

  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {

    // Loop from index 2 onwards (0 and 1 are always reserved for RELCAT and ATTRCAT)
    for (int i = 2; i < MAX_OPEN; i++) {
        if (tableMetaInfo[i].free) {
            return i;  // return the first free slot found
        }
    }

    return E_CACHEFULL;  // no free slot found
}


int OpenRelTable::openRel(char relName[ATTR_SIZE]) {

    // Step 1: check if relation is already open
    int relId = OpenRelTable::getRelId(relName);
    if (relId != E_RELNOTOPEN) {
        return relId;  // already open, return existing rel-id
    }

    // Step 2: find a free slot
    int freeSlot = OpenRelTable::getFreeOpenRelTableEntry();
    if (freeSlot == E_CACHEFULL) {
        return E_CACHEFULL;
    }

    /************ Step 3: Load relation catalog entry ************/

    // set up the attribute value to search for
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    // reset search index of RELCAT before searching
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // search RELCAT for the relation
    RecId relCatRecId = BlockAccess::linearSearch(
        RELCAT_RELID,
        (char*)"RelName",
        relNameAttr,
        EQ
    );

    // if not found, relation does not exist
    if (relCatRecId.block == -1 && relCatRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    // read the record from disk
    RecBuffer relCatBlock(relCatRecId.block);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, relCatRecId.slot);

    // allocate memory for the relation cache entry
    RelCacheEntry* relCacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));

    // convert the record to a RelCatEntry and store it
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry->relCatEntry);

    // store the record id so we know where this came from on disk
    relCacheEntry->recId.block = relCatRecId.block;
    relCacheEntry->recId.slot  = relCatRecId.slot;

    // initialise the search index to {-1, -1} (no search in progress)
    relCacheEntry->searchIndex = {-1, -1};

    // put it in the cache
    RelCacheTable::relCache[freeSlot] = relCacheEntry;

    /************ Step 4: Load attribute catalog entries ************/

    // reset search index of ATTRCAT before searching
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // we'll build a linked list of AttrCacheEntry nodes
    AttrCacheEntry* attrCacheHead    = nullptr;
    AttrCacheEntry* attrCacheCurrent = nullptr;

    // how many attributes does this relation have?
    int numAttrs = relCacheEntry->relCatEntry.numAttrs;

    // search ATTRCAT repeatedly — once per attribute
    for (int i = 0; i < numAttrs; i++) {

        // search for the next attribute belonging to this relation
        RecId attrCatRecId = BlockAccess::linearSearch(
            ATTRCAT_RELID,
            (char*)"RelName",
            relNameAttr,
            EQ
        );

        if (attrCatRecId.block == -1 && attrCatRecId.slot == -1) {
            break;
        }

        // read the attribute record from disk
        RecBuffer attrCatBlock(attrCatRecId.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrCatRecord, attrCatRecId.slot);

        // allocate a new linked list node
        AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));

        // convert the record and store it
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);

        // store where this record lives on disk
        attrCacheEntry->recId.block = attrCatRecId.block;
        attrCacheEntry->recId.slot  = attrCatRecId.slot;

        // initialise search index
        attrCacheEntry->searchIndex = {-1, -1};
        attrCacheEntry->next = nullptr;

        // append to the linked list
        if (attrCacheHead == nullptr) {
            attrCacheHead    = attrCacheEntry;
            attrCacheCurrent = attrCacheEntry;
        } else {
            attrCacheCurrent->next = attrCacheEntry;
            attrCacheCurrent       = attrCacheEntry;
        }
    }

    // store the linked list head in the cache
    AttrCacheTable::attrCache[freeSlot] = attrCacheHead;

    /************ Step 5: Update tableMetaInfo ************/

    tableMetaInfo[freeSlot].free = false;
    strcpy(tableMetaInfo[freeSlot].relName, relName);

    return freeSlot;  // return the rel-id
}

int OpenRelTable::closeRel(int relId) {

    // Step 1: prevent closing RELCAT or ATTRCAT
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    // Step 2: check relId is within valid range
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Step 3: check the slot is actually occupied
    if (tableMetaInfo[relId].free) {
        return E_RELNOTOPEN;
    }

    /************ Step 4: Free the relation cache entry ************/

    free(RelCacheTable::relCache[relId]);
    RelCacheTable::relCache[relId] = nullptr;

    /************ Step 5: Free the attribute cache linked list ************/

    AttrCacheEntry* curr = AttrCacheTable::attrCache[relId];
    while (curr != nullptr) {
        AttrCacheEntry* next = curr->next;
        free(curr);
        curr = next;
    }
    AttrCacheTable::attrCache[relId] = nullptr;

    /************ Step 6: Update tableMetaInfo ************/

    tableMetaInfo[relId].free = true;
    // clear the relation name stored in this slot
    memset(tableMetaInfo[relId].relName, 0, ATTR_SIZE);

    return SUCCESS;
}