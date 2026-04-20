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

    // Task 1: Close all user-opened relations (slots 2 to MAX_OPEN-1)
    // Slots 0 and 1 are RELCAT and ATTRCAT — handled separately below
    for (int i = 2; i < MAX_OPEN; i++) {

        // Check if this slot is occupied (a relation is open here)
        if (tableMetaInfo[i].free == false) {

            // Close it — this writes back its cache entries to disk
            // and frees the cache memory for that relation
            OpenRelTable::closeRel(i);
        }
    }

    /*---------------------------------------------------------
      Task 2: Write back ATTRCAT's RelCache entry (relId = 1)
    ----------------------------------------------------------*/

    // Check if ATTRCAT's relation cache entry was modified
    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true) {

        // Convert the in-memory RelCatEntry back to a disk record (array of Attributes)
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(
            &RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry,
            relCatRecord
        );

        // Get the RecId — where on disk is ATTRCAT's entry in RELCAT?
        RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;

        // Create a RecBuffer for that block and write the record back
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(relCatRecord, recId.slot);
    }

    // Free the dynamically allocated RelCacheEntry for ATTRCAT
    delete RelCacheTable::relCache[ATTRCAT_RELID];


    /*---------------------------------------------------------
      Task 3: Write back RELCAT's RelCache entry (relId = 0)
    ----------------------------------------------------------*/

    // Check if RELCAT's relation cache entry was modified
    if (RelCacheTable::relCache[RELCAT_RELID]->dirty == true) {

        // Convert the in-memory RelCatEntry back to a disk record
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(
            &RelCacheTable::relCache[RELCAT_RELID]->relCatEntry,
            relCatRecord
        );

        // Get the RecId — where on disk is RELCAT's own entry in RELCAT?
        RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;

        // Create a RecBuffer for that block and write the record back
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(relCatRecord, recId.slot);
    }

    // Free the dynamically allocated RelCacheEntry for RELCAT
    delete RelCacheTable::relCache[RELCAT_RELID];


    /*---------------------------------------------------------
      Task 4: Free AttrCache entries for RELCAT and ATTRCAT
    ----------------------------------------------------------*/

    // Free AttrCache linked list for RELCAT (relId = 0)
    AttrCacheEntry* attrCacheEntry = AttrCacheTable::attrCache[RELCAT_RELID];
    while (attrCacheEntry != nullptr) {
        AttrCacheEntry* next = attrCacheEntry->next;
        delete attrCacheEntry;
        attrCacheEntry = next;
    }

    // Free AttrCache linked list for ATTRCAT (relId = 1)
    attrCacheEntry = AttrCacheTable::attrCache[ATTRCAT_RELID];
    while (attrCacheEntry != nullptr) {
        AttrCacheEntry* next = attrCacheEntry->next;
        delete attrCacheEntry;
        attrCacheEntry
        
        = next;
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

// Cache/OpenRelTable.cpp

int OpenRelTable::closeRel(int relId) {

    // Guard 1: Cannot close the catalog relations mid-session
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    // Guard 2: relId must be in valid range
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Guard 3: The relation must actually be open
    if (tableMetaInfo[relId].free == true) {
        return E_RELNOTOPEN;
    }

    /****** Releasing the Relation Cache entry ******/

    if (RelCacheTable::relCache[relId]->dirty == true) {

        // Get the RelCatEntry from cache
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        // Get the recId (where this entry lives on disk)
        RecId recId = RelCacheTable::relCache[relId]->recId;

        // Convert RelCatEntry → record format
        union Attribute record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry, record);

        // Write back to disk buffer
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(record, recId.slot);
    }

    // Free the dynamically allocated RelCacheEntry
    delete RelCacheTable::relCache[relId];
    RelCacheTable::relCache[relId] = nullptr;

    /****** Releasing the Attribute Cache entry ******/
    //  THIS SECTION IS NEW IN STAGE 11

    // Traverse the linked list of AttrCacheEntry for this relation
    AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];

    while (curr != nullptr) {

        if (curr->dirty == true) {

            // Get the recId of this attribute catalog entry on disk
            RecId recId = curr->recId;

            // Convert AttrCatEntry → record format using our new function
            union Attribute record[ATTRCAT_NO_ATTRS];
            AttrCacheTable::attrCatEntryToRecord(&curr->attrCatEntry, record);

            // Write back to the attribute catalog block on disk
            RecBuffer attrCatBlock(recId.block);
            attrCatBlock.setRecord(record, recId.slot);
        }

        // Move to next node, then free current node
        AttrCacheEntry *next = curr->next;
        delete curr;
        curr = next;
    }

    // Set the head of the linked list to nullptr
    AttrCacheTable::attrCache[relId] = nullptr;

    /****** Update tableMetaInfo ******/

    // Mark this entry as free in the Open Relation Table
    tableMetaInfo[relId].free = true;
    strcpy(tableMetaInfo[relId].relName, "");

    return SUCCESS;
}