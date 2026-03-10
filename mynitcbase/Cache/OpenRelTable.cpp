#include "OpenRelTable.h"

#include <cstring>
#include<cstdlib>

OpenRelTable::OpenRelTable() {

  // Initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }

  /************ Setting up Relation Cache entries ************/
  
  // 1. Create a buffer object for Block 4 (RELCAT_BLOCK)
  RecBuffer relCatBlock(RELCAT_BLOCK);

  // 2. Read Slot 0 (RELCAT_SLOTNUM_FOR_RELCAT)
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  // 3. Create a temporary struct and translate the record into it
  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  
  // 4. Fill in the runtime metadata (where did we find this record?)
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // 5. THE CRITICAL STEP: Memory Allocation
  // We use `malloc` to create memory on the "Heap". If we just pointed to our temporary 
  // `relCacheEntry` variable, it would be destroyed as soon as the constructor finishes!
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  
  // 6. Copy the data into the newly allocated heap memory
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;


  /**** Setting up Attribute Catalog relation in the Relation Cache Table ****/

  // 1. Read Slot 1 (RELCAT_SLOTNUM_FOR_ATTRCAT) from the same block
  Attribute attrCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(attrCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

  // 2. Translate into a temporary struct
  struct RelCacheEntry attrCacheEntry_Rel;
  RelCacheTable::recordToRelCatEntry(attrCatRecord, &attrCacheEntry_Rel.relCatEntry);
  
  // 3. Fill in the runtime metadata
  attrCacheEntry_Rel.recId.block = RELCAT_BLOCK;
  attrCacheEntry_Rel.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  // 4. Allocate memory on the heap for Rel-id 1
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  
  // 5. Copy the data
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = attrCacheEntry_Rel;

  /************ Setting up Attribute cache entries ************/

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  
  // 1. Open Block 5 (ATTRCAT_BLOCK)
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrRecord[ATTRCAT_NO_ATTRS];

  // Variables to help us build the linked list
  struct AttrCacheEntry *head = nullptr;
  struct AttrCacheEntry *current = nullptr;

  // 2. Iterate through slots 0 to 5
  for (int i = 0; i <= 5; i++) {
      // Read the record from the disk buffer
      attrCatBlock.getRecord(attrRecord, i);

      // Allocate memory on the heap for the new linked list node
      struct AttrCacheEntry *newEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));

      // Translate the raw record into our C++ struct
      AttrCacheTable::recordToAttrCatEntry(attrRecord, &newEntry->attrCatEntry);
      
      // Set the runtime metadata (where we found it)
      newEntry->recId.block = ATTRCAT_BLOCK;
      newEntry->recId.slot = i;
      newEntry->next = nullptr;

      // 3. Add the new node to our linked list
      if (head == nullptr) {
          head = newEntry;      // First node becomes the head
          current = newEntry;
      } else {
          current->next = newEntry; // Link the previous node to this new one
          current = newEntry;       // Move the current pointer forward
      }
  }

  // 4. Store the head of the linked list in the cache array
  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  // Reset our linked list pointers
  head = nullptr;
  current = nullptr;

  // Read slots 6 to 11
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

  // Store the head in the cache array at rel-id 1
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;


  /************ Setting up Students relation for Exercise ************/

  // 1. Load the Students Relation into relCache[2]
  // Students is at slot 2 of the Relation Catalog
  relCatBlock.getRecord(relCatRecord, 2);

  struct RelCacheEntry *studentRelCacheEntry = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(relCatRecord, &studentRelCacheEntry->relCatEntry);
  studentRelCacheEntry->recId.block = RELCAT_BLOCK;
  studentRelCacheEntry->recId.slot = 2;

  RelCacheTable::relCache[2] = studentRelCacheEntry;

  // 2. Load the Students Attributes into attrCache[2]
  // Students has 4 attributes (slots 12, 13, 14, 15 in the Attribute Catalog)
  struct AttrCacheEntry *studentHead = nullptr;
  struct AttrCacheEntry *studentCurrent = nullptr;

  for (int i = 12; i <= 15; i++) {
      attrCatBlock.getRecord(attrRecord, i);
      
      struct AttrCacheEntry *newEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
      AttrCacheTable::recordToAttrCatEntry(attrRecord, &newEntry->attrCatEntry);
      
      newEntry->recId.block = ATTRCAT_BLOCK;
      newEntry->recId.slot = i;
      newEntry->next = nullptr;

      if (studentHead == nullptr) {
          studentHead = newEntry;
          studentCurrent = newEntry;
      } else {
          studentCurrent->next = newEntry;
          studentCurrent = newEntry;
      }
  }

  AttrCacheTable::attrCache[2] = studentHead;

} // <--- This should be the closing brace of the constructor

OpenRelTable::~OpenRelTable() {
  // Free all the memory that we allocated in the constructor
  for (int i = 0; i < MAX_OPEN; ++i) {
      
      // Free the Relation Cache entry
      if (RelCacheTable::relCache[i] != nullptr) {
          free(RelCacheTable::relCache[i]);
      }

      // Free the Attribute Cache linked list
      if (AttrCacheTable::attrCache[i] != nullptr) {
          AttrCacheEntry *current = AttrCacheTable::attrCache[i];
          AttrCacheEntry *next = nullptr;
          
          // Walk down the linked list and free each node
          while (current != nullptr) {
              next = current->next;
              free(current);
              current = next;
          }
      }
  }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  // 1. Check if the requested table is the Relation Catalog
  if (strcmp(relName, RELCAT_RELNAME) == 0) {
    return RELCAT_RELID; // Returns 0
  }
  
  // 2. Check if the requested table is the Attribute Catalog
  if (strcmp(relName, ATTRCAT_RELNAME) == 0) {
    return ATTRCAT_RELID; // Returns 1
  }

  if (strcmp(relName, "Students") == 0) {
    return 2; // We hardcoded the Students table to slot 2 in the constructor!
  }
  // 3. If it's anything else, tell the caller it isn't open right now!
  return E_RELNOTOPEN;
}