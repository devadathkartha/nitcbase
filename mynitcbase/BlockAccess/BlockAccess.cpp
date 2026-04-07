#include "BlockAccess.h"
#include <iostream>

#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    
    // 1. Get the relation catalog entry to find the first block
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    // 2. Get the attribute catalog entry to find the column offset and type
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // 3. Get the current bookmark (searchIndex)
    RecId searchIndex;
    RelCacheTable::getSearchIndex(relId, &searchIndex);

    /* * 4. Determine exactly where to start looking.
     * We need two variables: `block` and `slot` to track our current position.
     */
    int block, slot;

    if (searchIndex.block == -1 && searchIndex.slot == -1) {
        // If the bookmark is empty, start from the absolute beginning
        block = relCatEntry.firstBlk;
        slot = 0;
    } else {
        // If we have a bookmark, resume from the exact same block
        block = searchIndex.block;
        // BUT, we must start at the NEXT slot so we don't return the same record forever!
        slot = searchIndex.slot + 1;
    }

    
    /* =========================================================
       THE SEARCH ENGINE LOOP
       ========================================================= */
       
    // The loop continues as long as we haven't hit the end of the table (block == -1)
    while (block != -1) {
        
        // 1. Load the current block into a buffer object
        RecBuffer recBuffer(block);

        // 2. Read the header to find out how many slots are here, and what the next block is
        struct HeadInfo head;
        recBuffer.getHeader(&head);

        // 3. Read the "seating chart" (slotmap) so we don't read garbage data
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        /* 4. The Inner Loop: Check every slot in this block.
         * Notice we don't initialize `int slot = 0` in the loop condition! 
         * We use the `slot` variable we set up in Part 7, because if we are 
         * resuming from a bookmark, we might start at slot 15, not 0.
         */
        for (; slot < head.numSlots; slot++) {
            
            // If the seat is empty (deleted or never used), skip it!
            if (slotMap[slot] == SLOT_UNOCCUPIED) {
                continue;
            }

            // --- We found a VALID record! ---
            // (Inside the for loop, right after the slotMap check)

            // 1. Declare an array to hold the record and fetch it from the buffer
            union Attribute record[head.numAttrs];
            recBuffer.getRecord(record, slot);

            // 2. Extract the specific column we want to test using the offset
            union Attribute cmpAttr = record[attrCatEntry.offset];

            // 3. Use our referee function to compare the record's value with the target value
            int cmpVal = compareAttrs(cmpAttr, attrVal, attrCatEntry.attrType);

            // 4. Check if the result matches the SQL operator the user typed
            bool match = false;
            switch (op) {
                case NE: match = (cmpVal != 0); break; // Not Equal (!=)
                case LT: match = (cmpVal < 0); break;  // Less Than (<)
                case LE: match = (cmpVal <= 0); break; // Less Than or Equal (<=)
                case EQ: match = (cmpVal == 0); break; // Equal (=)
                case GT: match = (cmpVal > 0); break;  // Greater Than (>)
                case GE: match = (cmpVal >= 0); break; // Greater Than or Equal (>=)
            }

            // 5. BINGO! We found a record that satisfies the condition.
            if (match) {
                // Create a RecId marking this exact location
                RecId resultId = {block, slot};

                // CRITICAL: Save this location as our bookmark!
                // The next time linearSearch is called, it will resume from here.
                RelCacheTable::setSearchIndex(relId, &resultId);

                // Hand the winning location back to the caller
                return resultId;
            }
            
        } // End of inner slot loop
        
        // 5. We finished checking all slots in this block.
        // Move to the next block in the linked list.
        block = head.rblock;
        
        // CRITICAL: Reset the slot back to 0 for the *next* block!
        slot = 0; 
        
    } // End of outer block loop

    // 6. If the while loop finishes and we are down here, it means we 
    // searched the entire table and found absolutely nothing that matches.

    return RecId{-1, -1}; // Placeholder return
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    /************ Step 1: check if newName already exists ************/

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);

    // search RELCAT for newName
    RecId searchResult = linearSearch(
        RELCAT_RELID,
        (char*)"RelName",
        newRelationName,
        EQ
    );

    // if found, a relation with newName already exists
    if (searchResult.block != -1 && searchResult.slot != -1) {
        return E_RELEXIST;
    }

    /************ Step 2: check if oldName exists ************/

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);

    // search RELCAT for oldName
    searchResult = linearSearch(
        RELCAT_RELID,
        (char*)"RelName",
        oldRelationName,
        EQ
    );

    // if not found, relation with oldName doesn't exist
    if (searchResult.block == -1 && searchResult.slot == -1) {
        return E_RELNOTEXIST;
    }

    /************ Step 3: update the relation name in RELCAT ************/

    // searchResult now holds the RecId of oldName's entry in RELCAT
    RecBuffer relCatBlock(searchResult.block);

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, searchResult.slot);

    // update the RelName field with newName
    // RELCAT_REL_NAME_INDEX is the index of the RelName attribute in RELCAT
    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);

    // write the updated record back to the buffer
    relCatBlock.setRecord(relCatRecord, searchResult.slot);

    /************ Step 4: update all ATTRCAT entries for this relation ************/

    // reset ATTRCAT search index before iterating
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // get number of attributes from the updated RELCAT record
    int numAttrs = (int)relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    // iterate once per attribute
    for (int i = 0; i < numAttrs; i++) {

        // find the next ATTRCAT entry with RelName == oldName
        RecId attrCatRecId = linearSearch(
            ATTRCAT_RELID,
            (char*)"RelName",
            oldRelationName,    // still searching for oldName
            EQ
        );

        // read the attribute catalog record
        RecBuffer attrCatBlock(attrCatRecId.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrCatRecord, attrCatRecId.slot);

        // update the RelName field to newName
        // ATTRCAT_REL_NAME_INDEX is the index of RelName in ATTRCAT
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);

        // write back the updated record
        attrCatBlock.setRecord(attrCatRecord, attrCatRecId.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], 
                                  char newName[ATTR_SIZE]) {

    /************ Step 1: verify relation exists in RELCAT ************/

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    // search RELCAT for the relation
    RecId relCatSearch = linearSearch(
        RELCAT_RELID,
        (char*)"RelName",
        relNameAttr,
        EQ
    );

    // if relation doesn't exist
    if (relCatSearch.block == -1 && relCatSearch.slot == -1) {
        return E_RELNOTEXIST;
    }

    /************ Step 2: scan ATTRCAT for old and new attribute names ************/

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // will store the RecId of the attribute to rename
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    while (true) {

        // get next attribute entry for this relation
        RecId attrCatRecId = linearSearch(
            ATTRCAT_RELID,
            (char*)"RelName",
            relNameAttr,
            EQ
        );

        // no more attributes left to check
        if (attrCatRecId.block == -1 && attrCatRecId.slot == -1) {
            break;
        }

        // read the attribute catalog record
        RecBuffer attrCatBlock(attrCatRecId.block);
        attrCatBlock.getRecord(attrCatEntryRecord, attrCatRecId.slot);

        // get the attribute name from the record
        // ATTRCAT_ATTR_NAME_INDEX is the index of AttrName in ATTRCAT
        char attrName[ATTR_SIZE];
        strcpy(attrName, attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal);

        // if this is the attribute we want to rename — store its RecId
        if (strcmp(attrName, oldName) == 0) {
            attrToRenameRecId = attrCatRecId;
        }

        // if newName already exists as an attribute — conflict!
        if (strcmp(attrName, newName) == 0) {
            return E_ATTREXIST;
        }
    }

    /************ Step 3: check if oldName was found ************/

    if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1) {
        return E_ATTRNOTEXIST;
    }

    /************ Step 4: update the attribute name in ATTRCAT ************/

    RecBuffer attrCatBlock(attrToRenameRecId.block);
    Attribute attrRecord[ATTRCAT_NO_ATTRS];
    attrCatBlock.getRecord(attrRecord, attrToRenameRecId.slot);

    // update the AttrName field
    strcpy(attrRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);

    // write back the updated record
    attrCatBlock.setRecord(attrRecord, attrToRenameRecId.slot);

    return SUCCESS;
}