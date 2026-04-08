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

// BlockAccess/BlockAccess.cpp

int BlockAccess::insert(int relId, Attribute *record) {

    // Step 1: Get relation metadata from cache
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    // Starting point for traversal = first block of the relation
    int blockNum = relCatEntry.firstBlk;

    // rec_id will store WHERE we will insert the new record
    RecId rec_id = {-1, -1};

    int numOfSlots      = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;

    // prevBlockNum tracks the last block we visited
    // needed when we have to link a new block into the list
    int prevBlockNum = -1;

    /*
        Step 2: Traverse the linked list of record blocks
        looking for a block that has at least one free slot
    */
    while (blockNum != -1) {

        // Open this block using Constructor2 (block already exists)
        RecBuffer block(blockNum);

        // Read the header to get rblock (next block) and numEntries
        struct HeadInfo head;
        block.getHeader(&head);

        // Read the slot map to find free slots
        unsigned char slotMap[numOfSlots];
        block.getSlotMap(slotMap);

        // Scan the slot map for a free slot
        for (int i = 0; i < numOfSlots; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED) {
                // Found a free slot!
                rec_id.block = blockNum;
                rec_id.slot  = i;
                break;
            }
        }

        // If a free slot was found, stop traversing
        if (rec_id.slot != -1) {
            break;
        }

        // No free slot in this block, move to next block
        prevBlockNum = blockNum;
        blockNum     = head.rblock;  // follow the linked list
    }

    /*
        Step 3: If no free slot found in ANY existing block,
        allocate a brand new block
    */
    if (rec_id.block == -1 && rec_id.slot == -1) {

        // RELATIONCAT is special — it cannot grow beyond its initial block
        // Maximum relations = slots in RELCAT block = 20
        if (relId == RELCAT_RELID) {
            return E_MAXRELATIONS;
        }

        // Allocate a new record block using Constructor1
        RecBuffer newBlock;

        // Check if allocation succeeded
        int ret = newBlock.getBlockNum();
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        // New record will go into slot 0 of this new block
        rec_id.block = ret;
        rec_id.slot  = 0;

        /*
            Set up the header of the new block:
            - Link it to the previous last block via lblock
            - rblock = -1 (it's now the last block)
            - numEntries = 0 (empty)
            - numSlots and numAttrs from relation metadata
        */
        struct HeadInfo newHead;
        newHead.blockType  = REC;
        newHead.pblock     = -1;
        newHead.lblock     = prevBlockNum;  // -1 if first block ever
        newHead.rblock     = -1;            // nothing after this
        newHead.numEntries = 0;
        newHead.numAttrs   = numOfAttributes;
        newHead.numSlots   = numOfSlots;
        newBlock.setHeader(&newHead);

        // Initialize all slots as free in the new block
        unsigned char newSlotMap[numOfSlots];
        for (int i = 0; i < numOfSlots; i++) {
            newSlotMap[i] = SLOT_UNOCCUPIED;
        }
        newBlock.setSlotMap(newSlotMap);

        /*
            Link the new block into the existing linked list:

            Case 1: prevBlockNum != -1 (relation had blocks before)
            → update the previous last block's rblock to point to new block

            Case 2: prevBlockNum == -1 (relation had NO blocks before)
            → this is the very first block, update firstBlock in cache
        */
        if (prevBlockNum != -1) {
            // Update the old last block's rblock to point to new block
            RecBuffer prevBlock(prevBlockNum);
            struct HeadInfo prevHead;
            prevBlock.getHeader(&prevHead);
            prevHead.rblock = rec_id.block;
            prevBlock.setHeader(&prevHead);
        } else {
            // This is the first block ever for this relation
            // Update firstBlock in the relation cache
            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        // Either way, this new block is now the last block
        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    /*
        Step 4: Write the record into rec_id.slot of rec_id.block
    */
    RecBuffer insertBlock(rec_id.block);

    // Write the actual record data into the slot
    insertBlock.setRecord(record, rec_id.slot);

    /*
        Step 5: Update the slot map — mark this slot as occupied
    */
    unsigned char slotMap[numOfSlots];
    insertBlock.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    insertBlock.setSlotMap(slotMap);

    /*
        Step 6: Update the block header — increment numEntries
    */
    struct HeadInfo head;
    insertBlock.getHeader(&head);
    head.numEntries++;
    insertBlock.setHeader(&head);

    /*
        Step 7: Update the relation cache — increment numRecords
        This marks the cache entry as dirty (setRelCatEntry sets dirty=true)
        so it will be written back to disk when the relation is closed
    */
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    return SUCCESS;
}

// BlockAccess/BlockAccess.cpp

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {

    // ─────────────────────────────────────────────
    // TASK 1: Safety check — never delete catalogs
    // ─────────────────────────────────────────────

    if (strcmp(relName, RELCAT_RELNAME) == 0 ||
        strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }
    // Note: Schema::deleteRel() already checks this.
    // This is a second layer of protection at the Block Access layer.

    // ─────────────────────────────────────────────
    // TASK 2: Find the relation in RELATIONCAT
    // ─────────────────────────────────────────────

    // Reset search index so linearSearch starts from beginning of RELCAT
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    // Search RELATIONCAT for a record where RelName == relName
    RecId relCatRecId = BlockAccess::linearSearch(
        RELCAT_RELID,
        RELCAT_ATTR_RELNAME,   // "RelName"
        relNameAttr,
        EQ
    );

    // If not found, relation doesn't exist
    if (relCatRecId.block == -1 && relCatRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    // Read the actual RELATIONCAT record to get firstBlock and numAttrs
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

    RecBuffer relCatBlock(relCatRecId.block);
    // Create a RecBuffer object for the block containing the RELCAT entry
    
    relCatBlock.getRecord(relCatEntryRecord, relCatRecId.slot);
    // Read the record from that block at that slot

    // Extract firstBlock and numAttrs from the record
    int firstBlock = (int) relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttrs   = (int) relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    // ─────────────────────────────────────────────
    // TASK 3: Free ALL record blocks of the relation
    // ─────────────────────────────────────────────

    // The record blocks form a singly-traversable linked list
    // We follow rblock pointers until we reach -1

    int currentBlock = firstBlock;

    while (currentBlock != -1) {
        // Create a RecBuffer for the current block
        RecBuffer recBuffer(currentBlock);

        // Read the header to find the next block in the chain
        HeadInfo header;
        recBuffer.getHeader(&header);

        int nextBlock = header.rblock;
        // Save next block BEFORE releasing current block
        // (after release, we can't read the header anymore)

        // Release this block — marks it as UNUSED_BLK in blockAllocMap
        recBuffer.releaseBlock();

        // Move to the next block
        currentBlock = nextBlock;
    }
    // After this loop, all data blocks of "Stores" are freed
    // e.g. blocks 10, 11, 12 are now marked UNUSED_BLK

    // ─────────────────────────────────────────────
    // TASK 4: Delete ALL ATTRIBUTECAT entries for this relation
    // ─────────────────────────────────────────────

    // Reset search index so we start from beginning of ATTRCAT
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    int numberOfAttributesDeleted = 0;

    while (true) {

        // Search ATTRCAT for next entry where RelName == relName
        RecId attrCatRecId = BlockAccess::linearSearch(
            ATTRCAT_RELID,
            ATTRCAT_ATTR_RELNAME,   // "RelName"
            relNameAttr,
            EQ
        );

        // No more attribute entries found → done
        if (attrCatRecId.block == -1 && attrCatRecId.slot == -1) {
            break;
        }

        numberOfAttributesDeleted++;

        // ── Read the attribute catalog block and record ──

        RecBuffer attrCatBuffer(attrCatRecId.block);

        HeadInfo attrCatHeader;
        attrCatBuffer.getHeader(&attrCatHeader);

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRecord, attrCatRecId.slot);

        // Get root block for index (used later when indexing is implemented)
        int rootBlock = (int) attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        // rootBlock == -1 means no index exists for this attribute

        // ── Mark the slot as unoccupied in the slotmap ──

        unsigned char slotMap[attrCatHeader.numSlots];
        attrCatBuffer.getSlotMap(slotMap);

        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        // This logically "deletes" the record from the block

        attrCatBuffer.setSlotMap(slotMap);

        // ── Decrement numEntries in the block header ──

        attrCatHeader.numEntries--;
        attrCatBuffer.setHeader(&attrCatHeader);

        // ── If block is now completely empty, release it ──

        if (attrCatHeader.numEntries == 0) {

            // We need to fix the doubly linked list of ATTRCAT blocks
            // before releasing this block

            int lBlock = attrCatHeader.lblock;
            int rBlock = attrCatHeader.rblock;

            // Fix left neighbor: its rblock should now point to our rblock
            if (lBlock != -1) {
                RecBuffer leftBuffer(lBlock);
                HeadInfo leftHeader;
                leftBuffer.getHeader(&leftHeader);
                leftHeader.rblock = rBlock;
                leftBuffer.setHeader(&leftHeader);
            }

            // Fix right neighbor: its lblock should now point to our lblock
            if (rBlock != -1) {
                RecBuffer rightBuffer(rBlock);
                HeadInfo rightHeader;
                rightBuffer.getHeader(&rightHeader);
                rightHeader.lblock = lBlock;
                rightBuffer.setHeader(&rightHeader);
            } else {
                // This was the LAST block of ATTRCAT
                // Update ATTRCAT's entry in RELATIONCAT to reflect new lastBlock

                RelCatEntry attrCatRelEntry;
                RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatRelEntry);
                attrCatRelEntry.lastBlk = lBlock;
                RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatRelEntry);
            }

            // Now safe to release the empty block
            attrCatBuffer.releaseBlock();
        }

        // ── Handle index deletion (skip for now, no indexing yet) ──
        if (rootBlock != -1) {
            // BPlusTree::bPlusDestroy(rootBlock);
            // Not implemented yet — leave this commented out
        }

    } // end while — all attribute entries deleted

    // numberOfAttributesDeleted now equals numAttrs
    // This will be used in Part 5 to update the cache
    // (continuing inside BlockAccess::deleteRelation())

    // ─────────────────────────────────────────────
    // TASK 5: Delete the RELATIONCAT entry
    // ─────────────────────────────────────────────

    // We already have relCatRecId from TASK 2 — it tells us exactly
    // which block and slot holds the RELATIONCAT entry for this relation

    // Re-create the RecBuffer for the RELCAT block
    // (we used it earlier but need a fresh object now)
    RecBuffer relCatBlockForDeletion(relCatRecId.block);

    // Read the current header of the RELCAT block
    HeadInfo relCatHeader;
    relCatBlockForDeletion.getHeader(&relCatHeader);

    // ── Decrement numEntries in the RELCAT block header ──
    relCatHeader.numEntries--;
    relCatBlockForDeletion.setHeader(&relCatHeader);
    // This tells the block: "one fewer record lives here now"

    // ── Mark the slot as free in the RELCAT slotmap ──
    unsigned char relCatSlotMap[relCatHeader.numSlots];
    relCatBlockForDeletion.getSlotMap(relCatSlotMap);

    relCatSlotMap[relCatRecId.slot] = SLOT_UNOCCUPIED;
    // This "frees" the slot — future inserts can reuse it

    relCatBlockForDeletion.setSlotMap(relCatSlotMap);

    // NOTE: We do NOT release the RELCAT block even if numEntries becomes 0
    // Why? RELCAT always has at least 2 entries (for RELCAT and ATTRCAT themselves)
    // So it will never become completely empty

    // ─────────────────────────────────────────────
    // TASK 6: Update the Relation Cache
    // ─────────────────────────────────────────────

    // ── Update RELCAT's cache entry ──
    // The relation catalog itself now has one fewer record

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
    // Fetch the current cache entry for RELATIONCAT

    relCatEntry.numRecs--;
    // One relation has been deleted → one fewer record in RELCAT

    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);
    // Write the updated entry back to the cache
    // (This marks the cache entry as "modified" → will be written
    //  to disk when the destructor runs at shutdown)

    // ── Update ATTRCAT's cache entry ──
    // The attribute catalog now has fewer records too

    RelCatEntry attrCatEntry;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatEntry);
    // Fetch the current cache entry for ATTRIBUTECAT

    attrCatEntry.numRecs -= numberOfAttributesDeleted;
    // All attribute entries for the deleted relation are gone

    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatEntry);
    // Write the updated entry back to the cache

    return SUCCESS;
}
// ─── End of BlockAccess::deleteRelation() 


int BlockAccess::search(int relId, Attribute *record,
                        char attrName[ATTR_SIZE], Attribute attrVal, int op) {

    // Step 1: Declare a RecId to store the result of the search
    RecId recId;

    /* Step 2: Search for a record satisfying the condition
       attrName op attrVal using linearSearch.
       
       linearSearch() uses the searchIndex stored in the relation cache
       to continue from where it left off (stateful search).
       Each call returns the NEXT matching record.
    */
    recId = BlockAccess::linearSearch(relId, attrName, attrVal, op);

    // Step 3: If no matching record was found, return E_NOTFOUND
    if (recId.block == -1 && recId.slot == -1) {
        return E_NOTFOUND;
    }

    /* Step 4: A matching record was found at recId.
       Copy that record into the output buffer `record`.
       
       Create a RecBuffer object for the block containing the record,
       then use getRecord() to fetch it into `record`.
    */
    RecBuffer recBuffer(recId.block);
    int ret = recBuffer.getRecord(record, recId.slot);

    // Step 5: Return the result of getRecord()
    // (SUCCESS if all went well, or an error code)
    return ret;
}