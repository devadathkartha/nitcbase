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