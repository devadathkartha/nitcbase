#include "BPlusTree.h"

#include <cstring>

// BPlusTree/BPlusTree.cpp

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE],
                              Attribute attrVal, int op) {

    /*** SETUP: Get search index and attribute catalog entry ***/

    // will hold the current B+ search position
    IndexId searchIndex;

    // get current search index for this attribute
    // (was set by previous call, or {-1,-1} if reset/first call)
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    // get attribute catalog entry — we need rootBlock
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // variables to track current position in the tree
    int block, index;

    /*** PHASE 1a: Determine starting position ***/

    if (searchIndex.block == -1 && searchIndex.index == -1) {
        // FIRST CALL (or after reset): start from root

        block = attrCatEntry.rootBlock;
        index = 0;

        // if no B+ tree exists for this attribute
        if (block == -1) {
            return RecId{-1, -1};
        }

    } else {
        // SUBSEQUENT CALL: resume from where we left off

        // we were at searchIndex.block (a leaf), searchIndex.index
        // advance by 1 to get the NEXT entry
        block = searchIndex.block;
        index = searchIndex.index + 1;

        // load the leaf block to check if we've gone past the end
        IndLeaf leaf(block);
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries) {
            // current leaf is exhausted — move to next leaf in linked list
            // rblock in the header points to the next leaf block
            block = leafHead.rblock;
            index = 0;

            if (block == -1) {
                // reached end of leaf linked list — no more records
                return RecId{-1, -1};
            }
        }
        // NOTE: if index < numEntries, we stay in current leaf at new index
        // Phase 2 will handle the actual search from here
        // We skip internal node traversal (already at a leaf)
    }

    /*** PHASE 1b: Traverse internal nodes to reach correct leaf ***/

    // This loop only executes when:
    // 1. searchIndex was {-1,-1} (first call, starting from root)
    // 2. AND root is not a leaf (tree has internal nodes)
    //
    // When searchIndex was valid, block is already a leaf block,
    // so getStaticBlockType returns IND_LEAF and loop is skipped

    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {

        // load this internal block
        IndInternal internalBlk(block);

        HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        // will hold the current entry being examined
        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE) {
            // For NE, LT, LE: always go to the LEFTMOST leaf
            // (we need to scan the entire leaf list anyway)

            // get the very first entry of this internal node
            internalBlk.getEntry(&intEntry, 0);

            // move to left child of first entry
            block = intEntry.lChild;

        } else {
            // For EQ, GE, GT: find first entry satisfying condition
            // then go to its left child

            // flag to track if we found a suitable entry
            bool found = false;

            for (int i = 0; i < intHead.numEntries; i++) {
                internalBlk.getEntry(&intEntry, i);

                // For EQ, GE: find first entry where attrVal >= search value
                // For GT:     find first entry where attrVal > search value
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal,
                                          attrCatEntry.attrType);

                if (
                    (op == EQ && cmpVal >= 0) ||
                    (op == GE && cmpVal >= 0) ||
                    (op == GT && cmpVal > 0)
                ) {
                    // this entry's key is >= (or >) our search value
                    // the record we want is in the LEFT child of this entry
                    found = true;
                    block = intEntry.lChild;
                    break;
                }
            }

            if (!found) {
                // all entries in this node are < attrVal
                // the record must be in the RIGHTMOST child
                // get the last entry to find rChild
                internalBlk.getEntry(&intEntry, intHead.numEntries - 1);
                block = intEntry.rChild;
            }
        }
        // loop continues: check if new block is still internal or now a leaf
    }

    // At this point: block is definitely a leaf index block
    // Phase 2 will scan this leaf (and subsequent leaves) for a match

   // BPlusTree/BPlusTree.cpp (continuing from Phase 1)

    // NOTE: At this point, `block` is a leaf block number
    // and `index` is where we start scanning within it

    /*** PHASE 2: Scan leaf nodes for matching record ***/

    while (block != -1) {

        // load the current leaf block
        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        // will hold each entry we examine
        Index leafEntry;

        // scan entries starting from `index`
        while (index < leafHead.numEntries) {

            // get the entry at position `index`
            leafBlk.getEntry(&leafEntry, index);

            // compare the entry's attribute value against our search value
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal,
                                      attrCatEntry.attrType);

            // check if this entry satisfies the condition
            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0)  ||
                (op == GT && cmpVal > 0)  ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                // MATCH FOUND

                // update the search index to current position
                // so next call resumes from index+1
                IndexId newSearchIndex = {block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &newSearchIndex);

                // return the RecId of the actual record
                // (leafEntry.block and leafEntry.slot point to the record)
                return RecId{leafEntry.block, leafEntry.slot};
            }

            // Early termination for EQ, LE, LT:
            // entries are sorted ascending, so if current entry > attrVal,
            // no future entry can satisfy EQ, LE, or LT
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) {
                return RecId{-1, -1};
            }

            // no match yet — move to next entry in this leaf
            index++;
        }

        // exhausted all entries in current leaf block

        // for operators other than NE, if we reach here without finding
        // a match, no match exists (values only go higher to the right)
        if (op != NE) {
            break;
        }

        // for NE: must check ALL leaves (non-matching value could be anywhere)
        // move to the next leaf in the linked list
        block = leafHead.rblock;
        index = 0;  // start from first entry of next leaf
    }

    // no entry satisfying the condition was found
    return RecId{-1, -1};

}


int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {

    // Step 1: Catalog relations cannot be indexed
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    // Step 2: Get the attribute cache entry for attrName
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // Step 3: If attribute doesn't exist, return error
    if (ret != SUCCESS) {
        return ret;  // E_ATTRNOTEXIST or E_RELNOTOPEN etc.
    }

    // Step 4: If index already exists, nothing to do
    if (attrCatEntry.rootBlock != -1) {
        return SUCCESS;
    }

    /****** Create the initial root block ******/

    // Step 5: Allocate a new leaf block using constructor 1
    // Constructor 1 allocates a NEW block on disk
    IndLeaf rootBlockBuf;

    // Step 6: Get the block number of the newly allocated block
    int rootBlock = rootBlockBuf.getBlockNum();

    // Step 7: Check if disk is full
    if (rootBlock == E_DISKFULL) {
        return E_DISKFULL;
    }

    // Step 8: Update the rootBlock in the attribute cache entry
    // This marks the cache entry as dirty → written back on closeRel()
    attrCatEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    /****** Traverse all records and insert into B+ tree ******/

    // Step 9: Get the relation catalog entry
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    // Step 10: Start from the first record block
    int block = relCatEntry.firstBlk;

    while (block != -1) {

        // Step 11: Create RecBuffer for this block
        RecBuffer recBuffer(block);

        // Step 12: Get the slot map to know which slots are occupied
        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        recBuffer.getSlotMap(slotMap);

        // Step 13: Iterate over all slots in this block
        for (int slot = 0; slot < relCatEntry.numSlotsPerBlk; slot++) {

            // Skip unoccupied slots
            if (slotMap[slot] == SLOT_UNOCCUPIED) {
                continue;
            }

            // Step 14: Get the full record for this slot
            Attribute record[relCatEntry.numAttrs];
            recBuffer.getRecord(record, slot);

            // Step 15: Build the RecId for this record
            RecId recId = {block, slot};

            // Step 16: Insert the attribute value into the B+ tree
            // attrCatEntry.offset tells us which field in the record is attrName
            int retVal = bPlusInsert(relId, attrName, 
                                     record[attrCatEntry.offset], recId);

            if (retVal == E_DISKFULL) {
                // bPlusInsert already destroyed the tree internally
                // when E_DISKFULL occurs
                return E_DISKFULL;
            }
        }

        // Step 17: Move to the next record block
        HeadInfo header;
        recBuffer.getHeader(&header);
        block = header.rblock;
    }

    return SUCCESS;
}


int BPlusTree::bPlusDestroy(int rootBlockNum) {

    // Step 1: Validate block number range
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    // Step 2: Determine what type of block this is
    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF) {

        // Step 3a: It's a leaf — just release it, no children to recurse into
        IndLeaf leafBlock(rootBlockNum);  // constructor 2: open existing block
        leafBlock.releaseBlock();
        return SUCCESS;

    } else if (type == IND_INTERNAL) {

        // Step 3b: It's an internal block — must recurse into all children first
        IndInternal internalBlock(rootBlockNum);  // constructor 2

        // Step 4: Get the header to know how many entries exist
        HeadInfo header;
        internalBlock.getHeader(&header);

        // Step 5: Destroy lChild of the FIRST entry
        InternalEntry firstEntry;
        internalBlock.getEntry(&firstEntry, 0);
        bPlusDestroy(firstEntry.lChild);

        // Step 6: Destroy rChild of EVERY entry
        // (rChild of entry i = lChild of entry i+1, so this covers all children)
        for (int i = 0; i < header.numEntries; i++) {
            InternalEntry entry;
            internalBlock.getEntry(&entry, i);
            bPlusDestroy(entry.rChild);
        }

        // Step 7: Now release this internal block itself
        internalBlock.releaseBlock();
        return SUCCESS;

    } else {
        // Block is not an index block at all
        return E_INVALIDBLOCK;
    }
}


int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], 
                            Attribute attrVal, RecId recId) {

    // Step 1: Get the attribute cache entry for attrName
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // Step 2: If getAttrCatEntry failed, return the error
    if (ret != SUCCESS) {
        return ret;
    }

    // Step 3: Get rootBlock from attribute cache entry
    int blockNum = attrCatEntry.rootBlock;

    // Step 4: If no index exists on this attribute
    if (blockNum == -1) {
        return E_NOINDEX;
    }

    // Step 5: Find the correct leaf block for insertion
    // Traverses from root down to appropriate leaf
    int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrCatEntry.attrType);

    // Step 6: Build the Index entry to insert
    // Index struct = {attrVal, block, slot}
    Index indexEntry;
    indexEntry.attrVal = attrVal;
    indexEntry.block = recId.block;
    indexEntry.slot = recId.slot;

    // Step 7: Insert into the leaf block
    // insertIntoLeaf handles splitting and upward propagation if needed
    int retVal = insertIntoLeaf(relId, attrName, leafBlkNum, indexEntry);

    // Step 8: Handle disk full during insertion
    if (retVal == E_DISKFULL) {

        // The tree is now in an inconsistent state — destroy it entirely
        // Get fresh rootBlock (it may have changed if root was split)
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
        bPlusDestroy(attrCatEntry.rootBlock);

        // Reset rootBlock to -1 in attribute cache
        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {

    // Start at the root
    int blockNum = rootBlock;

    // Keep going down until we reach a leaf block
    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {

        // Open this internal block
        IndInternal internalBlock(blockNum);

        // Get the header to know number of entries
        HeadInfo header;
        internalBlock.getHeader(&header);

        // Search for first entry whose attrVal >= value to insert
        int entryIndex = -1;  // will store index of first entry >= attrVal

        for (int i = 0; i < header.numEntries; i++) {
            InternalEntry entry;
            internalBlock.getEntry(&entry, i);

            // compareAttrs returns:
            //   < 0  if entry.attrVal <  attrVal
            //   = 0  if entry.attrVal == attrVal
            //   > 0  if entry.attrVal >  attrVal
            int cmpResult = compareAttrs(entry.attrVal, attrVal, attrType);

            if (cmpResult >= 0) {
                // Found first entry >= attrVal
                entryIndex = i;
                break;
            }
        }

        if (entryIndex == -1) {
            // attrVal is greater than ALL entries in this node
            // Go to rightmost child (rChild of last entry)
            InternalEntry lastEntry;
            internalBlock.getEntry(&lastEntry, header.numEntries - 1);
            blockNum = lastEntry.rChild;

        } else {
            // Go to lChild of the first entry that is >= attrVal
            InternalEntry foundEntry;
            internalBlock.getEntry(&foundEntry, entryIndex);
            blockNum = foundEntry.lChild;
        }
    }

    // blockNum now points to the correct leaf block
    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], 
                               int blockNum, Index indexEntry) {

    // Step 1: Get attribute cache entry (need attrType for comparison)
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // Step 2: Open the leaf block
    IndLeaf leafBlock(blockNum);  // constructor 2: existing block

    // Step 3: Get header (need numEntries)
    HeadInfo blockHeader;
    leafBlock.getHeader(&blockHeader);

    // Step 4: Build sorted array with all existing + new entry
    Index indices[blockHeader.numEntries + 1];

    int newPos = 0;
    bool inserted = false;

    for (int i = 0; i < blockHeader.numEntries; i++) {
        Index existingEntry;
        leafBlock.getEntry(&existingEntry, i);

        if (!inserted && 
            compareAttrs(existingEntry.attrVal, indexEntry.attrVal,
                         attrCatEntry.attrType) >= 0) {
            indices[newPos++] = indexEntry;
            inserted = true;
        }
        indices[newPos++] = existingEntry;
    }
    if (!inserted) {
        indices[newPos] = indexEntry;
    }

    /****** CASE 1: Leaf is not full ******/
    if (blockHeader.numEntries != MAX_KEYS_LEAF) {

        // Increment entry count
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);

        // Write all entries back to the block
        for (int i = 0; i < blockHeader.numEntries; i++) {
            leafBlock.setEntry(&indices[i], i);
        }

        return SUCCESS;
    }

    /****** CASE 2: Leaf is full — need to split ******/

    // Step 5: Split the leaf
    // indices[] has MAX_KEYS_LEAF + 1 = 64 entries to distribute
    int newRightBlk = splitLeaf(blockNum, indices);

    if (newRightBlk == E_DISKFULL) {
        return E_DISKFULL;
    }

    // Step 6: Propagate middle value to parent
    // Middle value = indices[MIDDLE_INDEX_LEAF] = indices[31]
    // Left block gets indices[0..31], right block gets indices[32..63]
    // The value at index 31 (last of left block) goes UP to parent

    if (blockHeader.pblock != -1) {
        // Current leaf is NOT the root → insert into parent internal block

        // Build InternalEntry with:
        // attrVal = middle value (last entry of left block)
        // lChild  = current block (left block after split)
        // rChild  = newRightBlk
        InternalEntry parentEntry;
        parentEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        parentEntry.lChild = blockNum;
        parentEntry.rChild = newRightBlk;

        return insertIntoInternal(relId, attrName, 
                                   blockHeader.pblock, parentEntry);

    } else {
        // Current leaf IS the root → create a new root
        return createNewRoot(relId, attrName,
                              indices[MIDDLE_INDEX_LEAF].attrVal,
                              blockNum, newRightBlk);
    }
}


int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {

    // Step 1: Allocate a NEW leaf block for the right half
    // Constructor 1: allocates a new block on disk
    IndLeaf rightBlk;
    int rightBlkNum = rightBlk.getBlockNum();

    // Step 2: Open existing leaf block as left block
    // Constructor 2: opens existing block
    IndLeaf leftBlk(leafBlockNum);
    int leftBlkNum = leafBlockNum;

    // Step 3: Check if new block allocation succeeded
    if (rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    // Step 4: Get headers of both blocks
    HeadInfo leftBlkHeader, rightBlkHeader;
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    // Step 5: Set up right block header
    // Right block gets:
    //   - numEntries = 32 (= (MAX_KEYS_LEAF + 1) / 2)
    //   - pblock = same parent as left block
    //   - lblock = leftBlkNum (left block is its left neighbor)
    //   - rblock = left block's old rblock (right neighbor)
    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;  // = 32
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlk.setHeader(&rightBlkHeader);

    // Step 6: Update left block header
    // Left block gets:
    //   - numEntries = 32
    //   - rblock = rightBlkNum (new right neighbor)
    //   (lblock and pblock stay the same)
    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;  // = 32
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    // Step 7: Distribute entries
    // Left block gets indices[0] to indices[31]  (first 32)
    for (int i = 0; i < 32; i++) {
        leftBlk.setEntry(&indices[i], i);
    }

    // Right block gets indices[32] to indices[63] (last 32)
    for (int i = 0; i < 32; i++) {
        rightBlk.setEntry(&indices[32 + i], i);
    }

    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE],
                                   int intBlockNum, InternalEntry intEntry) {

    // Step 1: Get attribute cache entry (need attrType for comparison)
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // Step 2: Open the internal block
    IndInternal intBlk(intBlockNum);  // constructor 2: existing block

    // Step 3: Get header (need numEntries and pblock)
    HeadInfo blockHeader;
    intBlk.getHeader(&blockHeader);

    // Step 4: Build sorted array with all existing + new entry
    // Size = numEntries + 1
    InternalEntry internalEntries[blockHeader.numEntries + 1];

    int newPos = 0;
    bool inserted = false;

    for (int i = 0; i < blockHeader.numEntries; i++) {
        InternalEntry existingEntry;
        intBlk.getEntry(&existingEntry, i);

        if (!inserted &&
            compareAttrs(existingEntry.attrVal, intEntry.attrVal,
                         attrCatEntry.attrType) >= 0) {

            // Place new entry here
            internalEntries[newPos++] = intEntry;
            inserted = true;

            // CRITICAL: Update lChild of the existing entry
            // The existing entry's lChild becomes intEntry's rChild
            // because intEntry's right subtree sits between
            // intEntry.attrVal and existingEntry.attrVal
            existingEntry.lChild = intEntry.rChild;
        }
        internalEntries[newPos++] = existingEntry;
    }

    // If new entry is largest, append at end
    if (!inserted) {
        internalEntries[newPos] = intEntry;
    }

    /****** CASE 1: Internal block not full ******/
    if (blockHeader.numEntries != MAX_KEYS_INTERNAL) {

        // Increment entry count
        blockHeader.numEntries++;
        intBlk.setHeader(&blockHeader);

        // Write all entries back
        for (int i = 0; i < blockHeader.numEntries; i++) {
            intBlk.setEntry(&internalEntries[i], i);
        }

        return SUCCESS;
    }

    /****** CASE 2: Internal block is full — need to split ******/

    // Step 5: Split the internal block
    // internalEntries[] has MAX_KEYS_INTERNAL + 1 = 101 entries
    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL) {

        // Destroy the right subtree rooted at intEntry.rChild
        // This is the newly built subtree that hasn't been
        // connected to the main tree yet
        BPlusTree::bPlusDestroy(intEntry.rChild);

        return E_DISKFULL;
    }

    // Step 6: Propagate middle value to parent
    // Middle value = internalEntries[MIDDLE_INDEX_INTERNAL] = internalEntries[50]
    // Left block gets indices[0..49], middle[50] goes UP, right gets [51..100]

    if (blockHeader.pblock != -1) {
        // Current block is NOT root → insert into parent

        InternalEntry parentEntry;
        parentEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        parentEntry.lChild  = intBlockNum;    // left block after split
        parentEntry.rChild  = newRightBlk;   // right block after split

        int ret = insertIntoInternal(relId, attrName,
                                      blockHeader.pblock, parentEntry);

        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }

    } else {
        // Current block IS root → create new root

        int ret = createNewRoot(relId, attrName,
                                 internalEntries[MIDDLE_INDEX_INTERNAL].attrVal,
                                 intBlockNum, newRightBlk);

        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }
    }

    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {

    // Step 1: Allocate NEW internal block for right half
    // Constructor 1: allocates new block
    IndInternal rightBlk;
    int rightBlkNum = rightBlk.getBlockNum();

    // Step 2: Open existing internal block as left block
    // Constructor 2: opens existing block
    IndInternal leftBlk(intBlockNum);
    int leftBlkNum = intBlockNum;

    // Step 3: Check if allocation succeeded
    if (rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    // Step 4: Get headers of both blocks
    HeadInfo leftBlkHeader, rightBlkHeader;
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    // Step 5: Set up right block header
    // - numEntries = 50 (= MAX_KEYS_INTERNAL / 2)
    // - pblock = same as left block's parent
    // Note: lblock and rblock are not used for internal nodes
    rightBlkHeader.numEntries = MAX_KEYS_INTERNAL / 2;  // = 50
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlk.setHeader(&rightBlkHeader);

    // Step 6: Update left block header
    // - numEntries = 50
    leftBlkHeader.numEntries = MAX_KEYS_INTERNAL / 2;  // = 50
    leftBlk.setHeader(&leftBlkHeader);

    // Step 7: Distribute entries
    // Left block gets internalEntries[0] to internalEntries[49]
    for (int i = 0; i < 50; i++) {
        leftBlk.setEntry(&internalEntries[i], i);
    }

    // Right block gets internalEntries[51] to internalEntries[100]
    // Note: internalEntries[50] = MIDDLE_INDEX_INTERNAL goes UP to parent
    // It is NOT stored in either child
    for (int i = 0; i < 50; i++) {
        rightBlk.setEntry(&internalEntries[51 + i], i);
    }

    // Step 8: Update pblock of all children of the right block
    // These children currently think their parent is leftBlkNum
    // We must update them to point to rightBlkNum

    // Get the block type of children
    // (all children are the same type — either all IND_LEAF or all IND_INTERNAL)
    int type = StaticBuffer::getStaticBlockType(
                   internalEntries[51].lChild);  // any child of right block

    // Children of right block: lChild of entry 51, rChild of entries 51..100
    // = lChild of internalEntries[51], then rChild of internalEntries[51..100]

    // Update lChild of first entry in right block (= internalEntries[51].lChild)
    // and rChild of all entries in right block (= internalEntries[51..100].rChild)

    for (int i = 51; i <= 100; i++) {
        // For entry at index i in internalEntries:
        // It maps to position (i-51) in the right block

        int childBlockNum;

        if (i == 51) {
            // Update lChild of the first entry of right block
            childBlockNum = internalEntries[i].lChild;
            BlockBuffer child(childBlockNum);
            HeadInfo childHeader;
            child.getHeader(&childHeader);
            childHeader.pblock = rightBlkNum;
            child.setHeader(&childHeader);
        }

        // Update rChild of every entry in right block
        childBlockNum = internalEntries[i].rChild;
        BlockBuffer child(childBlockNum);
        HeadInfo childHeader;
        child.getHeader(&childHeader);
        childHeader.pblock = rightBlkNum;
        child.setHeader(&childHeader);
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE],
                              Attribute attrVal, int lChild, int rChild) {

    // Step 1: Get attribute cache entry
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // Step 2: Allocate a new internal block to serve as new root
    // Constructor 1: allocates new block on disk
    IndInternal newRootBlk;
    int newRootBlkNum = newRootBlk.getBlockNum();

    // Step 3: Check if disk is full
    if (newRootBlkNum == E_DISKFULL) {

        // Destroy the right subtree — it exists on disk but
        // cannot be connected to the tree (no root to link through)
        BPlusTree::bPlusDestroy(rChild);

        return E_DISKFULL;
    }

    // Step 4: Set up the new root's header
    // New root has exactly 1 entry
    HeadInfo newRootHeader;
    newRootBlk.getHeader(&newRootHeader);
    newRootHeader.numEntries = 1;
    newRootHeader.pblock = -1;     // root has no parent
    newRootHeader.lblock = -1;     // internal nodes not linked
    newRootHeader.rblock = -1;
    newRootBlk.setHeader(&newRootHeader);

    // Step 5: Create and write the single entry in the new root
    // This entry separates lChild (left subtree) from rChild (right subtree)
    InternalEntry rootEntry;
    rootEntry.lChild  = lChild;
    rootEntry.attrVal = attrVal;   // separator value from the split
    rootEntry.rChild  = rChild;
    newRootBlk.setEntry(&rootEntry, 0);  // entry at index 0

    // Step 6: Update pblock of left child to point to new root
    BlockBuffer lChildBuf(lChild);
    HeadInfo lChildHeader;
    lChildBuf.getHeader(&lChildHeader);
    lChildHeader.pblock = newRootBlkNum;
    lChildBuf.setHeader(&lChildHeader);

    // Step 7: Update pblock of right child to point to new root
    BlockBuffer rChildBuf(rChild);
    HeadInfo rChildHeader;
    rChildBuf.getHeader(&rChildHeader);
    rChildHeader.pblock = newRootBlkNum;
    rChildBuf.setHeader(&rChildHeader);

    // Step 8: Update rootBlock in attribute cache
    // This is the ONLY place rootBlock gets updated during index creation
    // (apart from the initial allocation in bPlusCreate)
    attrCatEntry.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}