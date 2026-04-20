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