#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum) {
  // initialise this.blockNum with the argument
  this->blockNum=blockNum;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  unsigned char *bufferPtr; // Notice this is just a pointer now, not a full array!
  
  // Call our new engine. If successful, bufferPtr now points to the data in RAM.
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret; // Return any errors (like E_OUTOFBOUND)
  }

  // Read the fields from bufferPtr just like we did in Stage 2
  memcpy(&head->pblock, bufferPtr + 4, 4);
  memcpy(&head->lblock, bufferPtr + 8, 4);
  memcpy(&head->rblock, bufferPtr + 12, 4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs, bufferPtr + 20, 4);
  memcpy(&head->numSlots, bufferPtr + 24, 4);

  return SUCCESS;
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;
  
  // getHeader now automatically uses the buffer too!
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  unsigned char *bufferPtr;
  // Get the pointer to the block in the buffer
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  // Calculate the offset exactly as before, but using bufferPtr
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + HEADER_SIZE + slotCount + (recordSize * slotNum);

  // Copy the specific record into the caller's `rec` variable
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}
// Write the record at slotNum from the argument pointer
int RecBuffer::setRecord(union Attribute *rec, int slotNum) {

    // Step 1: get pointer to buffer containing this block
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS) {
        return ret;
    }

    // Step 2: get the header to find numAttrs and numSlots
    struct HeadInfo head;
    getHeader(&head);

    int numAttrs = head.numAttrs;
    int numSlots = head.numSlots;

    // Step 3: validate slotNum
    if (slotNum < 0 || slotNum >= numSlots) {
        return E_OUTOFBOUND;
    }

    // Step 4: calculate the size of one record
    int recordSize = ATTR_SIZE * numAttrs;

    // Step 5: calculate exact offset to the target slot
    // bufferPtr + HEADER_SIZE               → skip header
    //           + numSlots                  → skip slotmap
    //           + (slotNum * recordSize)    → skip to correct slot
    unsigned char *slotPointer = bufferPtr 
                                + HEADER_SIZE 
                                + (numSlots) 
                                + (slotNum * recordSize);

    // Step 6: copy record data into the buffer
    memcpy(slotPointer, rec, recordSize);

    // Step 7: mark this buffer as dirty (needs write-back)
    StaticBuffer::setDirtyBit(this->blockNum);

    return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** buffPtr) {

    // Step 1: check if block is already in buffer
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if (bufferNum != E_BLOCKNOTINBUFFER) {
        // block IS in buffer — just update timestamps

        // increment timestamps of all other occupied buffers
        for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
            if (StaticBuffer::metainfo[bufferIndex].free == false &&
                bufferIndex != bufferNum) {
                StaticBuffer::metainfo[bufferIndex].timeStamp++;
            }
        }

        // reset THIS buffer's timestamp to 0 (most recently used)
        StaticBuffer::metainfo[bufferNum].timeStamp = 0;

    } else {
        // block is NOT in buffer — need to load it

        // Step 2: get a free buffer slot (may evict LRU block)
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND) {
            return E_OUTOFBOUND;
        }

        // Step 3: read the block from disk into the buffer slot
        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }

    // Step 4: set the pointer to the start of this buffer slot
    *buffPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    double diff;
    
    // 1. If the attributes are STRINGS, use strcmp for lexicographic comparison
    if (attrType == STRING) {
        diff = strcmp(attr1.sVal, attr2.sVal);
    } 
    // 2. If the attributes are NUMBERS, simply subtract them
    else if (attrType == NUMBER) {
        diff = attr1.nVal - attr2.nVal;
    }

    // 3. Return a clean 1, -1, or 0 based on the difference
    if (diff > 0) {
        return 1;
    } else if (diff < 0) {
        return -1;
    } else {
        return 0;
    }
}

/* Used to get the slotmap from a record block */
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // 1. Get the starting address of the block in the buffer
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  // 2. Get the header of the block so we know how many slots there are
  struct HeadInfo head;
  this->getHeader(&head);

  int slotCount = head.numSlots;

  // 3. Get a pointer to the beginning of the slotmap in memory.
  // The slotmap starts immediately after the 32-byte header!
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // 4. Copy the values from the buffer into the caller's array
  // We copy exactly `slotCount` bytes because 1 byte = 1 slot in the slotmap.
  memcpy(slotMap, slotMapInBuffer, slotCount);

  return SUCCESS;
}

// Buffer/BlockBuffer.cpp

int BlockBuffer::setHeader(struct HeadInfo *head) {

    unsigned char *bufferPtr;
    // loadBlockAndGetBufferPtr loads the block into buffer if not already
    // present, and gives us a pointer to the start of that buffer slot
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS) {
        // block couldn't be loaded into buffer
        return ret;
    }

    // The header sits at the very start of the block (first 32 bytes)
    // Cast bufferPtr to HeadInfo* so we can write field by field
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // Copy each field from the input head into the buffer's header
    // We do NOT copy 'reserved' — that's internal padding
    bufferHeader->blockType  = head->blockType;
    bufferHeader->pblock     = head->pblock;
    bufferHeader->lblock     = head->lblock;
    bufferHeader->rblock     = head->rblock;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numAttrs   = head->numAttrs;
    bufferHeader->numSlots   = head->numSlots;

    // Mark this buffer slot as dirty so it gets written back to disk
    // at shutdown (or when the buffer slot is evicted)
    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if (ret != SUCCESS) {
        return ret;
    }

    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType) {

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS) {
        return ret;
    }

    // The blockType field is the FIRST 4 bytes of the block header
    // Cast bufferPtr to int32_t* and directly assign the block type
    *((int32_t *)bufferPtr) = blockType;
    //  ↑ this writes blockType into bytes 0-3 of the block, which is
    //    exactly where HeadInfo.blockType lives

    // Also update the in-memory blockAllocMap so the system knows
    // this block is now of the given type
    // this->blockNum is the block's position in the disk / blockAllocMap
    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    // Mark dirty — this change must be written back to disk
    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if (ret != SUCCESS) {
        return ret;
    }

    return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType) {

    // Step 1: Scan blockAllocMap to find a free block on disk
    int freeBlockNum = -1;
    for (int i = 0; i < DISK_BLOCKS; i++) {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
            freeBlockNum = i;
            break;
        }
    }

    // If no free block found, disk is completely full
    if (freeBlockNum == -1) {
        return E_DISKFULL;
    }

    // Step 2: Store this block number in the object's blockNum field
    // This is important — other methods like loadBlockAndGetBufferPtr
    // use this->blockNum to know which block they're working with
    this->blockNum = freeBlockNum;

    // Step 3: Allocate a buffer slot for this new block
    // getFreeBuffer() finds an empty (or evictable) buffer slot
    // and associates it with this block number
    int bufferNum = StaticBuffer::getFreeBuffer(freeBlockNum);
    if (bufferNum == E_OUTOFBOUND) {
        return E_OUTOFBOUND;
    }

    // Step 4: Initialize the block header with all-zero/empty values
    // This wipes whatever garbage was in the buffer slot
    struct HeadInfo head;
    head.blockType  = blockType;  // will be set properly by setBlockType()
    head.pblock     = -1;         // no parent
    head.lblock     = -1;         // no left neighbour yet
    head.rblock     = -1;         // no right neighbour yet
    head.numEntries = 0;          // no records yet
    head.numAttrs   = 0;          // will be set later by BlockAccess::insert
    head.numSlots   = 0;          // will be set later by BlockAccess::insert
    setHeader(&head);

    // Step 5: Mark the block type in both the header and blockAllocMap
    setBlockType(blockType);

    // Return the allocated block number to the caller
    return freeBlockNum;
}

BlockBuffer::BlockBuffer(char blockType) {
    // Convert char type to int type for getFreeBlock
    // 'R' → REC, 'I' → IND_INTERNAL, 'L' → IND_LEAF
    int intBlockType;
    if (blockType == 'R') {
        intBlockType = REC;
    } else if (blockType == 'I') {
        intBlockType = IND_INTERNAL;
    } else if (blockType == 'L') {
        intBlockType = IND_LEAF;
    }

    // Call getFreeBlock to allocate a disk block and buffer slot
    // The result (block number OR error code) goes into this->blockNum
    this->blockNum = getFreeBlock(intBlockType);

    // IMPORTANT: If getFreeBlock returned E_DISKFULL,
    // then this->blockNum = E_DISKFULL (a negative error code)
    // The CALLER must check this->blockNum after construction!
}

// This is the entire implementation — one line!
RecBuffer::RecBuffer() : BlockBuffer('R') {
    // Calls BlockBuffer('R') which calls getFreeBlock(REC)
    // this->blockNum is set to the new block number OR E_DISKFULL
}

// Buffer/BlockBuffer.cpp

int RecBuffer::setSlotMap(unsigned char *slotMap) {

    unsigned char *bufferPtr;
    // Load the block into buffer and get pointer to start of buffer slot
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS) {
        return ret;
    }

    // Get the header to find out how many slots this block has
    // We need numSlots to know how many bytes to copy
    struct HeadInfo head;
    getHeader(&head);
    int numSlots = head.numSlots;

    // The slot map starts exactly HEADER_SIZE bytes into the block
    // Copy numSlots bytes from the input slotMap into the buffer
    memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);
    //      ↑ destination              ↑ source   ↑ number of bytes

    // Mark the block as dirty — changes must be written to disk
    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if (ret != SUCCESS) {
        return ret;
    }

    return SUCCESS;
}

int BlockBuffer::getBlockNum() {
    // Simply return the private blockNum field
    // This is needed because blockNum is private —
    // outside code cannot access it directly
    return this->blockNum;
}

void BlockBuffer::releaseBlock() {

    // Step 1: If blockNum is already invalid, do nothing
    // (prevents double-release bugs)
    if (blockNum == INVALID_BLOCKNUM) {
        return;
    }

    // Step 2: Check if this block is currently loaded in the buffer
    int bufferNum = StaticBuffer::getBufferNum(blockNum);

    // Step 3: If it IS in the buffer, free that buffer slot
    if (bufferNum != E_BLOCKNOTINBUFFER) {
        // Mark the buffer slot as free so it can be reused
        StaticBuffer::metainfo[bufferNum].free = true;
    }

    // Step 4: Mark the block as UNUSED in the block allocation map
    // This is the actual "freeing" on disk — now this block number
    // can be allocated to a new block in the future
    StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;

    // Step 5: Invalidate this object's blockNum
    // Any further calls to this object's methods will now fail safely
    blockNum = INVALID_BLOCKNUM;
}

// ===== IndBuffer Constructors =====

// Constructor 1: allocate a NEW index block of given type
// blockType is 'I' for internal, 'L' for leaf
IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {
    // BlockBuffer(char) allocates a new block of the given type on disk
    // and loads it into buffer. Nothing extra needed here.
}

// Constructor 2: use an EXISTING index block (already on disk)
IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {
    // BlockBuffer(int) loads the block with given blockNum into buffer.
    // Nothing extra needed here.
}


// ===== IndInternal Constructors =====

// Constructor 1: allocate a new INTERNAL index block
IndInternal::IndInternal() : IndBuffer('I') {
    // 'I' tells IndBuffer (and ultimately BlockBuffer) to allocate
    // a new block of type IND_INTERNAL
}

// Constructor 2: use existing internal index block
IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {
    // loads the existing block with given blockNum
}


// ===== IndLeaf Constructors =====

// Constructor 1: allocate a new LEAF index block
IndLeaf::IndLeaf() : IndBuffer('L') {
    // 'L' tells IndBuffer to allocate a new block of type IND_LEAF
}

// Constructor 2: use existing leaf index block
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {
    // loads the existing block with given blockNum
}


// ===== IndInternal::getEntry() =====
// Copies the indexNum-th entry from an internal index block into *ptr

int IndInternal::getEntry(void *ptr, int indexNum) {

    // Step 1: validate indexNum range
    if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
        return E_OUTOFBOUND;
    }

    // Step 2: get pointer to this block's buffer
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
        return ret;
    }

    // Step 3: cast the void* to InternalEntry*
    // The caller is responsible for passing a valid InternalEntry*
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    // Step 4: calculate where this entry starts in the buffer
    // Layout: [HEADER(32B)][lChild(4B)][attrVal(16B)][lChild(4B)][attrVal(16B)]...
    // Each entry occupies 20 bytes (4 for lChild + 16 for attrVal)
    // The rChild of entry i = lChild stored at start of entry i+1
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    // Step 5: copy each field individually (avoid alignment issues)
    memcpy(&(internalEntry->lChild),  entryPtr,      sizeof(int32_t)); // 4 bytes
    memcpy(&(internalEntry->attrVal), entryPtr + 4,  sizeof(Attribute)); // 16 bytes
    memcpy(&(internalEntry->rChild),  entryPtr + 20, sizeof(int32_t)); // 4 bytes
    // Note: rChild is at offset +20 because it's the lChild of the NEXT entry

    return SUCCESS;
}


// ===== IndInternal::setEntry() =====
// (Stub for now — will be implemented in later stage)
int IndInternal::setEntry(void *ptr, int indexNum) {
    return 0;
}


// ===== IndLeaf::getEntry() =====
// Copies the indexNum-th entry from a leaf index block into *ptr

int IndLeaf::getEntry(void *ptr, int indexNum) {

    // Step 1: validate indexNum range
    if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF) {
        return E_OUTOFBOUND;
    }

    // Step 2: get pointer to this block's buffer
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
        return ret;
    }

    // Step 3: calculate entry start position
    // LEAF_ENTRY_SIZE = 32 bytes per entry
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);

    // Step 4: copy the entire Index struct at once
    // (safe here because Index fields are packed same as on disk)
    memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}


// ===== IndLeaf::setEntry() =====
// (Stub for now — will be implemented in later stage)
int IndLeaf::setEntry(void *ptr, int indexNum) {
    return 0;
}