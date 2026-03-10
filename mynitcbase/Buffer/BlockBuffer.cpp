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
  struct HeadInfo head;
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  // 1. Read the block
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  // 2. Calculate the offset
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = buffer + HEADER_SIZE + slotCount + (recordSize * slotNum);

  // 3. Copy caller's data (rec) INTO the buffer
  memcpy(slotPointer, rec, recordSize);

  // 4. Write the modified buffer back to disk
  Disk::writeBlock(buffer, this->blockNum);

  return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  // 1. CACHE HIT CHECK: Is the block already on our "bookshelf"?
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  // 2. CACHE MISS: It's not in the buffer! We need to fetch it from disk.
  if (bufferNum == E_BLOCKNOTINBUFFER) {
    
    // Find an empty slot on the shelf
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND; // Block number was invalid
    }

    // Read directly from the disk into the assigned slot in the StaticBuffer
    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // 3. SUCCESS: Whether it was a Hit or a Miss, `bufferNum` now holds the correct index.
  // We point the caller's pointer directly to that row in our 2D array.
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