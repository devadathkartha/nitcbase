#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

// declare the blockAllocMap array
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {
    // The BAM occupies the first 4 blocks on disk (blocks 0,1,2,3)
    // Each block is 2048 bytes, 4 blocks = 8192 bytes = DISK_BLOCKS bytes
    // We read them one block at a time into our blockAllocMap array

    for (int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE; i++) {
        // Disk::readBlock reads one block (2048 bytes) from disk
        // blockAllocMap + i*BLOCK_SIZE points to the right offset in the array
        Disk::readBlock(blockAllocMap + (i * BLOCK_SIZE), i);
    }

    // initialise all buffer slots as free and clean
    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
        metainfo[bufferIndex].free      = true;
        metainfo[bufferIndex].dirty     = false;
        metainfo[bufferIndex].timeStamp = -1;
        metainfo[bufferIndex].blockNum  = -1;
    }
}

// The destructor is empty for now. 
// In Stage 4, we will add code here to save changes back to the disk when we exit!
StaticBuffer::~StaticBuffer() {
    // Write the in-memory blockAllocMap back to disk blocks 0-3
    for (int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE; i++) {
        Disk::writeBlock(blockAllocMap + (i * BLOCK_SIZE), i);
    }
    // iterate through all buffer slots
    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {

        // only care about occupied AND dirty buffers
        if (metainfo[bufferIndex].free == false && 
            metainfo[bufferIndex].dirty == true) {

            // write the buffer contents back to the correct disk block
            Disk::writeBlock(blocks[bufferIndex], metainfo[bufferIndex].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum) {

    // Step 1: validate blockNum
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    // Step 2: increment timestamp of ALL occupied buffer slots
    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
        if (metainfo[bufferIndex].free == false) {
            metainfo[bufferIndex].timeStamp++;
        }
    }

    // Step 3: look for a free buffer slot
    int bufferNum = -1;

    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
        if (metainfo[bufferIndex].free == true) {
            bufferNum = bufferIndex;
            break;
        }
    }

    // Step 4: if no free slot, find the LRU buffer (highest timestamp)
    if (bufferNum == -1) {

        int maxTimeStamp = -1;

        for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
            if (metainfo[bufferIndex].timeStamp > maxTimeStamp) {
                maxTimeStamp = metainfo[bufferIndex].timeStamp;
                bufferNum = bufferIndex;
            }
        }

        // Step 5: if the LRU buffer is dirty, write it back to disk
        if (metainfo[bufferNum].dirty == true) {
            Disk::writeBlock(blocks[bufferNum], metainfo[bufferNum].blockNum);
        }
    }

    // Step 6: update metainfo for the newly assigned buffer slot
    metainfo[bufferNum].free      = false;
    metainfo[bufferNum].dirty     = false;
    metainfo[bufferNum].blockNum  = blockNum;
    metainfo[bufferNum].timeStamp = 0;

    return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum) {
  // 1. Sanity check
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  // 2. Scan the clipboard
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
      // If the slot is NOT free AND it holds the block we are looking for
      if (!metainfo[i].free && metainfo[i].blockNum == blockNum) {
          return i; // Return the slot index (e.g., "It's in slot 5!")
      }
  }

  // 3. If we finish the loop and didn't find it, tell the caller it's missing.
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum) {

    // Step 1: find which buffer slot holds this block
    int bufferNum = StaticBuffer::getBufferNum(blockNum);

    // Step 2: handle error cases from getBufferNum()
    if (bufferNum == E_BLOCKNOTINBUFFER) {
        return E_BLOCKNOTINBUFFER;
    }

    if (bufferNum == E_OUTOFBOUND) {
        return E_OUTOFBOUND;
    }

    // Step 3: set the dirty bit
    metainfo[bufferNum].dirty = true;

    return SUCCESS;
}


// Buffer/StaticBuffer.cpp

int StaticBuffer::getStaticBlockType(int blockNum) {

    // Step 1: validate blockNum
    // blockNum must be in range [0, DISK_BLOCKS - 1]
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    // Step 2: read from the block allocation map
    // blockAllocMap[blockNum] stores the type of block at that position
    // Types: UNUSED_BLK, BMAP, REC, IND_INTERNAL, IND_LEAF
    return (int)blockAllocMap[blockNum];
}