#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {
  // Initialize all blocks as free
  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
    metainfo[bufferIndex].free = true;
  }
}

// The destructor is empty for now. 
// In Stage 4, we will add code here to save changes back to the disk when we exit!
StaticBuffer::~StaticBuffer() {}

int StaticBuffer::getFreeBuffer(int blockNum) {
  // DISK_BLOCKS is 8192 (0 to 8191)
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  
  int allocatedBuffer = -1;

  // 2. Iterate through the clipboard to find a free slot
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
      if (metainfo[i].free == true) {
          allocatedBuffer = i;
          break; // Found one! Stop searching.
      }
  }

  // Note: For this stage, we assume there is ALWAYS a free buffer.
  // (In later stages, if the buffer is full, we will have to "evict" an old block).

  // 3. Update the clipboard to say this slot is now taken
  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].blockNum = blockNum;

  // 4. Return the index of the slot (e.g., Slot 0, 1, 2...)
  return allocatedBuffer;
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