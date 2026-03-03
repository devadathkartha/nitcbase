#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache; // This single line now loads 0, 1, AND 2 into the cache!

  // Loop through rel-id 0 (RelationCat), 1 (AttributeCat), and 2 (Students)
  for (int i = 0; i <= 2; i++) { 
      RelCatEntry relCatEntry;
      RelCacheTable::getRelCatEntry(i, &relCatEntry);
      
      printf("Relation: %s\n", relCatEntry.relName);

      for (int j = 0; j < relCatEntry.numAttrs; j++) {
          AttrCatEntry attrCatEntry;
          AttrCacheTable::getAttrCatEntry(i, j, &attrCatEntry);

          const char *attrType = attrCatEntry.attrType == NUMBER ? "NUM" : "STR";
          printf("  %s: %s\n", attrCatEntry.attrName, attrType);
      }
      printf("\n");
  }

  return 0;
}