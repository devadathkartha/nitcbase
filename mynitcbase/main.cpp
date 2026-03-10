#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"

int main(int argc, char *argv[]) {
  // 1. Initialize the base layers
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  // 2. Hand over control to the interactive shell
  return FrontendInterface::handleFrontend(argc, argv);
}