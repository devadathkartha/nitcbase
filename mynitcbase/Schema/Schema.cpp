#include "Schema.h"

#include <cmath>
#include <cstring>

int Schema::openRel(char relName[ATTR_SIZE]) {
    int ret = OpenRelTable::openRel(relName);

    if(ret >= 0){
        return SUCCESS;
    }

    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]) {
    if (strcmp(relName, RELCAT_RELNAME) == 0 || 
        strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {

    // Step 1: block renaming of catalog relations
    if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || 
        strcmp(oldRelName, ATTRCAT_RELNAME) == 0 ||
        strcmp(newRelName, RELCAT_RELNAME) == 0 ||
        strcmp(newRelName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    // Step 2: check if the relation is currently open
    // getRelId returns E_RELNOTOPEN if relation is NOT open
    // we want to PREVENT renaming if it IS open
    if (OpenRelTable::getRelId(oldRelName) != E_RELNOTOPEN) {
        return E_RELOPEN;
    }

    // Step 3: delegate to Block Access Layer
    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);

    return retVal;
}

int Schema::renameAttr(char relName[ATTR_SIZE], char oldAttrName[ATTR_SIZE], 
                       char newAttrName[ATTR_SIZE]) {

    // Step 1: block renaming attributes of catalog relations
    if (strcmp(relName, RELCAT_RELNAME) == 0 || 
        strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    // Step 2: check if the relation is currently open
    if (OpenRelTable::getRelId(relName) != E_RELNOTOPEN) {
        return E_RELOPEN;
    }

    // Step 3: delegate to Block Access Layer
    int retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);

    return retVal;
}

// Schema/Schema.cpp

int Schema::createRel(char relName[], int nAttrs, 
                       char attrs[][ATTR_SIZE], int attrtype[]) {

    // ─────────────────────────────────────────────
    // STEP 1: Check if a relation with this name already exists
    // ─────────────────────────────────────────────

    Attribute relNameAsAttribute;
    strcpy(relNameAsAttribute.sVal, relName);
    // We need to search RELATIONCAT for a record where
    // RelName == relName. To do a linearSearch, we need
    // the value as an Attribute union type.

    RecId targetRelId;

    // Reset the search index so linearSearch starts from the beginning
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Search RELATIONCAT for any existing relation with this name
    targetRelId = BlockAccess::linearSearch(
        RELCAT_RELID,           // search in relation catalog
        RELCAT_ATTR_RELNAME,    // field name = "RelName"
        relNameAsAttribute,     // value to match
        EQ                      // operator: equals
    );

    // If linearSearch found something (not {-1,-1}), relation already exists
    if (targetRelId.block != -1 && targetRelId.slot != -1) {
        return E_RELEXIST;
    }

    // ─────────────────────────────────────────────
    // STEP 2: Check for duplicate attribute names
    // ─────────────────────────────────────────────

    // Compare every pair of attribute names
    for (int i = 0; i < nAttrs; i++) {
        for (int j = i + 1; j < nAttrs; j++) {
            if (strcmp(attrs[i], attrs[j]) == 0) {
                return E_DUPLICATEATTR;
            }
        }
    }
    // e.g. create table T(name STR, name NUM) → caught here

    // ─────────────────────────────────────────────
    // STEP 3: Build and insert the RELATIONCAT record
    // ─────────────────────────────────────────────

    Attribute relCatRecord[RELCAT_NO_ATTRS]; // array of 6 Attribute values

    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
    // "RelName" field = the relation's name

    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    // "#Attributes" field = how many columns

    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    // "#Records" field = 0 (brand new, no data yet)

    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    // "FirstBlock" = -1 (no data blocks allocated yet)

    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    // "LastBlock" = -1 (no data blocks allocated yet)

    // "#Slots" = how many record slots fit in one block for this relation
    // Formula from physical layer: floor(2016 / (16 * nAttrs + 1))
    // Why 2016? Block size is 2048, header is 32 bytes → 2016 usable bytes
    // Each slot needs: 16 bytes per attribute × nAttrs, plus 1 byte for slotmap
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = 
        floor(2016.0 / (16 * nAttrs + 1));

    // Insert this record into RELATIONCAT
    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
    if (retVal != SUCCESS) {
        return retVal;
        // Could fail with E_DISKFULL if catalog is completely full
    }

    // ─────────────────────────────────────────────
    // STEP 4: Build and insert one ATTRIBUTECAT record per attribute
    // ─────────────────────────────────────────────

    for (int i = 0; i < nAttrs; i++) {

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS]; // array of 6 Attribute values

        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
        // "RelName" = which relation this attribute belongs to

        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
        // "AttributeName" = the column name (e.g. "rollNo", "name")

        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
        // "AttributeType" = NUMBER(0) or STRING(1)

        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        // "PrimaryFlag" = -1 (no primary key concept in NITCbase)

        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        // "RootBlock" = -1 (no B+ tree index yet)

        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;
        // "Offset" = position of this attribute in a record (0-indexed)

        retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);

        if (retVal != SUCCESS) {
            // IMPORTANT: We already inserted the RELATIONCAT entry!
            // We must undo that by deleting the partial relation.
            Schema::deleteRel(relName);
            return E_DISKFULL;
        }
    }

    return SUCCESS;
}

// Schema/Schema.cpp

int Schema::deleteRel(char *relName) {

    // ─────────────────────────────────────────────
    // CHECK 1: Prevent deletion of system catalogs
    // ─────────────────────────────────────────────

    if (strcmp(relName, RELCAT_RELNAME) == 0 || 
        strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }
    // RELCAT_RELNAME  = "RELATIONCAT"
    // ATTRCAT_RELNAME = "ATTRIBUTECAT"
    // Both defined in constants.h

    // ─────────────────────────────────────────────
    // CHECK 2: Is the relation currently open?
    // ─────────────────────────────────────────────

    // getRelId() searches the Open Relation Table for a relation with
    // this name. If found, it returns its relId (0 to MAX_OPEN-1).
    // If not found (relation is closed), it returns E_RELNOTOPEN.
    int relId = OpenRelTable::getRelId(relName);

    if (relId != E_RELNOTOPEN) {
        // getRelId() succeeded → relation IS open → cannot delete
        return E_RELOPEN;
    }

    // ─────────────────────────────────────────────
    // HAND OFF to Block Access Layer
    // ─────────────────────────────────────────────

    // At this point we know:
    // 1. It's not a system catalog
    // 2. It's not currently open
    // So we let BlockAccess do the actual deletion
    int retVal = BlockAccess::deleteRelation(relName);

    return retVal;
    // Possible return values from deleteRelation():
    // SUCCESS        → relation deleted successfully
    // E_RELNOTEXIST  → relation was never created in the first place
}