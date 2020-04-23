
#ifndef META_H
#define META_H

#include "DBFile.h"
#include <fstream>
#include <unistd.h>

struct MetaInfo {
    fType fileType;
    string binFilePath;
    SortInfo* sortInfo;
};

MetaInfo GetMetaInfo();

void WriteMetaInfo(string path, fType type, void *startup);

char* GetTempPath();

#endif