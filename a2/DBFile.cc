#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

DBFile::DBFile () {
}

DBFile::~DBFile () {
}

int DBFile::Create (const char *fpath, fType file_type, void *startup) {
    ofstream metafile;
    char *metafilename = (char *) malloc(strlen(fpath) + strlen(".meta") + 1);
    strcpy(metafilename, fpath);
    strcat(metafilename, ".meta");

    metafile.open(metafilename);
    std::cout<<file_type;
    switch(file_type) {
        case heap:
            metafile << "heap";
            internalDBFile = new Heap();
            internalDBFile->Create(fpath, heap, NULL);
            break;
        case sorted:
            break;
        case tree:
            break;
    }
    metafile.close();
    return 1;
}

int DBFile::Open (const char *fpath) {

}

int DBFile::Close () {
    std::cout<< "CLOSE\n";
    return 1;
}

void DBFile::Load (Schema &myschema, const char *loadpath) {
    std::cout<< "LOAD\n";
}

void DBFile::MoveFirst () {

}

void DBFile::Add (Record &addme) {

}

int DBFile::GetNext (Record &fetchme) {

}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

}