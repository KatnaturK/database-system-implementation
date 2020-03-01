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
    char *metafilename = GetMetafileName(fpath);
    metafile.open(metafilename);

    switch(file_type) {
        case heap:
            metafile << "heap\n";
            internalDBFile = new HeapFile();
            metafile.close();
            return internalDBFile->Create(fpath, heap, NULL);

        case sorted:
            metafile << "sorted\n";
            {
                SortInfo sortinfo = *(SortInfo *) startup;
                OrderMaker *o = sortinfo.myOrder;
                // memset(o, 0, sizeof(OrderMaker));

                metafile << sortinfo.runlength << "\n";
                metafile << *o;
                internalDBFile = new SortedFile(sortinfo.runlength, sortinfo.myOrder);   
            }
            return internalDBFile->Create(fpath, sorted, startup);

        case tree:
            break;
        default:
            std::cerr << "Incorrect file type\n";
    }
    return 0;
}

int DBFile::Open (const char *fpath) {
    fType ftype = GetTypeFromMetafile(fpath);
    switch (ftype) {
        case heap:
            internalDBFile = new HeapFile();
            break;
        case sorted:
        {
            int runlength = GetRunlengthFromMetafile(fpath);
            OrderMaker *sort_order = GetSortOrderFromMetafile(fpath);
            internalDBFile = new SortedFile(runlength, sort_order);
        }
            break;
        case tree:
            break;
        default:
            std::cerr << "Incorrect file type\n";
            return 0;
    }
    return internalDBFile->Open(fpath);
}

int DBFile::Close () {
    return internalDBFile->Close();
}

void DBFile::Load (Schema &myschema, const char *loadpath) {
    internalDBFile->Load(myschema, loadpath);
}

void DBFile::MoveFirst () {
    internalDBFile->MoveFirst();
}

void DBFile::Add (Record &addme) {
    internalDBFile->Add(addme);
}

int DBFile::GetNext (Record &fetchme) {
    return internalDBFile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return internalDBFile->GetNext(fetchme, cnf, literal);
}

char* DBFile::GetMetafileName (const char *fpath) {
    char *metafilename = (char *) malloc(strlen(fpath) + strlen(".meta") + 1);
    strcpy(metafilename, fpath);
    strcat(metafilename, ".meta");
    return metafilename;
}

fType DBFile::GetTypeFromMetafile (const char *fpath) {
    ifstream metafile(GetMetafileName(fpath));
    string line;
    if (metafile.good()) {
        getline(metafile, line);
    }
    metafile.close();
    if (line == "heap") return heap;
    if (line == "sorted") return sorted;
    if (line == "tree") return tree;
    std::cout << line;
    std::cerr << "Something's wrong with metafile!!\n"; 
    return heap; 
}

int DBFile::GetRunlengthFromMetafile (const char *fpath) {
    ifstream metafile(GetMetafileName(fpath));
    string line;
    int runlength = 0;
    if (metafile.good()) {
        getline(metafile, line);
        metafile >> runlength;
    }
    metafile.close();
    return runlength;
}

OrderMaker* DBFile::GetSortOrderFromMetafile (const char *fpath) {
    ifstream metafile(GetMetafileName(fpath));
    OrderMaker *sort_order = new OrderMaker();
    string line;
    if (metafile.good()) {
        getline(metafile, line);
        getline(metafile, line);
    }
    metafile >> (*sort_order);
    metafile.close();
    return sort_order;   
}