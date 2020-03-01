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


void DBFile::HeapFile::AddDeltaPageToFile() {
    if (deltapage->GetLength()) {
        heapfile->AddPage(deltapage, heapfile->GetLength());
        deltapage->EmptyItOut();
    }
}

bool DBFile::HeapFile::fileExists(const char *filePath) {
    std::ifstream ifile(filePath);
    return (bool)ifile;
}

DBFile::HeapFile::HeapFile () {
    heapfile = new (std::nothrow) File();
    deltapage = new (std::nothrow) Page();
    readpage = new (std::nothrow) Page();
    filename = NULL;
    readPageNumber = -1;
}

DBFile::HeapFile::~HeapFile () {
    delete heapfile;
    delete deltapage;
    delete readpage;
    delete filename;
}

int DBFile::HeapFile::Create (const char *fpath, fType f_type, void *startup) {
    char *filename = strdup(fpath);
    if (filename != NULL) {
        strcpy(filename, fpath);
        heapfile->Open(0, filename);
        return 1;
    }
    return 0;   // out of memory return value;
}

void DBFile::HeapFile::Load (Schema &f_schema, const char *loadpath) {
    Record rec;
    FILE *file = fopen(loadpath, "r");
    while (rec.SuckNextRecord(&f_schema, file)) {
        this->Add(rec);
    }
}

int DBFile::HeapFile::Open (const char *f_path) {
    if( !fileExists(f_path) ) return 0;
    char *filename = strdup(f_path);
    if (filename != NULL) {
        strcpy(filename, f_path);
        heapfile->Open(1, filename);
        return 1;
    }
    return 0;
}

void DBFile::HeapFile::MoveFirst () {
    readPageNumber = -1;
    readpage->EmptyItOut();
}

// closes the file and returns the file length (in number of pages)
int DBFile::HeapFile::Close () {
    AddDeltaPageToFile();
    return heapfile->Close();
}

void DBFile::HeapFile::Add (Record &rec) {
    // printf("num records %d\n", deltapage->GetLength());
    int samepage = deltapage->Append(&rec);
    // printf("samepage %d\n", samepage);
    if (!samepage) {
        AddDeltaPageToFile();
        deltapage->Append(&rec);
    }
}

int DBFile::HeapFile::GetNext (Record &fetchme) {
    if (readpage->GetFirst(&fetchme) == 0) {
        if (readPageNumber <= heapfile->GetLength() - 3) {
            heapfile->GetPage(readpage, ++readPageNumber);
        }
        else {
            if (deltapage->GetLength()) {
                AddDeltaPageToFile();
                heapfile->GetPage(readpage, ++readPageNumber);
            }
            else
                return 0;
        }
        readpage->GetFirst(&fetchme);
    }
    return 1;
}

int DBFile::HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    Record rec;
    while (GetNext(rec)) {
        if (comp.Compare(&rec, &literal, &cnf)) {
            fetchme.Consume(&rec);
            return 1;
        }
    }
    return 0;
}
