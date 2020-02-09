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

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
    heapfile = new (std::nothrow) File();
    deltapage = new (std::nothrow) Page();
    readpage = new (std::nothrow) Page();
    filename = NULL;
    readPageNumber = -1;
}

DBFile::~DBFile () {
    delete heapfile;
    delete deltapage;
    delete readpage;
    delete filename;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if(f_type != heap) return 0;
    if (filename == NULL)
        filename = (char *) malloc(strlen(f_path) + 1);
    if (filename != NULL) {
        strcpy(filename, f_path);
        heapfile->Open(0, filename);
        return 1;
    }
    return 0;   // out of memory return value;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    Record rec;
    FILE *file = fopen(loadpath, "r");
    while (rec.SuckNextRecord(&f_schema, file)) {
        this->Add(rec);
    }
}

int DBFile::Open (const char *f_path) {
    if( !fileExists(f_path) ) return 0;
    if (filename == NULL) filename = (char *) malloc(strlen(f_path) + 1);
    if (filename != NULL) {
        strcpy(filename, f_path);
        heapfile->Open(1, filename);
        return 1;
    }
    return 0;
}

void DBFile::MoveFirst () {
    readPageNumber = -1;
    readpage->EmptyItOut();
}

// closes the file and returns the file length (in number of pages)
int DBFile::Close () {
    AddDeltaPageToFile();
    return heapfile->Close();
}

void DBFile::Add (Record &rec) {
    // printf("num records %d\n", deltapage->GetLength());
    int samepage = deltapage->Append(&rec);
    // printf("samepage %d\n", samepage);
    if (!samepage) {
        AddDeltaPageToFile();
        deltapage->Append(&rec);
    }
}

int DBFile::GetNext (Record &fetchme) {
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

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    Record rec;
    while (GetNext(rec)) {
        if (comp.Compare(&rec, &literal, &cnf)) {
            fetchme.Consume(&rec);
            return 1;
        }
    }
    return 0;
}

// adds the page holding recent changes/transactions
void DBFile::AddDeltaPageToFile() {
    if (deltapage->GetLength()) {
        heapfile->AddPage(deltapage, heapfile->GetLength());
        deltapage->EmptyItOut();
    }
}

bool DBFile::fileExists(const char *filePath) {
    std::ifstream ifile(filePath);
    return (bool)ifile;
}