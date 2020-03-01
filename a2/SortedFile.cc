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

void DBFile::SortedFile::initializePipes() {
    in_pipe = new (std::nothrow) Pipe(100);
    out_pipe = new (std::nothrow) Pipe(100);
}

void DBFile::SortedFile::initializeBigQ() {
    diff_file = new (std::nothrow) BigQ(*in_pipe, *out_pipe, *sort_order, runlength);
}

void DBFile::SortedFile::deleteBigQ() {
    delete in_pipe;
    delete out_pipe;
    delete diff_file;
}

void DBFile::SortedFile::addRecordToFile(File *file, Page *writePage, Record *rec) {
    int samepage = writePage->Append(rec);
    if (!samepage) {
        file->AddPage(writePage, file->GetLength());
        writePage->EmptyItOut();
        writePage->Append(rec);
    }
}

void DBFile::SortedFile::mergeDiffFile() {
    if (!readmode) {
        in_pipe->ShutDown();
        initializeBigQ();

        File *resultfile = new File();
        char tempFile[100];
        sprintf(tempFile,"tmp%d.bin",2);
        resultfile->Open(0, tempFile);
        
        Page *writePage = new Page();
        int total_records = 0;

        Record rec1;
        Record rec2;
        int file_completed = 0;
        int r1 = 0, r2 = 0;

        off_t currPageNum = -1;
        Page *currPage;

        while (true) {
            std::cout << "LOOP START\n";
            // Read rec1 from out_pipe of diff file
            if (file_completed != 1 && !r1) {
                r1 = out_pipe->Remove(&rec1);
                std::cout << "FIRST REMOVED. TOTAL RECORDS = " << total_records << "\n";
            }
            // Read rec2 from internal sorted file
            if (file_completed != 2 && !r2) {
                r2 = 1;
                if (currPage->GetFirst(&rec2) == 0) {
                    if (currPageNum <= sortedfile->GetLength() - 3) {
                        sortedfile->GetPage(currPage, ++currPageNum);
                        currPage->GetFirst(&rec2);
                    }
                    else
                        r2 = 0;
                }

            }
            if (r1 && r2) {
                if (comp.Compare(&rec1, &rec2, sort_order) <= 0) {
                    addRecordToFile(resultfile, writePage, &rec1);
                    total_records++;
                    r1 = 0;
                } else {
                    addRecordToFile(resultfile, writePage, &rec2);
                    total_records++;
                    r2 = 0;
                }
            }
            else if (!r1 && r2) {
                file_completed = 1;
                // out_pipe->ShutDown();
                addRecordToFile(resultfile, writePage, &rec2);
                r2 = 0;
                total_records++;
            }
            else if (r1 && !r2) {
                file_completed = 2;
                addRecordToFile(resultfile, writePage, &rec1);
                r1 = 0;
                total_records++;
            }
            else {
                break;
            }

        }
        std::cout << "Total records merged : " << total_records << std::endl;
        delete sortedfile;
        sortedfile = resultfile;

        deleteBigQ();
        readmode = true;
    }
}

bool DBFile::SortedFile::fileExists(const char *filePath) {
    std::ifstream ifile(filePath);
    return (bool)ifile;
}

DBFile::SortedFile::SortedFile (int rl, OrderMaker *o) {
    sort_order = o;
    filename = NULL;
    readmode = true;
    runlength = rl;
    readPageNumber = -1;
    sortedfile = new (std::nothrow) File();
    readpage = new (std::nothrow) Page();
}

DBFile::SortedFile::~SortedFile () {
    delete sortedfile;
    if (!readmode)
        deleteBigQ();
    delete filename;
}

int DBFile::SortedFile::Create (const char *fpath, fType f_type, void *startup) {
    filename = strdup(fpath);
    if (filename != NULL) {
        strcpy(filename, fpath);
        sortedfile->Open(0, filename);
        return 1;
    }
    return 0;   // out of memory return value;
}

void DBFile::SortedFile::Load (Schema &f_schema, const char *loadpath) {
    Record rec;
    FILE *file = fopen(loadpath, "r");
    while (rec.SuckNextRecord(&f_schema, file)) {
        this->Add(rec);
    }
}

int DBFile::SortedFile::Open (const char *f_path) {
    if( !fileExists(f_path) ) return 0;
    filename = strdup(f_path);
    if (filename != NULL) {
        strcpy(filename, f_path);
        sortedfile->Open(1, filename);
        return 1;
    }
    return 0;
}

void DBFile::SortedFile::MoveFirst () {
    if (!readmode) {
        mergeDiffFile();
    }
    readPageNumber = -1;
    readpage->EmptyItOut();
}

// closes the file and returns the file length (in number of pages)
int DBFile::SortedFile::Close () {
    if (!readmode) {
        mergeDiffFile();
    }
    return sortedfile->Close();
}

void DBFile::SortedFile::Add (Record &rec) {
    if (readmode) {
        readmode = false;
        initializePipes();
    }
    in_pipe->Insert(&rec);
}

int DBFile::SortedFile::GetNext (Record &fetchme) {
    if (!readmode) {
        mergeDiffFile();
    }
    if (readpage->GetFirst(&fetchme) == 0) {
        if (readPageNumber <= sortedfile->GetLength() - 3) {
            sortedfile->GetPage(readpage, ++readPageNumber);
            readpage->GetFirst(&fetchme);
        }
        else
            return 0;
    }
    return 1;
}

int DBFile::SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if (!readmode) {
        mergeDiffFile();
    }
    // Record rec;
    // while (GetNext(rec)) {
    //     if (comp.Compare(&rec, &literal, &cnf)) {
    //         fetchme.Consume(&rec);
    //         return 1;
    //     }
    // }
    return 0;
}
