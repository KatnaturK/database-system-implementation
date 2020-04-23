#include <cstdlib>
#include <iostream>
#include <string.h>
#include <unistd.h>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "DBFile.h"
#include "Meta.h"

using namespace std;


extern struct AndList *final;

DBFile::DBFile() {
}

DBFile::~DBFile() {
}

GenericDBFile::GenericDBFile() {
    readingPage = *(new Page());
    writingPage = *(new Page());
    myOrder = new(OrderMaker);
}

int GenericDBFile::Create(const char *f_path, fType type, void *startup) {
    file.Open(0, strdup(f_path));
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    file.AddPage(&writingPage, -1);
    WriteMetaInfo(f_path, type, startup);
    if (startup != NULL) {
        myOrder = ((SortInfo *) startup)->myOrder;
        runLength = ((SortInfo *) startup)->runLength;
    }
    MoveFirst();
    return 1;
}

int DBFile::Create(const char *f_path, fType type, void *startup) {
    if (type == heap) {
        myInternalVar = new Heap();
    } else if (type == sorted) {
        myInternalVar = new Sorted();
    }
    return myInternalVar->Create(f_path, type, startup);
}

int DBFile::Open(const char *f_path) {
    MetaInfo metaInfo = GetMetaInfo();

    if (f_path != metaInfo.binFilePath) {
        cout << "input file: " << f_path << endl;
        cout << "meta input file: " << metaInfo.binFilePath << endl;

        cout << "DbFile Open called without calling Create!!" << endl;
        WriteMetaInfo(f_path, heap, NULL);
        metaInfo = GetMetaInfo();
    }

    if (metaInfo.fileType == heap) {
        myInternalVar = new Heap();
    } else if (metaInfo.fileType == sorted) {
        myInternalVar = new Sorted();
    } else {
        return 0;
    }
    return myInternalVar->Open(f_path);
}

int GenericDBFile::Open(const char *fpath) {
    MetaInfo metaInfo = GetMetaInfo();
    myOrder = metaInfo.sortInfo->myOrder;
    file.Open(1, strdup(fpath));
    writingPage.EmptyItOut();
    readingPage.EmptyItOut();
    return 1;
}

void Heap::readingMode() {
    if (mode == reading) return;
    mode = reading;
    file.AddPage(&writingPage, file.GetLength() - 1);
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    writingPage.EmptyItOut();
}

void Heap::writingMode() {
    if (mode == writing) return;
    mode = writing;
}

void Sorted::writingMode() {
    if (mode == writing) return;
    mode = writing;
    bigQ = new BigQ(inPipe, outPipe, *myOrder, runLength);
}

//todo change
void Sorted::readingMode() {
    if (mode == reading)
        return;

    mode = reading;
    MoveFirst();
    inPipe.ShutDown();
    writingPage.EmptyItOut();

    Record recFromPipe;
    Record recFromPage;
    bool pipeEmpty = outPipe.Remove(&recFromPipe) == 0;
    bool fileEmpty = GetNext(recFromPage) == 0;

    while (!pipeEmpty && !fileEmpty) {
        if (compEngine.Compare(&recFromPipe, &recFromPage, myOrder) <= 0) {
            if (!writingPage.Append(&recFromPipe)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
            pipeEmpty = outPipe.Remove(&recFromPipe) == 0;
        } else {
            if (!writingPage.Append(&recFromPage)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
            fileEmpty = GetNext(recFromPage) == 0;
        }
    }
    if (pipeEmpty && !fileEmpty) {
        if (!writingPage.Append(&recFromPage)) {
            file.AddPage(&writingPage, file.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPage);
        }
        while (GetNext(recFromPage)) {
            if (!writingPage.Append(&recFromPage)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
        }
    } else if (!pipeEmpty) {
        if (!writingPage.Append(&recFromPipe)) {
            file.AddPage(&writingPage, file.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPipe);
        }
        while (outPipe.Remove(&recFromPipe)) {
            if (!writingPage.Append(&recFromPipe)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
        }
    }

    delete bigQ;

    if (!pipeEmpty || !fileEmpty)
        file.AddPage(&writingPage, file.GetLength() - 1);
}


void DBFile::Load(Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

void Heap::Load(Schema &f_schema, const char *loadpath) {
    writingMode();
    FILE *tableFile = fopen(loadpath, "r");
    if (tableFile == NULL) {
        cerr << "invalid table file" << endl;
        exit(-1);
    }

    Record tempRecord;
    long recordCount = 0;
    long pageCount = file.GetLength() - 1;

    int isNotFull = 0;
    while (tempRecord.SuckNextRecord(&f_schema, tableFile) == 1) {
        recordCount++;
        isNotFull = writingPage.Append(&tempRecord);
        if (!isNotFull) {
            file.AddPage(&writingPage, ++pageCount);
            curPageIndex++;
            writingPage.EmptyItOut();
            writingPage.Append(&tempRecord);
        }
    }
    if (!isNotFull)
        file.AddPage(&writingPage, 0);
    fclose(tableFile);
    cout << "loaded " << recordCount << " records into " << pageCount << " pages." << endl;
}

void Sorted::Load(Schema &schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");
    int numofRecords = 0;
    if (tableFile == NULL) {
        cerr << "invalid load_path" << loadpath << endl;
        exit(1);
    } else {
        Record tempRecord = Record();
        writingMode();
        while (tempRecord.SuckNextRecord(&schema, tableFile)) {
            inPipe.Insert(&tempRecord);
            ++numofRecords;
        }
        fclose(tableFile);
        cout << "Loaded " << numofRecords << " records" << endl;
    }
}

void DBFile::MoveFirst() {
    myInternalVar->MoveFirst();
}

void GenericDBFile::MoveFirst() {
    readingMode();
    readingPage.EmptyItOut();
    delete queryOrder;
    delete literalOrder;
    curPageIndex = -1;
}

int DBFile::Close() {
    return myInternalVar->Close();
}

int GenericDBFile::Close() {
    readingMode();
    return file.Close() > 0 ? 1 : 0;
}

void DBFile::Add(Record &record) {
    myInternalVar->Add(record);
}

void Heap::Add(Record &record) {
    writingMode();
    int isFull = writingPage.Append(&record);

    if (isFull == 0) {
        if (file.GetLength() == 0)
            file.AddPage(&writingPage, 0);
        else {
            file.AddPage(&writingPage, file.GetLength() - 1);
        }
        writingPage.EmptyItOut();
        writingPage.Append(&record);
    }
}

void Sorted::Add(Record &rec) {
    writingMode();
    inPipe.Insert(&rec);
}

int DBFile::GetNext(Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}

int Heap::GetNext(Record &fetchme) {
    if (mode == writing) {
        readingMode();
    }
    while (!readingPage.GetFirst(&fetchme)) {
        if (curPageIndex == file.GetLength() - 2) {
            return 0;
        } else {
            file.GetPage(&readingPage, ++curPageIndex);
        }
    }
    return 1;
}

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (mode == writing)
        readingMode();

    while (GetNext(fetchme))
        if (compEngine.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}

int Sorted::GetNext(Record &fetchme) {
//    cout<<"ok"<<endl;
//    cout<<mode<<endl;
    if (mode == writing)
        readingMode();

//    cout<<"ok"<<endl;
    while (!readingPage.GetFirst(&fetchme)) {

//        cout<<"ok"<<endl;
        if (curPageIndex == file.GetLength() - 2)
            return 0;
        else
            file.GetPage(&readingPage, ++curPageIndex);
    }
    return 1;
}

//binary search to boil down the search to a specific page
int Sorted::binarySearch(Record &fetchme, Record &literal) {
    ComparisonEngine comparisonEngine;
    if (!GetNext(fetchme))
        return 0;
    int compare = comparisonEngine.Compare(&fetchme, queryOrder, &literal, literalOrder);
    if (compare > 0) return 0;
    else if (compare == 0) return 1;

    off_t low = curPageIndex, high = file.GetLength() - 2, mid = (low + high) / 2;
//    cout << "binarySearch:   low" << low << "    high: " << high << endl;
    while (low < mid) {
        mid = (low + high) / 2;
//        cout << "binarySearch:   low" << low << "    high: " << high << endl;
        file.GetPage(&readingPage, mid);
        if (!GetNext(fetchme))
            exit(1);
        compare = comparisonEngine.Compare(&fetchme, queryOrder, &literal, literalOrder);
        if (compare < 0) low = mid;
        else if (compare > 0) high = mid - 1;
        else high = mid;
    }
}

//construct ordermaker instances from the cnf
void Sorted::constructQueryOrder(CNF &cnf, Record &literal) {
    queryOrder = new OrderMaker();
    literalOrder = new OrderMaker();
    for (int i = 0; i < myOrder->numAtts; ++i) {
        bool flag = false;
        int index = 0;
        for (int j = 0; j < cnf.numAnds && !flag; ++j) {
            for (int k = 0; k < cnf.orLens[j] && !flag; ++k) {
                if (cnf.orList[j][k].operand1 == Literal) {
                    if (cnf.orList[j][k].operand2 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt2 &&
                        cnf.orList[j][k].op == Equals) {
                        flag = true;

                        queryOrder->whichAtts[queryOrder->numAtts] = myOrder->whichAtts[i];
                        queryOrder->whichTypes[queryOrder->numAtts] = myOrder->whichTypes[i];
                        ++queryOrder->numAtts;

                        literalOrder->whichAtts[literalOrder->numAtts] = index;
                        literalOrder->whichTypes[literalOrder->numAtts] = cnf.orList[j][k].attType;
                        ++literalOrder->numAtts;
                    }
                    ++index;
                } else if (cnf.orList[j][k].operand2 == Literal) {
                    if (cnf.orList[j][k].operand1 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt1 &&
                        cnf.orList[j][k].op == Equals) {
                        flag = true;

                        queryOrder->whichAtts[queryOrder->numAtts] = myOrder->whichAtts[i];
                        queryOrder->whichTypes[queryOrder->numAtts] = myOrder->whichTypes[i];
                        ++queryOrder->numAtts;

                        literalOrder->whichAtts[literalOrder->numAtts] = index;
                        literalOrder->whichTypes[literalOrder->numAtts] = cnf.orList[j][k].attType;
                        ++literalOrder->numAtts;
                    }
                    ++index;
                }
            }
        }
        if (!flag) break;
    }
}

int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (queryOrder == NULL || literalOrder == NULL) {  // check whether this is a fresh call
        constructQueryOrder(cnf, literal);

        if (queryOrder->numAtts != 0)
            if (!binarySearch(fetchme, literal)) // binary search to pin down to the page
                return 0;
    }

    while (GetNext(fetchme)) { // linear search to inside the page
        if (compEngine.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    }

    return 0;
}