#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

typedef enum {
    heap, sorted, tree
} fType;
typedef enum {
    reading, writing
} mType;

struct SortInfo {
    OrderMaker *myOrder;  // Order of sorted DBFile
    int runLength;  // runlength for BigQ
};

class GenericDBFile {
protected:
    File file;
    Page readingPage;
    Page writingPage;
    off_t curPageIndex = -1;
    ComparisonEngine compEngine;

    //todo can be moved to sorteddbfile
    OrderMaker* myOrder;
    OrderMaker *queryOrder = NULL;
    OrderMaker *literalOrder = NULL;

    virtual void readingMode() = 0;
//    virtual void writingMode() = 0;

public:
    int runLength;
    mType mode = reading;
    virtual void writingMode() = 0;
    GenericDBFile();
    int Create(const char *fpath, fType f_type, void *startup);
    int Open(const char *fpath);
    int Close();
    void MoveFirst();
    virtual void Load(Schema &myschema, const char *loadpath) = 0;
    virtual void Add(Record &addme) = 0;
    virtual int GetNext(Record &fetchme) = 0;
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class Heap : public GenericDBFile {
private:
    void readingMode();
    void writingMode();

public:
    void Load(Schema &myschema, const char *loadpath);
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class Sorted : public GenericDBFile {
private:
    Pipe inPipe = Pipe(100);
    Pipe outPipe = Pipe(100);
    BigQ *bigQ = NULL;

    void readingMode();
    void writingMode();
    void constructQueryOrder(CNF &cnf, Record &literal);
    int binarySearch(Record& fetchme, Record& literal);

public:
    void Load(Schema &myschema, const char *loadpath);
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class BPlus : public GenericDBFile {
private:
    void readingMode();
    void writingMode();

public:
    void Load(Schema &myschema, const char *loadpath);
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class DBFile {
public:
    DBFile();
    ~DBFile();
    GenericDBFile *myInternalVar;
    int Create(const char *fpath, fType file_type, void *startup);
    int Open(const char *fpath);
    int Close();
    void Load(Schema &myschema, const char *loadpath);
    void MoveFirst();
    void Add(Record &record);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif