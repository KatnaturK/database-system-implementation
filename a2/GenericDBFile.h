#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H

#include <fstream>

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Pipe.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

typedef enum {heap, sorted, tree} fileTypeEnum;

class GenericDBFile {

public:
        GenericDBFile ();
        virtual ~GenericDBFile ();

        bool readPage;
        bool writePage;
        ComparisonEngine ce;
        fstream fstatus;
        int runlen;
        int pageNumber;
        int pageCount;
        File runFile;
        FILE *tableFile;
        OrderMaker sortOrder;
        Page p;
        Record *newRecord;
        
        virtual void Add (Record &rec);
        virtual int Close ();
        virtual int Create (char *filePath, fileTypeEnum fileType, void *startup);
        virtual int GetNext (Record &fetchMe);
        virtual int GetNext (Record &fetchMe, CNF &cnf, Record &literal);
        virtual void Load (Schema &fileSchema, char *loadPath);
        virtual void MoveFirst ();
        virtual int Open (char *filePath);
};
#endif
