#ifndef DBFILE_H
#define DBFILE_H

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "GenericDBFile.h"
#include "Pipe.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

struct runStruct {
        int runlength;
        fileTypeEnum filetype;
        OrderMaker sortOrder;
};

struct sortStruct {
        OrderMaker *sortOrder;
        int length;
};

class DBFile {

public:
        
        GenericDBFile* genericDBFile;
        
        DBFile ();
        ~DBFile();
        
        sortStruct *sort;

        void Add (Record &addme);
        int Close ();
        int Create (char *filePath, fileTypeEnum fileType, void *startup);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        void Load (Schema &myschema, char *loadpath);
        void MoveFirst ();
        int Open (char *filePath);
};
#endif

