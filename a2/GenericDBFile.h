#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>
typedef enum {heap, sorted, tree} fType;

class GenericDBFile {

public:
fstream fstatus;
File f;
Page p;
int pageNumber;
int pageCount;
bool pageInRead;
bool pageInWrite;
FILE *tableFile;
Record *newRecord;
OrderMaker sort_order;
int runlen;
bool dirty;
ComparisonEngine comp;


        GenericDBFile ();
        virtual int Create (char *fpath, fType file_type, void *startup);
        virtual int Open (char *fpath);
        virtual int Close ();
        virtual void Load (Schema &myschema, char *loadpath);
        virtual void MoveFirst ();
        virtual void Add (Record &addme);
        virtual int GetNext (Record &fetchme);
        virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        

       virtual ~GenericDBFile ();
};
#endif
