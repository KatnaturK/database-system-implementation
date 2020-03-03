#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

struct metadata {
        int runlength;
        OrderMaker sorting_order;
        fType filetype;
};

struct sortinfo {
        OrderMaker *ord;
        int l;
};


class DBFile {

public:

        GenericDBFile* myInternalVar;
        DBFile ();
        ~DBFile();

        int Create (char *fpath, fType file_type, void *startup);
        int Open (char *fpath);
        int Close ();
        sortinfo *mysrt;

        void Load (Schema &myschema, char *loadpath);

        void MoveFirst ();
        void Add (Record &addme);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif

