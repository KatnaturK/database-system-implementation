#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

using namespace std;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

    class GenericDBFile {
    public:
        virtual int Create (const char *f_path, fType f_type, void *startup) = 0;
        virtual int Open (const char *f_path) = 0;
        virtual int Close () = 0;
        virtual void Load (Schema &f_schema, const char *loadpath) = 0;

        virtual void MoveFirst () = 0;
        virtual void Add (Record &addme) = 0;
        virtual int GetNext (Record &fetchme) = 0;
        virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
    };

    class Heap : public GenericDBFile {
        char *filename;
        File *heapfile;
        Page *deltapage;
        Page *readpage;
        ComparisonEngine comp;
        int readPageNumber;

        void AddDeltaPageToFile();
        bool fileExists(const char *filePath);

    public:
        Heap ();
        ~Heap ();

        int Create (const char *f_path, fType f_type, void *startup);
        void Load (Schema &f_schema, const char *loadpath);
        int Open (const char *f_path);
        void MoveFirst ();

        int Close ();
        void Add (Record &rec);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    };

    GenericDBFile *internalDBFile;

    char* GetMetafileName (const char *fpath);
    
    fType GetTypeFromMetafile (const char *fpath);


public:
    DBFile ();
    ~DBFile ();

    int Create (const char *fpath, fType file_type, void *startup);
    int Open (const char *fpath);
    int Close ();

    void Load (Schema &myschema, const char *loadpath);

    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
