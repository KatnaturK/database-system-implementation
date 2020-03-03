#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "GenericDBFile.h"

class HeapFile : virtual public GenericDBFile {

public:
        HeapFile ();
        ~HeapFile();

        void Add (Record &addme);
        int Close ();
        int Create (char *filePath, fileTypeEnum fileType, void *startup);
        int GetNext (Record &fetchMe);
        int GetNext (Record &fetchMe, CNF &cnf, Record &literal);   
        void Load (Schema &fileSchema, char *loadPath);
        void MoveFirst ();
        int Open (char *filePath);
};
#endif
