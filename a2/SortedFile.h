#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include <iostream>
#include <stdio.h>
#include <stdlib.h>

#include "GenericDBFile.h"

using namespace std;

typedef struct {
        Pipe *pipe;
        OrderMaker *sortOrder;
        Schema *fileSchema;
        File *file;
        char* loadPath;
} producer_util;

typedef enum {read, write} mode;


class SortedFile : virtual public  GenericDBFile {

public:
        SortedFile ();
        ~SortedFile();

        mode fileMode;

        void Add (Record &rec);
        int Close ();
        int Create (char *filePath, fileTypeEnum fileEnum, void *startup);
        void FixDirtyFile();
        int GetNext (Record &fetchMe);
        int GetNext (Record &fetchMe, CNF &cnf, Record &literal);
        void Load (Schema &fileSchema, char *loadpath);
        void MoveFirst ();
        int Open (char *filePath);
	void SwitchMode(mode nextMode);
};
#endif
