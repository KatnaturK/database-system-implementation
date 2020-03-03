#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <string>
#include <algorithm>
using namespace std;

class recordsW {
public:
        Record newRecord;
        int runPosition;
        OrderMaker *sortedOrder;
};


class recordsW1 {
public:
        static int compareRecords(const void *rc1, const void *rc2);
        Record tmpRecord;
        OrderMaker *sortedOrder;
};

class runMetaData {
public:
        int startPage, endPage;
};

class BigQ {

        Pipe *inputPipe, *outputPipe;
        OrderMaker *sort_order;
        int runLength;

public:


       static void* sortingWorker(void* threadid);


        void writeInFile(vector<recordsW1*> rcVector);
        void sortRecords();
        void mergeRecords();
        vector<pair<int,int> > pageBegin;
        vector<runMetaData*> runmetaDataVec;
         
        File sortedFile;
        char *fileName;
        int currentPageNum, numberRuns;
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

class pageWrapper {
public:
        Page newPage; 
        int currentPage; 
};
#endif
