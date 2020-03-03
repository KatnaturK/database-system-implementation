#ifndef BIGQ_H
#define BIGQ_H

#include <algorithm>
#include <iostream>
#include <pthread.h>
#include<set>
#include <string>
#include <vector>

#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

class RecordOperator {

public:
        static int compareRecords (const void *rc1, const void *rc2);
        Record tmpRecord;
        OrderMaker *sortedOrder;
};

class CustomRecord {

public:
        Record newRecord;
        int runPosition;
        OrderMaker *sortedOrder;
};

class CustomPage {

public:
        Page page; 
        int currentPageNum; 
};

class RunPage {

public:
        int startPageNum;
        int endPageNum;
};

class BigQ {

        Pipe *in, *out;
        OrderMaker *sortOrder;
        int runLength;

public:
        BigQ (Pipe &in, Pipe &out, OrderMaker &sortOrder, int runlen);
	~BigQ ();

        char *tmpRunFile;
        File runFile;
        int currentPageNum;
        int runCount;
        vector<pair<int,int> > pages;
        vector<RunPage*> runPages;

        static void* worker (void* worker);
        
        void tmpmmsPhase1 ();
        void tmpmmsPhase2 ();
        void generateRuns (vector<RecordOperator*> records);
	
};
#endif
