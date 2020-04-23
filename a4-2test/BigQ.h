#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparison.h"

#include <vector>
#include <queue>
#include <algorithm>

class BigQ
{

public:

    BigQ (Pipe &in, Pipe &out, OrderMaker &orderMaker, int runlen);
    BigQ();
    ~ BigQ();

    static void* workerThread(void* arg);
    void dumpSortedList(std::vector<Record*>& recordList);
    void phaseOne();
    void phaseTwo();
    string getRandStr(int length);
    void init();
    void close();
    Pipe* inPipe;
    Pipe* outPipe;
    int blockNum = 0;
    vector<off_t> blockStartOffset;
    vector<off_t> blockEndOffset;
    File file;


private:
    OrderMaker* maker;
    int runlen;

    pthread_t worker_thread;

    class Compare{
        ComparisonEngine CmpEng;
        OrderMaker& CmpOrder;

    public:
        Compare(OrderMaker& orderMaker): CmpOrder(orderMaker) {}
        bool operator()(Record* a, Record* b){return CmpEng.Compare(a, b, &CmpOrder) < 0;}
    };

    class IndexedRecord{
    public:
        Record record;
        int blockIndex;
    };

    class IndexedRecordCompare{
        ComparisonEngine comparisonEngine;
        OrderMaker& orderMaker;

    public:
        IndexedRecordCompare(OrderMaker& orderMaker): orderMaker(orderMaker) {}

        bool operator()(IndexedRecord* a, IndexedRecord* b){return comparisonEngine.Compare(&(a->record), &(b->record), &orderMaker) > 0;}
    };
};


#endif