#include <algorithm>
#include <pthread.h>
#include <queue> 
#include <vector>

#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	if(runlen < 1) throw std::runtime_error("BigQ: runlen must be > 0");

	this->in = &in;
	this->out = &out;
	this->runCount = 0;
	this->runLength = runLength;
	this->sortOrder = &sortorder;

    pthread_t workerThread;
	int ret = pthread_create(&workerThread, NULL, &worker, (void*) this);

	out.ShutDown ();
}

void* BigQ :: worker (void *workerThread) {
	BigQ *bigQWorker = (BigQ*) workerThread;
	bigQWorker->tpmmsPhase1 ();
	bigQWorker->in->ShutDown ();
	cout << "Shutting Down Input Pipe." << endl;
	bigQWorker->tpmmsPhase2();
	delete bigQWorker;
	pthread_exit(NULL);
}

bool OrderedRecord :: compareRecords (OrderedRecord *orderedRecord1, OrderedRecord *orderedRecord2) {
        ComparisonEngine comparisonEngine;
        if( comparisonEngine.Compare (orderedRecord1->record, orderedRecord2->record, orderedRecord1->sortOrder) < 0)
                return true;
	   return false;
}

void BigQ :: tpmmsPhase1 () {
	int pageCount = 0, recordCount = 0;
	Page currPage;
	Record *currRecord = new Record;
	vector<OrderedRecord *> orderedRecords;
	while (in->Remove (currRecord)) {
		OrderedRecord *orderedRecord = new OrderedRecord (currRecord, sortOrder);
		if ( !currPage.Append (currRecord)) {
			pageCount++;
			if ( runLength == pageCount) {
				sort (orderedRecords.begin (), orderedRecords.end (), OrderedRecord::compareRecords);
				generateRuns (orderedRecords);
				pageCount = 0;
				orderedRecords.clear ();
			} else {
				currPage.EmptyItOut ();
				currPage.Append (currRecord);
			}
		}
		orderedRecords.push_back (orderedRecord);
		runCount++;
	}
	if (orderedRecords.size() != 0) {
		sort (orderedRecords.begin(), orderedRecords.end(), OrderedRecord::compareRecords);
		generateRuns (orderedRecords);
		orderedRecords.clear ();
	}
}

void BigQ :: generateRuns (vector<OrderedRecord *> sortedRecords) {
	pageCount++;
	Page currPage;
	PageRun *pageRun = new PageRun;
	vector<OrderedRecord *>::iterator orderedRecord = sortedRecords.begin();
	pageRun->startCount = pageCount;
	for (int i = 0; i < sortedRecords.size (); i++) {
		if (!currPage.Append ((* orderedRecord)->record)) {
			runFile.AddPage (&currPage, pageCount++);
			currPage.EmptyItOut ();
			currPage.Append ((* orderedRecord)->record);
		}
		orderedRecord++;
	}
	runFile.AddPage (&currPage, pageCount);
	currPage.EmptyItOut ();
	pageRun->endCount = pageCount++;
	pageRuns.push_back(pageRun);
}


void BigQ :: tpmmsPhase2 () { 
	
}

BigQ::~BigQ () {
}
