 #include <queue> 
 #include <algorithm>

#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	if(runlen < 1) throw std::runtime_error("BigQ: runlen must be > 0");

	this-> in = &in;
	this-> out = &out;
	this->runCount = 0;
	this->runLength = runLength;
	BigQ::sortOrder = &sortorder;

    pthread_t workerThread;
	int ret = pthread_create(&workerThread, NULL, &worker, (void*) this);

	out.ShutDown ();
}

void* BigQ :: worker (void *workerThread) {
	BigQ *bigQWorker = (BigQ*) workerThread;
	bigQWorker->tppmsPhase1 ();

	delete bigQWorker;
	pthread_exit(NULL);
}

bool BigQ :: compareRecords (Record *record1, Record *record2) {
        ComparisonEngine comparisonEngine;
        if( comparisonEngine.Compare (record1, record2, BigQ::sortOrder) < 0)
                return true;
	   return false;
}

void BigQ :: tppmsPhase1 () {
	int recordCount = 0;
	vector<Record *> records;
	Record *currRecord = new Record;
	while (in->Remove (currRecord) && recordCount < runLength) {
		records.push_back(currRecord);
		if(recordCount == runLength) {
			sort (records.begin (), records.end (), compareRecords);
			generateRuns (records);
			recordCount = 0;
			runCount++;
			records.clear ();
		}
	}
}

void BigQ :: generateRuns (vector<Record *> sortedRecords) {
	int pageCount = 0;
	Page currPage;
	vector<Record *>::iterator currRecord = sortedRecords.begin();
	for (int i = 0; i < sortedRecords.size (); i++) {
		if (!currPage.Append (* currRecord)) {
			runFile.AddPage (&currPage, pageCount++);
			currPage.EmptyItOut ();
			currPage.Append (* currRecord);
		}
		currRecord++;
	}
	runFile.AddPage (&currPage, pageCount++);
	currPage.EmptyItOut ();
}


BigQ::~BigQ () {
}
