#ifndef BIGQ_H
#define BIGQ_H

#include <iostream>
#include <pthread.h>
#include <vector>

#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class OrderedRecord {
public:

	OrderedRecord (Record * record, OrderMaker *sortOrder) {
		this->record = record;
		this->sortOrder = sortOrder;
	}
	~OrderedRecord ();

	static bool compareRecords(OrderedRecord *orderedRecord1, OrderedRecord *orderedRecord2);

	Record *record;
	OrderMaker *sortOrder;

};

class PageRun {
public:
	int startCount, endCount;
};


class BigQ {
public:
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

private:

	int pageCount, runCount, runLength;
	File runFile;
	OrderMaker *sortOrder;
	Pipe *in, *out;
	vector<PageRun *> pageRuns;

	static void* worker(void* workerThread);

	void tpmmsPhase1 ();
	void tpmmsPhase2 ();
	void generateRuns (vector<OrderedRecord *>);

};

#endif
