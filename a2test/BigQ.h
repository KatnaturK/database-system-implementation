#ifndef BIGQ_H
#define BIGQ_H
#include <algorithm>
#include <iostream>
#include <map>
#include <math.h>
#include <stdlib.h>
#include <queue>
#include <vector>

#include "ComparisonEngine.h"
#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

class ComparisonEngine;

class Phase1Compare{

OrderMaker *sortOrder;

public:

	Phase1Compare(OrderMaker *inSortOrder) {
		sortOrder = inSortOrder;
	}

	// compare operator to sort records into runs for phase 1 of tpmms algorithm.
	bool operator()(Record* record1, Record* record2) {
		ComparisonEngine ce;
		if (ce.Compare (record1, record2, sortOrder) < 0) return true;
		else return false;
	}
};

class Phase2Compare{

	OrderMaker *sortOrder;

public:

	Phase2Compare(OrderMaker *inSortOrder) {
		sortOrder = inSortOrder;
	}

	// compare operator to sort records and merge using priority queue for phase 2 of tpmms algorithm.
	bool operator()(Record* record1, Record* record2) {
		ComparisonEngine ce;
		if (ce.Compare (record1, record2, sortOrder) < 0) return false;
		else return true;
	}
};

class BigQ  {

public:

	int runLength;
	File runFile;
	OrderMaker *sortOrder;
	Pipe *in, *out;
	
	BigQ ();
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

	static void* worker(void* workerThread);

	// Phase 1 of Two-Pass Multiway Merge Sort Algorithm.
	void tpmmsPhase1 (Pipe *in, OrderMaker *sortOrder, int &runCount, int runLength, File &runFile,	map<int,Page*> &overflow);
	// Phase 2 of Two-Pass Multiway Merge Sort Algorithm.
	void tpmmsPhase2 (Pipe *out, OrderMaker *sortOrder, int runCount, int runLength, File &runFile, map<int,Page*> overflow);

	void initFile (File &runfile);
	int generateRuns (vector<Record *> &records, int &runCount, int runLength, File &runFile, map<int,Page*> &overflow);

};

#endif
