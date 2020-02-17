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
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

	// static void* worker(void* workerThread);

};

#endif
