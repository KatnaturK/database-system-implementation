#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;


class BigQ {
public:
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

private:

	int runCount, runLength;
	File runFile;
	static OrderMaker *sortOrder;
	Pipe *in, *out;

	static void* worker(void* workerThread);
	static bool compareRecords(Record *record1, Record *record2);

	void tppmsPhase1 ();
	void generateRuns (vector<Record *>);

};

#endif
