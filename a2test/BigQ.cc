#include "BigQ.h"

using namespace std;

void initFile (File &runfile) {
	char tempFile[100];
	sprintf(tempFile,"tmp%d.bin",rand());
	runfile.Open(0,tempFile);
}

void tpmmsPhase1 (Pipe *in, OrderMaker *sortOrder, int &runCount, int runLength, File &runFile,	map<int,Page*> &overflow) {
	cout << "Running Phase 1 of Two-Pass Multiway Merge Sort Algorithm." << endl;
	
	int count = 0;
	Page *currPage = new (nothrow) Page ();
	Record curRecord;
	vector<Page*> pages;
	vector<Record*> records;
	initFile (runFile);

	while (true) {
		if (in->Remove (&curRecord) && count < runLength) {
			if (!currPage->Append (&curRecord)) {
				pages.push_back (currPage);
				currPage = new (nothrow) Page ();
				currPage->Append (&curRecord);
				count++;
			}
		} else {
			if (count < runLength) pages.push_back (currPage);

			Record *tmpRecord;
			for (int i = 0; i < pages.size (); i++) {
				tmpRecord = new (nothrow) Record();
				while (pages[i]->GetFirst (tmpRecord)) {
					records.push_back (tmpRecord);
					tmpRecord = new (nothrow) Record();
				}
				delete pages[i];
				delete tmpRecord;
				pages[i] = NULL;
				tmpRecord = NULL;
			}

			cout << "Sorting runs pages." << endl;
			sort (records.begin (), records.end (), Phase1Compare (sortOrder));

			cout << "Writing sorted runs to a file. " << count << endl;
			int pageCount2 = 0;
			Page *filePage = new (nothrow) Page ();
			for (int i = 0; i < records.size (); i++) {
				if (!filePage->Append (records[i])) {
					pageCount2++;
					runFile.AddPage (filePage, runCount++);
					filePage->EmptyItOut ();
					filePage->Append (records[i]);
				}
				delete records[i];
				records[i] = NULL;
			}

			if (pageCount2 < runLength) runFile.AddPage (filePage, runCount++);
			else overflow[runCount - 1] = filePage;
			filePage = NULL;

			pages.clear();
			records.clear();

			if (count >= runLength) {
				currPage->Append (&curRecord);
				count = 0;
				continue;
			} else break;
		}
	}
}

void tpmmsPhase2 (Pipe *out, OrderMaker *sortOrder, int runCount, int runLength, File &runFile, map<int,Page*> overflow) {
	cout << "Running Phase 2 of Two-Pass Multiway Merge Sort Algorithm." << endl;
	int runs = 0;
	if (runCount != 0) runs = ceil((float)runCount / runLength);
	int lastRun = runCount - ((runs - 1) * runLength);

	priority_queue<Record*, vector<Record*>, Phase2Compare> pQ (sortOrder);
	map<Record*, int> recordMap;
	int *indexArr = new (nothrow) int[runs];
	Page** pageArr = new (nothrow) Page*[runs];
	int pageCount = 0;


	cout << "Reading first record from each runs and pushing it in the priority queue." << endl;
	for (int i = 0; i < runs; i++) {
		indexArr[i] = 1;
		pageArr[i] = new (nothrow) Page ();
		runFile.GetPage (pageArr[i], pageCount);
		Record* tmpRecord = new (nothrow) Record ();
		pageArr[i]->GetFirst (tmpRecord);
		pQ.push (tmpRecord);
		recordMap[tmpRecord] = i;
		tmpRecord = NULL;
		pageCount = pageCount + runLength;
	}

	while (!pQ.empty ()) {
		Record* record = pQ.top ();
		pQ.pop ();
		int recordIndex = -1;
		recordIndex = recordMap[record];
		// recordMap.erase (record);
		if (recordIndex == -1) {
			cout << "tpmmsPhase2: Invalid run index.";
			break;
		}
		
		Record* currRecord = new (nothrow) Record;
		bool recordFound = true;
		if (!pageArr[recordIndex]->GetFirst (currRecord)) {
 			if ((recordIndex < runs - 1 && indexArr[recordIndex] < runLength)
					|| 
				(recordIndex == runs - 1 && indexArr[recordIndex] < lastRun)) {
				
				runFile.GetPage (pageArr[recordIndex], indexArr[recordIndex] + (recordIndex * runLength));
				pageArr[recordIndex]->GetFirst (currRecord);
				indexArr[recordIndex]++;
			} else {  
				if (indexArr[recordIndex] == runLength) {
					if (overflow[((recordIndex + 1) * runLength) - 1]) {
						delete pageArr[recordIndex];
						pageArr[recordIndex] = NULL;
						pageArr[recordIndex] = overflow[((recordIndex + 1 ) * runLength) - 1];
						overflow[((recordIndex + 1) * runLength) - 1] = NULL;
						pageArr[recordIndex]->GetFirst (currRecord);
					} else recordFound = false;
				} else recordFound = false;
			}
		}
		
		if (recordFound) pQ.push (currRecord);
		recordMap[currRecord] = recordIndex;
		out->Insert (record);
	}
}

void tpmms (Pipe *in, Pipe *out, OrderMaker *sortOrder, int runLength, File runFile) {
	int runCount = 0;
	map<int,Page*> overflow;
	tpmmsPhase1 (in, sortOrder, runCount, runLength, runFile, overflow);
	tpmmsPhase2 (out, sortOrder, runCount, runLength, runFile, overflow);
	runFile.Close ();
	out->ShutDown ();
}

void* worker (void *workerThread) {
	BigQ *bigQWorkerThread = (BigQ*) workerThread;
	tpmms (
		bigQWorkerThread->in,
		bigQWorkerThread->out, 
		bigQWorkerThread->sortOrder,
		bigQWorkerThread->runLength,
		bigQWorkerThread->runFile
	); 
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runLength) {
	if(runLength < 1) {
		out.ShutDown ();
		throw runtime_error("BigQ: runLength must be > 0");
		return;
	}
	this->in = &in;
	this->out = &out;
	this->runLength = runLength;
	this->sortOrder = &sortorder;

	pthread_t workerThread;
	int ret = pthread_create(&workerThread, NULL, worker, (void*) this);
}

BigQ::~BigQ() {}