#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	this->currentPageNum = 0;
	this->in = &in;
	this->out = &out;
	this->runCount = 0;
	this->runLength = runlen;
	this->sortOrder = &sortorder;
	this->tmpRunFile = "tmpRunFile.bin";

	pthread_t workerThread;
	runFile.Open (0, tmpRunFile);
	int threadId = pthread_create (&workerThread, NULL, &worker, (void*)this);

}

BigQ::~BigQ () {}

class RecordsCompare {

public:
	int operator() (CustomRecord *record1, CustomRecord *record2 ) {
		ComparisonEngine ce;
		int result = ce.Compare( &(record1->newRecord), &(record2->newRecord), record1->sortedOrder);
		if (result < 0)
				return 1;
		else
			return 0;
	}
};

int RecordOperator :: compareRecords (const void *record1, const void *record2) {
	RecordOperator *recordOp1 = (RecordOperator *)record1;
	RecordOperator *recordOp2 = (RecordOperator *)record2;
	ComparisonEngine ce;
	int result = ce.Compare (&(recordOp1->tmpRecord), &(recordOp2->tmpRecord), recordOp2->sortedOrder);
	if (result < 0)
		return 1;
	else
		return 0;
}

void* BigQ:: worker (void *worker) {
	BigQ *bigQWorker = (BigQ *) worker;
	bigQWorker->tmpmmsPhase1();
	bigQWorker->tmpmmsPhase2 (); 
	pthread_exit(NULL);
}

void BigQ::tmpmmsPhase1 () {

    int counter = 0;
	int pageCounter = 0;
	int runCounter = 0; 
    Page currPage;
	Record *currRecord;
    RecordOperator *tmpRecordOperator;
	vector<RecordOperator*> recordOperators;

    currRecord = new Record;
	while (in->Remove (currRecord)) {
		//cout <<"getting records from input pipe" << endl;
		
		tmpRecordOperator = new RecordOperator;
		(tmpRecordOperator->tmpRecord).Copy (currRecord);
		(tmpRecordOperator->sortedOrder) = sortOrder;

		if (!currPage.Append (currRecord)) {
		    pageCounter++;

			if (pageCounter == runLength) {
				//cout << "Sending run for sort" << endl;
				sort (recordOperators.begin (), recordOperators.end (), RecordOperator::compareRecords);   
				generateRuns (recordOperators);
				//cout << "Writing into File" << endl;
				recordOperators.clear();
				pageCounter = 0;
			} else {
				currPage.EmptyItOut (); 
				currPage.Append (currRecord);
			}
		}
		recordOperators.push_back (tmpRecordOperator);
		counter++;
		// cout << "Pushing into vector" << endl;
	}

	if (recordOperators.size () != 0) {
		// sorting records in recorOperator vector
		sort (recordOperators.begin (), recordOperators.end (), RecordOperator::compareRecords);
		//  cout << "Writing into File for last time" << endl;
		generateRuns (recordOperators); 
		recordOperators.clear ();
	}
	in->ShutDown ();
	// cout << "Shutting down input pipe" <<endl;
}

void BigQ :: generateRuns (vector<RecordOperator*> recordOperators) {
    // cout << "Writing in File" << endl;	
	runCount++;
	Page currPage;
	RunPage *currRunPage = new RunPage;
	vector<RecordOperator*>::iterator startIterator = recordOperators.begin();
	vector<RecordOperator*>::iterator endIterator = recordOperators.end();

	currRunPage->startPageNum = currentPageNum;
	while (startIterator != endIterator) {

		if(!currPage.Append (&((*startIterator)->tmpRecord))) { 
			
			runFile.AddPage (&currPage, currentPageNum);
			//cout << "Sorted file add" << endl; 
			currentPageNum++;
			currPage.EmptyItOut ();
			currPage.Append ( &((*startIterator)->tmpRecord));
		} 
		startIterator++;
	}  

	runFile.AddPage (&currPage, currentPageNum);
	currPage.EmptyItOut ();
	currRunPage->endPageNum = currentPageNum; 
	runPages.push_back (currRunPage); 
	currentPageNum++;
}

void BigQ::tmpmmsPhase2 () {	
	// Merging sorted runs - Phase 2 of Two-Pass Multiway Merge Algorithm.
    int counter=0;
	int currPageNum = 0; 
	int mergeRunCounter = 0; 
	int runNum;
	CustomPage *currCustomPage = NULL; 
	multiset<CustomRecord*, RecordsCompare> mergedPageSet; 
	vector<CustomPage*> customPages; 

	for (int i = 1; i <= runCount; i++) {	

	   currPageNum = (runPages[i-1])->startPageNum; 
	   currCustomPage = new CustomPage;
	   runFile.GetPage( &(currCustomPage->page), currPageNum); 
	   currCustomPage->currentPageNum = currPageNum;
	   customPages.push_back(currCustomPage); 
	}

	CustomRecord *tempRec = NULL; 
	for(int i = 0; i < runCount; i++) {	

	   tempRec = new CustomRecord; 
	   if (((customPages[i])->page).GetFirst ( &(tempRec->newRecord)) != 0) {	   
	       tempRec->runPosition = (i + 1);
	       (tempRec->sortedOrder) = (this->sortOrder); 
	       mergedPageSet.insert (tempRec);
	   
	   } else {
            cerr<<"DB-000: No first record found "<<endl;
            exit(0);
        } 
    }
	
	CustomRecord *tempWrp;
	while (mergeRunCounter < runCount ) {

		tempWrp = *(mergedPageSet.begin ()) ; 
		mergedPageSet.erase(mergedPageSet.begin ()); 
		runNum = tempWrp->runPosition; 
		out->Insert( &(tempWrp->newRecord)); 
        counter++;

		if ((customPages[runNum-1]->page).GetFirst ( &(tempWrp->newRecord) ) == 0) {
	  		customPages[runNum-1]->currentPageNum++; 
	  		if (customPages[runNum-1]->currentPageNum <= runPages[runNum-1]->endPageNum) {
	    		runFile.GetPage (&(customPages[runNum-1]->page), customPages[runNum-1]->currentPageNum);
	     		if ((customPages[runNum-1]->page).GetFirst (&(tempWrp->newRecord) ) == 0 ) {
	      			cerr<<"DB-000 Empty page !"<<endl;
	      			exit(0);
	     		}
				tempWrp->runPosition = runNum; 
				mergedPageSet.insert(tempWrp);
			
			} else
	  			mergeRunCounter++;

	  	} else {
			tempWrp->runPosition = runNum; 
			mergedPageSet.insert (tempWrp);
		}
	}
	out->ShutDown(); 
}