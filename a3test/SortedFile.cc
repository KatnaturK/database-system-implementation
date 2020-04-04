#include <fstream>
#include <iostream>

#include "SortedFile.h"
#include "BigQ.h"

using namespace std;

SortedFile::SortedFile () {}

SortedFile::~SortedFile () {}

void SortedFile::ToggleMode (mode newMode){
	if (fileMode == newMode)
		return;
	else {
		if (newMode == read) {
			runFile.AddPage (&p, pageNumber);
            //cout << "Adding page number to disk : " << pageNumber << endl;
			TwoPassMergeing ();
			fileMode = read;
			return;

		} else {
			p.EmptyItOut ();
			pageNumber = runFile.GetLength () - 2;
			runFile.GetPage (&p, pageNumber);
			fileMode = write;
			return;

		}
	}	
} 

void *producerWorker (void *arg) {
	workerStruct *worker = (workerStruct *) arg;
	int count = 0;
	Record tmpRec;
	
	if (worker->loadPath != NULL) {
		//cout << "load producer" << endl;
		FILE *tmpFile = fopen (worker->loadPath, "r");

		while (tmpRec.SuckNextRecord (worker->fileSchema, tmpFile)) {
			count += 1;
			worker->pipe->Insert (&tmpRec);
		}

	} else {
		//cout << "Merging producer" <<  endl;
		//cout << "Total pages: " << worker->f->GetLength() -2 << endl;
		int currPageCount = 0;
		Page p2;
		worker->file->GetPage (&p2, currPageCount);

		while (true) {

			if (!p2.GetFirst (&tmpRec)) {
				if(currPageCount < worker->file->GetLength () -2) {
					currPageCount++;
					//cout << "incrementing pages: " << currPageCount<< endl;
					worker->file->GetPage (&p2,currPageCount);
					if (!p2.GetFirst (&tmpRec))
						break; // No More records to read
				
				} else
					break; //cout << "End of pages: " << currPageCount<< endl;
			}

			worker->pipe->Insert (&tmpRec);
			count++;

		}
	}
	//cout << "Inserted records by merge prod: " << count<< endl;
	worker->pipe->ShutDown ();
}

void *consumerWorker (void *arg) {

	workerStruct *worker = (workerStruct *) arg;
	ComparisonEngine ce;
	int count =0, currPageCount =0;
	Page page;
	Record rec;

	while (worker->pipe->Remove (&rec)) {
		count++;
		if (!page.Append(&rec)) {
			worker->file->AddPage (&page, currPageCount);
			page.EmptyItOut ();
			page.Append (&rec);
			currPageCount++;

		}
	}
	worker->file->AddPage (&page, currPageCount);
	page.EmptyItOut ();

}

void SortedFile::TwoPassMergeing () {
	int buffer = 100; // pipe cache size
	Pipe input (buffer);
	Pipe output (buffer);

	pthread_t thread_1;
	workerStruct inPipe = {&input, &sortOrder,NULL, &runFile, NULL  };
	pthread_create (&thread_1, NULL, producerWorker, (void *)&inPipe);

	pthread_t thread_2;
	workerStruct outPipe = {&output, &sortOrder,NULL, &runFile, NULL };
	pthread_create (&thread_2, NULL, consumerWorker, (void *)&outPipe);

	BigQ bq (input, output, sortOrder, runlen);

	pthread_join (thread_1, NULL);
	pthread_join (thread_2, NULL);

}

int SortedFile::Create (char *filePath, fileTypeEnum fileType, void *startup) {

	fstatus.open (filePath);
	runFile.Open (0,filePath);
	runFile.AddPage (&p,0);
	pageNumber = 0;
	fileMode = write;
	pageCount = 1;
                
	return 1;
}

void SortedFile::Load (Schema &fileSchema, char *loadPath) {}

int SortedFile::Open (char *filePath) {
	p.EmptyItOut ();
	runFile.Open (1, filePath);
	fileMode = read;
	pageCount = runFile.GetLength ();
	MoveFirst ();

	return 1;
}

void SortedFile::MoveFirst () {
	//cout << "In Move first function switch called to R actual mode: " << status << endl;
	ToggleMode (read);
	p.EmptyItOut ();
	pageNumber = 0;
	runFile.GetPage (&p, pageNumber);

}

int SortedFile::Close () {
	//cout << "In close function switch called to R actual mode: " << status << endl;
    ToggleMode (read);
	p.EmptyItOut ();
	runFile.Close ();

	return 1;
}

void SortedFile::Add (Record &rec) {
	//cout << "In Add function switch called to W actual mode: " << status << endl;
	ToggleMode (write);
	if (!p.Append (&rec)) {
		runFile.AddPage (&p, pageNumber);
		p.EmptyItOut ();
		pageCount++;
		pageNumber++;
		p.Append (&rec);

	}	
}


int SortedFile::GetNext (Record &fetchMe) {
	//cout << "In get next function switch called to R actual mode: " << status << endl;
	ToggleMode (read);
	if (p.GetFirst (&fetchMe))
		return 1;
	else {
		if (pageNumber < pageCount - 2) {
			pageNumber++;
			runFile.GetPage(&p,pageNumber);
			if(GetNext(fetchMe))
				return 1;
		}
		return 0;

	}	
}

int SortedFile::GetNext (Record &fetchMe, CNF &cnf, Record &literal) {
	//cout << "In get next cnf function switch called to R actual mode: " << status << endl;
	ToggleMode (read);
    ComparisonEngine ce;
	while (GetNext (fetchMe)) {
		if (ce.Compare (&fetchMe, &literal, &cnf))
			return 1;
	}
	
	return 0;
}
