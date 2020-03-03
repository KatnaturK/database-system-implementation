#include <fstream>
#include <iostream>

#include "SortedFile.h"
#include "BigQ.h"

using namespace std;

SortedFile::SortedFile () {}

SortedFile::~SortedFile () {}

void SortedFile::SwitchMode (mode newMode){
	if (fileMode == newMode)
		return;
	else {
		if (newMode == read) {
			f.AddPage (&p, pageNumber);
            //cout << "Adding page number to disk : " << pageNumber << endl;
			FixDirtyFile ();
			fileMode = read;
			return;

		} else {
			p.EmptyItOut ();
			pageNumber = f.GetLength () - 2;
			f.GetPage (&p, pageNumber);
			fileMode = write;
			return;

		}
	}	
} 

void *producer (void *arg) {
	producer_util *util = (producer_util *) arg;
	int count = 0;
	Record tmpRec;
	
	if (util->loadPath != NULL) {
		//cout << "load producer" << endl;
		FILE *tmpFile = fopen (util->loadPath, "r");

		while (tmpRec.SuckNextRecord (util->fileSchema, tmpFile)) {
			count += 1;
			util->pipe->Insert (&tmpRec);
		}

	} else {
		//cout << "Merging producer" <<  endl;
		//cout << "Total pages: " << util->f->GetLength() -2 << endl;
		int currPageCount = 0;
		Page p2;
		util->file->GetPage (&p2, currPageCount);

		while (true) {

			if (!p2.GetFirst (&tmpRec)) {
				if(currPageCount < util->file->GetLength () -2) {
					currPageCount++;
					//cout << "incrementing pages: " << currPageCount<< endl;
					util->file->GetPage (&p2,currPageCount);
					if (!p2.GetFirst (&tmpRec))
						break; // No More records to read
				
				} else
					break; //cout << "End of pages: " << currPageCount<< endl;
			}

			util->pipe->Insert (&tmpRec);
			count++;

		}
	}
	//cout << "Inserted records by merge prod: " << count<< endl;
	util->pipe->ShutDown ();
}

void *consumer (void *arg) {

	producer_util *util = (producer_util *) arg;
	ComparisonEngine ce;
	int count =0, currPageCount =0;
	Page page;
	Record rec;

	while (util->pipe->Remove (&rec)) {
		count++;
		if (!page.Append(&rec)) {
			util->file->AddPage (&page, currPageCount);
			page.EmptyItOut ();
			page.Append (&rec);
			currPageCount++;

		}
	}
	util->file->AddPage (&page, currPageCount);
	page.EmptyItOut ();

}

void SortedFile::FixDirtyFile () {
	int buffer = 100; // pipe cache size
	Pipe input (buffer);
	Pipe output (buffer);

	pthread_t thread_1;
	producer_util inp = {&input, &sort_order,NULL, &f, NULL  };
	pthread_create (&thread_1, NULL, producer, (void *)&inp);

	pthread_t thread_2;
	producer_util util = {&output, &sort_order,NULL, &f, NULL };
	pthread_create (&thread_2, NULL, consumer, (void *)&util);

	BigQ bq (input, output, sort_order, runlen);

	pthread_join (thread_1, NULL);
	pthread_join (thread_2, NULL);

}

int SortedFile::Create (char *filePath, fileTypeEnum fileType, void *startup) {

	fstatus.open (filePath);
	f.Open (0,filePath);
	f.AddPage (&p,0);
	pageNumber = 0;
	fileMode = write;
	pageCount = 1;
                
	return 1;
}

void SortedFile::Load (Schema &fileSchema, char *loadPath) {}

int SortedFile::Open (char *filePath) {
	p.EmptyItOut ();
	f.Open (1, filePath);
	fileMode = read;
	pageCount = f.GetLength ();
	MoveFirst ();

	return 1;
}

void SortedFile::MoveFirst () {
	//cout << "In Move first function switch called to R actual mode: " << status << endl;
	SwitchMode (read);
	p.EmptyItOut ();
	pageNumber = 0;
	f.GetPage (&p, pageNumber);

}

int SortedFile::Close () {
	//cout << "In close function switch called to R actual mode: " << status << endl;
    SwitchMode (read);
	p.EmptyItOut ();
	f.Close ();

	return 1;
}

void SortedFile::Add (Record &rec) {
	//cout << "In Add function switch called to W actual mode: " << status << endl;
	SwitchMode (write);
	if (!p.Append (&rec)) {
		f.AddPage (&p, pageNumber);
		p.EmptyItOut ();
		pageCount++;
		pageNumber++;
		p.Append (&rec);

	}	
}


int SortedFile::GetNext (Record &fetchMe) {
	//cout << "In get next function switch called to R actual mode: " << status << endl;
	SwitchMode (read);
	if (p.GetFirst (&fetchMe))
		return 1;
	else {
		if (pageNumber < pageCount - 2) {
			pageNumber++;
			f.GetPage(&p,pageNumber);
			if(GetNext(fetchMe))
				return 1;
		}
		return 0;

	}	
}

int SortedFile::GetNext (Record &fetchMe, CNF &cnf, Record &literal) {
	//cout << "In get next cnf function switch called to R actual mode: " << status << endl;
	SwitchMode (read);
    ComparisonEngine comp;
	while (GetNext (fetchMe)) {
		if (comp.Compare (&fetchMe, &literal, &cnf))
			return 1;
	}
	
	return 0;
}
