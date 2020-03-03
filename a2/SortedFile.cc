#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include <fstream>
#include <iostream>
#include "Pipe.h"
#include "GenericDBFile.h"
#include "BigQ.h"

using namespace std;

SortedFile::SortedFile () 
{

}



SortedFile::~SortedFile ()
{

}

void SortedFile::SwitchMode(mode change){
	if(status == change)
	{
		return;
	}
	else
	{
		if(change == R)
		{
			f.AddPage(&p, pageNumber);
                        //cout << "Adding page number to disk : " << pageNumber << endl;
			FixDirtyFile();
			status = R;
			return;
		}
		else
		{
			p.EmptyItOut();
			pageNumber = f.GetLength()-2;
			f.GetPage(&p,pageNumber);
			status = W;
			return;
		}
	}	
} 


void *produce (void *arg) {
	prdutil *myprd = (prdutil *) arg;
	Record temp;
	int counter = 0;
	if (myprd->ldpath != NULL) 
	{
		//cout << "load producer" << endl;
		FILE *tableFile = fopen(myprd->ldpath, "r");
		while(temp.SuckNextRecord(myprd->rschema, tableFile))
		{
			counter += 1;
			myprd->pipe->Insert (&temp);
		}
	}
	else
	{
		//cout << "Merging producer" <<  endl;
		//cout << "Total pages: " << myprd->f->GetLength() -2 << endl;
		int curPage =0;
		Page p2;
		myprd->f->GetPage(&p2,curPage);
		while(true) {
			if(!p2.GetFirst(&temp))
			{
				if(curPage < myprd->f->GetLength() -2)
				{
					
					curPage++;
					//cout << "incrementing pages: " << curPage<< endl;
					myprd->f->GetPage(&p2,curPage);
					if(!p2.GetFirst(&temp))
					{
						break;
						//cout << "No More records to read" << endl;
					}
				}
				else
				{
					break;
					//cout << "End of pages: " << curPage<< endl;
				}
			}

		myprd->pipe->Insert (&temp);
		counter++;
		}
	}
	//cout << "Inserted records by merge prod: " << counter<< endl;
	myprd->pipe->ShutDown ();
	cout << "************ " << counter << endl;
}

void *consume (void *arg) 
{

	prdutil *t = (prdutil *) arg;
	ComparisonEngine ceng;
	Record rec;
        int cnt =0;
        Page p1;
        int curpage =0;
	while (t->pipe->Remove (&rec)) 
	{
		cnt++;
		if (!p1.Append(&rec))
		{
			t->f->AddPage(&p1,curpage);
			p1.EmptyItOut();
			p1.Append(&rec);
			curpage++;
		}
	}
	t->f->AddPage(&p1,curpage);
	p1.EmptyItOut();
}
void SortedFile::FixDirtyFile() 
{
                int buffsz = 100; // pipe cache size
                Pipe input (buffsz);
                Pipe output (buffsz);
                pthread_t thread_1;
                prdutil inp = {&input, &sort_order,NULL, &f, NULL  };
                pthread_create (&thread_1, NULL, produce, (void *)&inp);
                pthread_t thread_2;
                prdutil outp = {&output, &sort_order,NULL, &f, NULL };
                pthread_create (&thread_2, NULL, consume, (void *)&outp);
                BigQ bq (input, output, sort_order, runlen);
                pthread_join (thread_1, NULL);
                pthread_join (thread_2, NULL);


}

int SortedFile::Create (char *f_path, fType f_type, void *startup) 
{

	fstatus.open(f_path);
	f.Open(0,f_path);
       	f.AddPage(&p,0);
       	pageNumber=0;
        status = W;
       	pageCount=1;
                
return 1;

}

void SortedFile::Load (Schema &f_schema, char *loadpath) 
{
    /*    
        //cout << "In load function switch called to W actual mode: " << status << endl;  
        SwitchMode(W);
	int buffsz = 100; // pipe cache size
        Pipe input (buffsz);
        Pipe output (buffsz);
        pthread_t thread_1;
        prdutil inp = {&input, &sort_order,&f_schema, &f, loadpath  };
        pthread_create (&thread_1, NULL, produce, (void *)&inp);
        pthread_t thread_2;
        prdutil outp = {&output, &sort_order,&f_schema, &f, loadpath };
        pthread_create (&thread_2, NULL, consume, (void *)&outp);
        BigQ bq (input, output, sort_order, runlen);
        pthread_join (thread_1, NULL);
        pthread_join (thread_2, NULL);
	status=R;	
*/
}

int SortedFile::Open(char *f_path)
{
	p.EmptyItOut();
	f.Open(1,f_path);
	status=R;
	pageCount = f.GetLength();
	MoveFirst();
return 1;
}

void SortedFile::MoveFirst ()
{
	//cout << "In Move first function switch called to R actual mode: " << status << endl;
	SwitchMode(R);
	p.EmptyItOut();
	pageNumber = 0;
	f.GetPage(&p,pageNumber);

}

int SortedFile::Close ()
{
	//cout << "In close function switch called to R actual mode: " << status << endl;
        SwitchMode(R);
	p.EmptyItOut();
	f.Close();
return 1;

}

void SortedFile::Add (Record &rec)
{

        //cout << "In Add function switch called to W actual mode: " << status << endl;
	SwitchMode(W);
	if(!p.Append(&rec))
	{

		f.AddPage(&p, pageNumber);
		p.EmptyItOut();
		pageNumber++;pageCount++;
		p.Append(&rec);
	}	
}



int SortedFile::GetNext (Record &fetchme) {
	//cout << "In get next function switch called to R actual mode: " << status << endl;
	SwitchMode(R);
	if(p.GetFirst(&fetchme))
	{
		return 1;
	}
	else
	{
		if(pageNumber < pageCount - 2)
		{
			pageNumber++;
			f.GetPage(&p,pageNumber);
			if(GetNext(fetchme))
			{
				return 1;
			}	
		}
	return 0;
	}	
}


int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
	//cout << "In get next cnf function switch called to R actual mode: " << status << endl;
	SwitchMode(R);
        ComparisonEngine comp;
	while(GetNext(fetchme)){
		if (comp.Compare (&fetchme, &literal, &cnf))
		{

			return 1;
		}		
	}
	return 0;
}
