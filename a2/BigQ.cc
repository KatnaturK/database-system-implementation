#include "BigQ.h"
#include <vector>
#include <string>
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <algorithm>
#include<set>

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	inputPipe = &in;
	outputPipe = &out;
	sort_order = &sortorder;
	pthread_t sortingThread;
	runLength = runlen;
	fileName ="BigQtmp_001.bin";
	sortedFile.Open(0, fileName);
	currentPageNum=0;
	numberRuns=0;
	int thno = pthread_create(&sortingThread, NULL, &sortingWorker, (void*)this);
 

}

        int recordsW1 :: compareRecords (const void *rc1, const void *rc2) {
        recordsW1 *rcd1 = (recordsW1 *)rc1;
        recordsW1 *rcd2 = (recordsW1 *)rc2;
        ComparisonEngine ce;
        int result = ce.Compare(&(rcd1->tmpRecord), &(rcd2->tmpRecord), rcd2->sortedOrder);
        if(result < 0) {
                return 1;
       }
        else {
        return 0;
        }
        }

class recordsCompare {
public:
	int operator() (recordsW *r1, recordsW *r2 ) {
	ComparisonEngine ce;
	int result = ce.Compare( &(r1->newRecord), &(r2->newRecord), r1->sortedOrder );
	if (result < 0)
	 return 1;
	else
	 return 0;
	}
      };

void* BigQ:: sortingWorker(void *sortingThread)
{
	BigQ *bigQobj = (BigQ *) sortingThread;
       // cout << "Sorting records" << endl;
	bigQobj -> sortRecords();
        
	pthread_exit(NULL);
}

void BigQ::sortRecords()
{
	vector<recordsW1*> recVector;
        recordsW1 *cpyRec;
	Record *getRecord;
	int runCnt =0, pageCnt=0;
        Page curPage;
        getRecord = new Record;
        int crcnt =0;
	while(inputPipe->Remove(getRecord))
	{
                //cout <<"getting records from input pipe" << endl;
                
                cpyRec = new recordsW1;
                (cpyRec->tmpRecord).Copy(getRecord);
                (cpyRec->sortedOrder) = sort_order;
		if(!curPage.Append(getRecord)) 
		{
		    pageCnt++;
                    if (pageCnt == runLength)
                    {
                       //cout << "Sending run for sort" << endl;
                       //cout << "crcnt" << crcnt<< endl;
                       sort(recVector.begin(), recVector.end(), recordsW1::compareRecords);   
                       writeInFile(recVector);
                       //cout << "Writing into File" << endl;
                      
                       recVector.clear();
                       pageCnt = 0;
                    }
	
                    else
                    {
                       curPage.EmptyItOut(); 
                       curPage.Append(getRecord);
                    }
                  }
                 recVector.push_back(cpyRec);
                 crcnt++;
               // cout << "Pushing into vector" << endl;
	}


        if(recVector.size() != 0) 
        {
           sort(recVector.begin(), recVector.end(), recordsW1::compareRecords);
           writeInFile(recVector);
         //  cout << "Writing into File for last time" << endl;
           recVector.clear();
        }
        inputPipe->ShutDown();
        cout << "Shutting down input pipe" <<endl;
        mergeRecords(); 
}

void BigQ :: writeInFile(vector<recordsW1*> rcVector) 
{

       // cout << "Writing in File" << endl;	
	numberRuns++;
        Page myPage;
        runMetaData *rmd = new runMetaData;
        rmd->startPage = currentPageNum;
        //int apcnt=0;
        vector<recordsW1*>::iterator startIt = rcVector.begin();
        vector<recordsW1*>::iterator endIt = rcVector.end();
        while(startIt != endIt) {
        
               // cout << "IN Vector append" << endl;
                if(!myPage.Append(&((*startIt)->tmpRecord))) 
                { 
                   sortedFile.AddPage(&myPage, currentPageNum);
                   //cout << "Sorted file add" << endl; 
                   currentPageNum++;
                   myPage.EmptyItOut();
                   myPage.Append( &((*startIt)->tmpRecord));
                   //apcnt++;
                } 

               startIt++;

           }  
       

	sortedFile.AddPage(&myPage, currentPageNum);
        myPage.EmptyItOut();
        rmd->endPage = currentPageNum; 
        runmetaDataVec.push_back(rmd); 
        currentPageNum++;
        //cout << "CUrret page: " << currentPageNum << endl;
        //cout << "Append cnt: " << apcnt << endl;




}
void BigQ::mergeRecords()
{	

	//cout <<"In Merge Records" << endl;
	int compRuns = 0; 
        int cntr=0;
	vector<pageWrapper*> PageVector; 
	pageWrapper *fPage = NULL; 
	int curPNum = 0; 
	for(int i=1; i<=numberRuns; i++) 
	{	
	   curPNum = (runmetaDataVec[i-1])->startPage; 
	   fPage = new pageWrapper;
	   sortedFile.GetPage( &(fPage->newPage), curPNum); 
	   fPage->currentPage = curPNum;
	   PageVector.push_back(fPage); 
	}
	multiset<recordsW*, recordsCompare> mergeset; 
	recordsW *tempRec = NULL; 
	for(int j=0; j<numberRuns; j++)
	{	
	   tempRec = new recordsW; 
	   if(((PageVector[j])->newPage).GetFirst( &(tempRec->newRecord)) != 0) 
	   {	   
	       tempRec->runPosition = (j+1);
	       (tempRec->sortedOrder) = (this->sort_order); 
	       mergeset.insert(tempRec);
	   }
           else 
           {
               cerr<<"DB-000: No first record found "<<endl;
               exit(0);
           } 
         }
	int posrun;
	recordsW *tempWrp;
	while( compRuns < numberRuns )
        {
	tempWrp = *(mergeset.begin()) ; 
	mergeset.erase(mergeset.begin()); 
	posrun = tempWrp->runPosition; 
	outputPipe->Insert( &(tempWrp->newRecord)); 
        cntr++;
	if((PageVector[posrun-1]->newPage).GetFirst( &(tempWrp->newRecord) ) == 0) 
	 {
	  PageVector[posrun-1]->currentPage++; 
	  if(PageVector[posrun-1]->currentPage <= runmetaDataVec[posrun-1]->endPage ) 
 	   {
	    sortedFile.GetPage(&(PageVector[posrun-1]->newPage), PageVector[posrun-1]->currentPage);
	     if( (PageVector[posrun-1]->newPage).GetFirst( &(tempWrp->newRecord) ) == 0 ) 
	     {
	      cerr<<"DB-000 Empty page !"<<endl;
	      exit(0);
	     }
	tempWrp->runPosition = posrun; 
	mergeset.insert(tempWrp);
	}	
	   else 
	   {
	  compRuns++; 
	   }
	  }
	  else
	{
	 tempWrp->runPosition = posrun; 
	 mergeset.insert(tempWrp);
	}
	}
       // cout << "Merged total records sent to output pipe: " << cntr << endl;
	outputPipe->ShutDown(); 

}

BigQ::~BigQ () {
}
