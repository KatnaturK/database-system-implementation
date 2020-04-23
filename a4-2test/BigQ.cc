#include "BigQ.h"

using namespace std;

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): inPipe(in), outPipe(out), orderMarker(sortorder), runLength(runlen)  {
	tmpFile = rndStr(15) + ".bin";
	runFile.Open (0, const_cast<char *> (tmpFile.c_str()));
	pthread_create (&bigQthread, NULL, bigQRoutine, (void *)this);
}

void * BigQ::bigQRoutine (void *routine){

	BigQ *biqQ = (BigQ *)routine;
	OrderMaker order1 = biqQ->orderMarker, order2 = biqQ->orderMarker;

	auto ce1 = [&] (Record *record1, Record *record2){
		ComparisonEngine ce;
		return ce.Compare (record1, record2, &order1)<0?true:false;
	};

	auto ce2 = [&] (pair<int,Record *> recordPair1, pair<int,Record *> recordPair2){
		ComparisonEngine ce;
		return ce.Compare (recordPair1.second, recordPair2.second, &order2) < 0 ? false : true;
	};

	Record tmpRec;
	Page *outPage = new Page();
	size_t currCount = 0;
	int pageNum = 0, counter = 0;
	vector<Record *> sortRecVector;
	vector<int> pageCounter;

	while ((biqQ->inPipe).Remove (&tmpRec)){
		char * tbits = tmpRec.GetBits ();

		if (currCount+((int *)tbits)[0] < (PAGE_SIZE)*(biqQ->runLength)){

			Record *recs = new Record();
			recs->Consume(&tmpRec);
			sortRecVector.push_back(recs);
			currCount += ((int *)tbits)[0];

		} else{

			std::sort (sortRecVector.begin (), sortRecVector.end (),ce1);
			pageCounter.push_back (pageNum);

			for (auto rec: sortRecVector) {	
				if (!outPage->Append(rec)) {
        			int currPageNum = biqQ->runFile.GetLength ();
					currPageNum = (currPageNum == 0 ? 0 : (currPageNum-1)); 
        			biqQ->runFile.AddPage (outPage,currPageNum);
        			outPage->EmptyItOut ();
        			outPage->Append (rec);
					pageNum++;
    			}
			}

			if (!outPage->empty ()){
				int currPageNum = biqQ->runFile.GetLength ();
				currPageNum = (currPageNum == 0 ? 0 : (currPageNum-1)); 
				biqQ->runFile.AddPage (outPage, currPageNum);
				outPage->EmptyItOut ();
				pageNum++;
			}

			for (auto rec: sortRecVector) delete rec;
			sortRecVector.clear ();

			Record *record = new Record ();
			record->Consume (&tmpRec);
			sortRecVector.push_back(record);
			currCount = ((int *)tbits)[0];
		}
	}

	if (!sortRecVector.empty ()) {

		std::sort (sortRecVector.begin (), sortRecVector.end (), ce1);
		pageCounter.push_back (pageNum);
		for (auto rec: sortRecVector) {
			if (!outPage->Append (rec)) {
				int currPageNum = biqQ->runFile.GetLength ();
				currPageNum = (currPageNum == 0 ? 0 : (currPageNum-1)); 
				biqQ->runFile.AddPage (outPage, currPageNum);
				outPage->EmptyItOut ();
				outPage->Append (rec);
				pageNum++;
			}
		}

		if (!outPage->empty ()) {
			int currPageNum = biqQ->runFile.GetLength ();
			currPageNum = (currPageNum == 0 ? 0 : (currPageNum-1)); 
			biqQ->runFile.AddPage (outPage, currPageNum);
			outPage->EmptyItOut ();
			pageNum++;
		}

		for(auto rec: sortRecVector) delete rec;
		sortRecVector.clear ();
	}

	pageCounter.push_back (pageNum);
	vector<Page *> pageVec;
	priority_queue<pair<int, Record*>, vector<pair<int, Record*>>,decltype (ce2 ) > pq (ce2);
	
    for (int i=0;i<pageCounter.size ()-1;i++ ) {
		Page *tmpPage = new Page ();
		biqQ->runFile.GetPage (tmpPage,pageCounter[i]);
		Record *temp_record = new Record ();
 		tmpPage->GetFirst (temp_record);
		pq.push (make_pair (i,temp_record));
		pageVec.push_back (tmpPage);
	}

	vector<int> pageCheckVec (pageCounter);

	while (!pq.empty ()){

		auto top = pq.top();
		biqQ->outPipe.Insert(top.second);
		pq.pop ();
		Record *temp_R = new Record ();
		if (!pageVec[top.first]->GetFirst (temp_R)){

			if (++pageCheckVec[top.first]<pageCounter[top.first+1]) {

				pageVec[top.first]->EmptyItOut ();
 				biqQ->runFile.GetPage (pageVec[top.first], pageCheckVec[top.first]);
 				pageVec[top.first]->GetFirst (temp_R);
				pq.push(make_pair (top.first,temp_R));
 			}
		} else
			pq.push (make_pair (top.first,temp_R));
    }

	for(auto rec: pageVec) delete rec;
	biqQ->outPipe.ShutDown ();
	biqQ->runFile.Close ();
	remove ((biqQ->tmpFile).c_str ());
	remove ((biqQ->tmpFile+".meta").c_str ());
}

BigQ::~BigQ () {
}