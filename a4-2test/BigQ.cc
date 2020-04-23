#include <climits>
#include <unistd.h>
#include <cstring>
#include "BigQ.h"

using namespace std;

char tempFilePath[PATH_MAX];

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &orderMaker, int runlen1) {
    inPipe = &in;
    outPipe = &out;
    runlen = runlen1;
    maker = &orderMaker;
    pthread_create(&worker_thread, NULL, workerThread, (void *) this);
}
BigQ::BigQ(){

}

BigQ::~BigQ() {
}

void BigQ::phaseOne() {
    Record tempRecord;
    Compare comparator = Compare(*maker);

    const long maxSize = PAGE_SIZE * runlen;

    long curSize = 0;

    vector<Record *> recordList;
    Page page;
    int page_index=0;

    while (inPipe->Remove(&tempRecord)) {
        Record *record = new Record();
        record->Copy(&tempRecord);


//        curSize += record->GetLength();

        if(!page.Append(&tempRecord)){
            if (++page_index == runlen){
                sort(recordList.begin(), recordList.end(), comparator);
                dumpSortedList(recordList);
                page_index=0;
            }
            page.EmptyItOut();
            page.Append(&tempRecord);
        }

//        if (curSize >= maxSize) {
//            curSize = 0;
//            sort(recordList.begin(), recordList.end(), comparator);
//            dumpSortedList(recordList);
//        }
        recordList.emplace_back(record);
    }
//    cout<<"Current Record Size: "<<curSize<<endl;
    if (recordList.empty()) {
        outPipe->ShutDown();
        file.Close();
        pthread_exit(NULL);
    }

    sort(recordList.begin(), recordList.end(), comparator);
//    cout<<"RecordList: "<<recordList.size()<<endl;
    dumpSortedList(recordList);
//    cout<<"Sucess of Dumping Sorted List"<<endl;
}

void BigQ::phaseTwo() {
//    cout <<"phase2: " << blockNum <<endl;
    vector<Page> tempPage(blockNum);
    priority_queue<IndexedRecord *, vector<IndexedRecord *>, IndexedRecordCompare> priorityQueue(*maker);

    for (int i = 0; i < blockNum; i++) {
        file.GetPage(&tempPage[i], blockStartOffset[i]++);
        IndexedRecord *indexedRecord = new IndexedRecord();
        indexedRecord->blockIndex = i;
        tempPage[i].GetFirst(&(indexedRecord->record));
        priorityQueue.emplace(indexedRecord);
    }

    while (!priorityQueue.empty()) {
        IndexedRecord *indexedRecord = priorityQueue.top();
        priorityQueue.pop();
        int blockIndex = indexedRecord->blockIndex;
        outPipe->Insert(&(indexedRecord->record));
        if (tempPage[blockIndex].GetFirst(&(indexedRecord->record))) {
            priorityQueue.emplace(indexedRecord);
        } else if (blockStartOffset[blockIndex] < blockEndOffset[blockIndex]) {
            file.GetPage(&tempPage[blockIndex], blockStartOffset[blockIndex]++);
            tempPage[blockIndex].GetFirst(&(indexedRecord->record));
            priorityQueue.emplace(indexedRecord);
        } else {
            delete indexedRecord;
        }
    }
}


void BigQ::dumpSortedList(vector<Record *> &recordList) {
    Page outPage;
    blockStartOffset.push_back(file.GetLength() - 1);
    for (auto rec : recordList) {
        if (!outPage.Append(rec)) {
            file.AddPage(&outPage, file.GetLength() - 1);
            outPage.EmptyItOut();
            outPage.Append(rec);
            delete rec;
            rec = NULL;
        }
    }

    file.AddPage(&outPage, file.GetLength() - 1);
    blockEndOffset.push_back(file.GetLength() - 1);
    ++blockNum;
    recordList.clear();
}

void *BigQ::workerThread(void *arg) {
    BigQ *bigQ = (BigQ *) arg;

    bigQ->init();

    bigQ->phaseOne();
//    clog << "bigq: phase-1 completed.." << endl;

    bigQ->phaseTwo();
//    clog << "bigq: phase-2 completed.." << endl;

    bigQ->close();
    remove(tempFilePath);
    pthread_exit(NULL);
}

void BigQ::init() {
    if (getcwd(tempFilePath, sizeof(tempFilePath)) != NULL) {
        strcat(tempFilePath, "/temp/tmp");
        strcat(tempFilePath, getRandStr(10).c_str());
    } else {
        cerr << "error while getting curent dir" << endl;
        exit(-1);
    }
    file.Open(0, tempFilePath);
    file.AddPage(new Page(), -1);
}

void BigQ::close() {
    outPipe->ShutDown();
    file.Close();
}

string BigQ::getRandStr(int len)
{
    static string charset = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    string result;
    result.resize(len);

    for (int i = 0; i < len; i++) {
        result[i] = charset[rand() % charset.length()];
    }

    return result;
}