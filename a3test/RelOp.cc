#include "RelOp.h"
#include <iostream>

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    cout << "inside run\n";
    select_file_struct s = {&inFile, &outPipe, &selOp, &literal};
    int ret = pthread_create(&thread, NULL, select_file_function, (void*) &s);
    cout << "thread created\n";
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}

void* SelectFile::select_file_function (void* s) {
    cout << "inside select_file_function\n";
    select_file_struct *s1 = (select_file_struct *)s;
    // CNF selOp = s1->selOp;
    Record literal = *(s1->literal);
    Record currRecord;
    while (s1->inFile->GetNext(currRecord, *(s1->selOp), literal)) {
        cout << "got a record?\n";
        s1->outPipe->Insert(&currRecord);
    }
    cout << "got noo records?\n";
    s1->outPipe->ShutDown();
}