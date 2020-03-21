#include "RelOp.h"
#include <iostream>

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inFile = &inFile;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    this->buffer = new Record;
    pthread_create(&thread, NULL, select_file_helper, (void*)this);
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
    // only one temp record being used (no pages)
    return;
}

void* SelectFile::select_file_helper (void* arg) {
    SelectFile *s = (SelectFile *)arg;
    s->select_file_function();
}

void* SelectFile::select_file_function () {
    ComparisonEngine ce;
    inFile->MoveFirst();

    while (inFile->GetNext(*buffer, *selOp, *literal))
        outPipe->Insert(buffer);

    outPipe->ShutDown();
    pthread_exit(NULL);
}