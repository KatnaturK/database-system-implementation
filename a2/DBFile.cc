#include <fstream>
#include <iostream>

#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"

using namespace std;

DBFile::DBFile (): genericDBFile(NULL) {}

DBFile::~DBFile () {
  delete genericDBFile;
}

int DBFile::Open (char *filePath) {
  
  string tmpFilePath = filePath;
  tmpFilePath = tmpFilePath + ".meta";
  fstream outputFile (tmpFilePath.c_str (), fstream::in);
  
  runStruct run;
  outputFile.read ((char *) & run, sizeof (runStruct));

  fileTypeEnum fileType;
  fileType = run.filetype;

  if (fileType == heap) {
    // cout << "Trying to open Heap DBFIle";
    genericDBFile = new HeapFile ();
    return genericDBFile->Open (filePath);

  } else if (fileType == sorted) {
    // cout << "Trying to open Sorted DBFIle";
    genericDBFile = new SortedFile ();
    genericDBFile->sort_order = run.sortOrder;
    genericDBFile->runlen = run.runlength;
    return genericDBFile->Open (filePath);

  }
}

int DBFile::Create (char *filePath, fileTypeEnum fileType, void *startup) {
   
  if (fileType == heap) {

    string tmpFilePath = filePath;
    tmpFilePath = tmpFilePath + ".meta";
    fstream outputFile(tmpFilePath.c_str(), fstream::out);

    runStruct run;
    run.filetype = heap;
    outputFile.write((char *) &run, sizeof(run));
    outputFile.close();

    genericDBFile = new HeapFile();
    return  genericDBFile->Create(filePath, fileType, startup);

  } else if (fileType == sorted) {

    //cout << "Trying to create DBFile" << endl;
    runStruct run;
    run.filetype = sorted;
    sort =(sortStruct*)startup;
    
    //cout << "Startup coded" << endl;
    //cout << "Sorting order taken" << endl;
    run.sortOrder = *(OrderMaker *)(sort->sortOrder);
    run.runlength = sort->length;   

    //cout << "trying to write" << endl;
    string tmpFilePath = filePath;
    tmpFilePath = tmpFilePath + ".meta";
    fstream outputFile(tmpFilePath.c_str(), fstream::out);
    outputFile.write((char *) &run, sizeof(runStruct));
    // cout << "Metadata written" << endl;
    outputFile.close();

    // cout << "saving into myvar" << endl;
    genericDBFile = new SortedFile(); 
    genericDBFile->sort_order = *(sort->sortOrder);
    genericDBFile->runlen = sort->length;

    // cout << "Sending to Sorted File" << endl;
    return genericDBFile->Create(filePath, fileType, startup);
  }
}

void DBFile::Load (Schema &fileSchema, char *loadPath) {
  genericDBFile->Load (fileSchema,loadPath);
}

void DBFile::MoveFirst () {
    genericDBFile->MoveFirst ();
}

 int DBFile::Close () {
   return genericDBFile->Close ();
 }

void DBFile::Add (Record &rec) {
  genericDBFile->Add (rec);
}

int DBFile::GetNext (Record &fetchMe) {
  return genericDBFile->GetNext (fetchMe);
}

int DBFile::GetNext (Record &fetchMe, CNF &cnf, Record &literal) {
  return genericDBFile->GetNext (fetchMe, cnf, literal);
}
