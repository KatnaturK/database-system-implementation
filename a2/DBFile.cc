#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Pipe.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <iostream>
#include "HeapFile.h"
#include "SortedFile.h"

using namespace std;

DBFile::DBFile (): myInternalVar(NULL) {

 

}

DBFile::~DBFile () {

delete myInternalVar;

}

 int DBFile::Open (char *fpath) {
  fType myftype;
  string metadataFilePath=fpath;
  metadataFilePath=metadataFilePath+".metadata";
  fstream outputFile(metadataFilePath.c_str(), fstream::in);
  metadata tst;
  outputFile.read((char *) & tst, sizeof (metadata));
  myftype = tst.filetype;
  
  if (myftype == heap){
   cout << "Trying to open Heap DBFIle";
   myInternalVar = new HeapFile();
   return myInternalVar->Open(fpath);

  }

  else if (myftype == sorted){
   cout << "Trying to open Sorted DBFIle";
   myInternalVar = new SortedFile();
   myInternalVar->sort_order = tst.sorting_order;
   myInternalVar->runlen = tst.runlength;
   return myInternalVar->Open(fpath);

  }


}
 int DBFile::Create (char *f_path, fType f_type, void *startup) {
   
  if (f_type == heap){
  string metadataFilePath=f_path;
  metadataFilePath=metadataFilePath+".metadata";
  fstream outputFile(metadataFilePath.c_str(), fstream::out);
  metadata myMeta;
  myMeta.filetype = heap;
  outputFile.write((char *) &myMeta, sizeof(myMeta));
  outputFile.close();
  myInternalVar = new HeapFile();
  return  myInternalVar->Create(f_path, f_type, startup);
  }

else if (f_type == sorted){
  metadata myMeta;
  //cout << "Trying to create DBFile" << endl;
  myMeta.filetype = sorted;
  //cout << "Statement run" << endl;
  mysrt =(sortinfo*)startup;
  
  //cout << "Startup coded" << endl;
  //cout << "printing l: " << &(mysrt->l) << endl;
  //mysrt->ord->Print();
  myMeta.sorting_order = *(OrderMaker *)(mysrt->ord);
 //cout << "Sorting order taken" << endl;
  myMeta.runlength = mysrt->l;   
  //cout << "trying to write" << endl;
  string metadataFilePath=f_path;
  metadataFilePath=metadataFilePath+".metadata";
  fstream outputFile(metadataFilePath.c_str(), fstream::out);
  outputFile.write((char *) &myMeta, sizeof(metadata));
  cout << "Metadata written" << endl;
  outputFile.close();
  cout << "saving into myvar" << endl;
  myInternalVar = new SortedFile(); 
  myInternalVar->sort_order = *(mysrt->ord);
  myInternalVar->runlen = mysrt->l;

  cout << "Sending to Sorted File" << endl;
  return myInternalVar->Create(f_path, f_type, startup);
}

}
void DBFile::Load (Schema &f_schema, char *loadpath) {
  myInternalVar->Load(f_schema,loadpath);
}

void DBFile::MoveFirst () 
 {
    myInternalVar->MoveFirst();
 }

 int DBFile::Close () 
 {
   return myInternalVar->Close();
 }

void DBFile::Add (Record &rec) 
{

  myInternalVar->Add(rec);
}

int DBFile::GetNext (Record &fetchme) 
{
  return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

  return myInternalVar->GetNext(fetchme,cnf,literal);
}
