#include <fstream>
#include <iostream>

#include "HeapFile.h"

using namespace std;

HeapFile::HeapFile () {
  int pageCount = 0;
  pageNumber = 0;
  readPage = true;
  writePage = false;
  pageCount = 0;
}

HeapFile::~HeapFile () {}

int HeapFile::Create (char *filePath, fileTypeEnum fileType, void *startup) {
 
  fstatus.open (filePath);
  if (fstatus.is_open ()) {
    runFile.Open (1,filePath);
    writePage = true;

  } else {
      runFile.Open (0,filePath);
      runFile.AddPage (&p,0);
      writePage = true;
      pageNumber = 0;
      pageCount = 1;

  }
  return 1;
}

void HeapFile::Load (Schema &fileSchema, char *loadPath) {

  Record rec;
  tableFile = fopen (loadPath, "r");
  if (!tableFile) {
    cerr << "\n Failed to open the file: " << loadPath << endl;
    return;
  }

  while (rec.SuckNextRecord (&fileSchema, tableFile)) {
    Add (rec);
  }
}

int HeapFile::Open (char *filePath) {

  fstatus.open (filePath);
  if (fstatus.is_open())
    runFile.Open (1,filePath);
  else 
    runFile.Open (0,filePath);

  readPage = true;
  pageCount = runFile.GetLength ();
  cout << "PAGE COUNT " << pageCount << "\n";
  MoveFirst ();
  return 1;
}

void HeapFile::MoveFirst () {

  if (runFile.GetLength () == 0) 
   cerr << "DB-000: Invalid Operation ! Empty File !!\n" ;
  else {   
    if (writePage) {
      runFile.AddPage (&p,pageNumber);
      p.EmptyItOut ();
      pageNumber = 0;
      runFile.GetPage (&p, pageNumber);
      writePage = false;

    } else {
      p.EmptyItOut();
      pageNumber=0;
      runFile.GetPage(&p, pageNumber);

    }
  }
 }

int HeapFile::Close () {

  if (writePage) {
    runFile.AddPage (&p,pageNumber);
    writePage = false;

  } 
  p.EmptyItOut ();
  if (runFile.Close () >= 0)
    return 1;
  else 
    return 0;

}

void HeapFile::Add (Record &rec) {

 if (readPage) {
    p.EmptyItOut ();
    pageNumber = runFile.GetLength ()- 2;
    runFile.GetPage (&p,runFile.GetLength ()- 2);
    readPage = false;
    writePage= true;
  }

  if (!p.Append (&rec)) {
    runFile.AddPage (&p,pageNumber);
    p.EmptyItOut ();
    pageCount++;
    pageNumber++;
    p.Append (&rec);

  }
}

int HeapFile::GetNext (Record &fetchMe) {

  if (writePage) {
    runFile.AddPage(&p, pageNumber);
    writePage = false;
    readPage=true;

  }

  if (p.GetFirst (&fetchMe)) 
    return 1;
  else {
    if (pageNumber++ < runFile.GetLength () - 2) {
      runFile.GetPage (&p, pageNumber);
      if (p.GetFirst (&fetchMe)) 
        return 1;
    }
    else
      return 0;

  }
}

int HeapFile::GetNext (Record &fetchMe, CNF &cnf, Record &literal) {

  if (writePage) {
    runFile.AddPage (&p, pageNumber);
    writePage = false;
    readPage = true;
  }

  while (GetNext (fetchMe)) {
    if (ce.Compare (&fetchMe, &literal, &cnf)) 
      return 1;
  }
  
  return 0;
}
