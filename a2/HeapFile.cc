#include <fstream>
#include <iostream>

#include "HeapFile.h"

using namespace std;

HeapFile::HeapFile () {
  int pageCount = 0;
  pageNumber = 0;
  pageInRead = true;
  pageInWrite = false;
  pageCount = 0;
}

HeapFile::~HeapFile () {}

int HeapFile::Create (char *filePath, fileTypeEnum fileType, void *startup) {
 
  fstatus.open (filePath);
  if (fstatus.is_open ()) {
    f.Open (1,filePath);
    pageInWrite = true;

  } else {
      f.Open (0,filePath);
      f.AddPage (&p,0);
      pageInWrite = true;
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
    f.Open (1,filePath);
  else 
    f.Open (0,filePath);

  pageInRead = true;
  pageCount = f.GetLength ();
  MoveFirst ();
  return 1;
}

void HeapFile::MoveFirst () {

  if (f.GetLength () == 0) 
   cerr << "DB-000: Invalid Operation ! Empty File !! " ;
  else {   
    if (pageInWrite) {
      f.AddPage (&p,pageNumber);
      p.EmptyItOut ();
      pageNumber = 0;
      f.GetPage (&p, pageNumber);
      pageInWrite = false;

    } else {
      p.EmptyItOut();
      pageNumber=0;
      f.GetPage(&p, pageNumber);

    }
  }
 }

int HeapFile::Close () {

  if (pageInWrite) {
    f.AddPage (&p,pageNumber);
    pageInWrite = false;

  } 
  p.EmptyItOut ();
  if (f.Close () >= 0)
    return 1;
  else 
    return 0;

}

void HeapFile::Add (Record &rec) {

 if (pageInRead) {
    p.EmptyItOut ();
    pageNumber = f.GetLength ()- 2;
    f.GetPage (&p,f.GetLength ()- 2);
    pageInRead = false;
    pageInWrite= true;
  }

  if (!p.Append (&rec)) {
    f.AddPage (&p,pageNumber);
    p.EmptyItOut ();
    pageCount++;
    pageNumber++;
    p.Append (&rec);

  }
}

int HeapFile::GetNext (Record &fetchMe) {

  if (pageInWrite) {
    f.AddPage(&p, pageNumber);
    pageInWrite = false;
    pageInRead=true;

  }

  if (p.GetFirst (&fetchMe)) 
    return 1;
  else {
    if (pageNumber++ < f.GetLength () - 2) {
      f.GetPage (&p, pageNumber);
      if (p.GetFirst (&fetchMe)) 
        return 1;
    }
    else
      return 0;

  }
}

int HeapFile::GetNext (Record &fetchMe, CNF &cnf, Record &literal) {

  if (pageInWrite) {
    f.AddPage (&p, pageNumber);
    pageInWrite = false;
    pageInRead = true;
  }

  while (GetNext (fetchMe)) {
    if (comp.Compare (&fetchMe, &literal, &cnf)) 
      return 1;
  }
  
  return 0;
}
