#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"
#include <fstream>
#include <iostream>

using namespace std;

HeapFile::HeapFile () {

int pageCount=0;
pageNumber=0;
pageInRead=true;
pageInWrite=false;
pageCount=0;

}

HeapFile::~HeapFile () {

}
 int HeapFile::Create (char *f_path, fType f_type, void *startup) {
 
  fstatus.open(f_path);
   if (fstatus.is_open())
       {
         f.Open(1,f_path);
         pageInWrite=true;
       } 
   else
       {
        f.Open(0,f_path);
        f.AddPage(&p,0);
        pageInWrite=true;
        pageNumber=0;
        pageCount=1;
       }

   return 1;
  }

 void HeapFile::Load (Schema &f_schema, char *loadpath) {
   Record tst;
   tableFile = fopen(loadpath, "r");
   int rcrdcntr=0;
      if (!tableFile) 
      {
       cerr << "\n Failed to open the file: " << loadpath << endl;
       return;
      }

   while(tst.SuckNextRecord(&f_schema, tableFile)) 
   {
     Add(tst);
     rcrdcntr++;
    }

  }

 int HeapFile::Open(char *f_path) {
  fstatus.open(f_path);
 if (fstatus.is_open())
    {
      f.Open(1,f_path);
    }
 else
    {
     f.Open(0,f_path);
    }

pageInRead=true;
pageCount=f.GetLength();
MoveFirst();

return 1;

}

void HeapFile::MoveFirst () 
 {
  if (f.GetLength() ==0) 
  {
   cerr << "DB-000: Invalid Operation ! Empty File !! " ;
  }
  else 
  {   
    if (pageInWrite)
     {
      f.AddPage(&p,pageNumber);
      p.EmptyItOut();
      pageNumber=0;
      f.GetPage(&p, pageNumber);
      pageInWrite=false;
      }

     else
     {
      p.EmptyItOut();
      pageNumber=0;
      f.GetPage(&p, pageNumber);
     }

  }
 }

 int HeapFile::Close () 
 {
   if (pageInWrite)
   {
    f.AddPage(&p,pageNumber);
    pageInWrite=false;
   }
   p.EmptyItOut();
   if (f.Close() >= 0) {
     return 1;
    }
    else 
    {
     return 0;
    }
 }

void HeapFile::Add (Record &rec) 
{

 if (pageInRead)
  {
     p.EmptyItOut();
     pageNumber = f.GetLength()-2;
     f.GetPage(&p,f.GetLength()-2);
     pageInRead = false;
     pageInWrite= true;
  }

 if(!p.Append(&rec))
  {
   f.AddPage(&p,pageNumber);
   p.EmptyItOut();
   pageNumber++; pageCount++;
   p.Append(&rec);
    
  }
 }

int HeapFile::GetNext (Record &fetchme) 
 {
  if(pageInWrite) 
  {
    f.AddPage(&p, pageNumber);
    pageInWrite = false;
    pageInRead=true;
    }
  if(p.GetFirst(&fetchme)) 
  {
    return 1;
  }
  else{
    if(pageNumber++ < f.GetLength()-2)
     {
       f.GetPage(&p, pageNumber);
        if (p.GetFirst(&fetchme)) 
        {
          return 1;
        }
     }
    else
    {
      return 0;
    }
  }
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {


 if(pageInWrite) 
 {
   f.AddPage(&p, pageNumber);
   pageInWrite = false;
   pageInRead=true;
  }

  while (GetNext(fetchme)) 
  {
    if (comp.Compare(&fetchme, &literal, &cnf)) 
    {
      return 1;
    }
  }
 return 0;
 }
