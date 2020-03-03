#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>
#include "GenericDBFile.h"


       GenericDBFile::GenericDBFile (){
        } 
        

      int  GenericDBFile:: Create (char *fpath, fType file_type, void *startup) {

       }

      int  GenericDBFile:: Open(char *fpath){

       }

      int  GenericDBFile:: Close (){

       }

      void  GenericDBFile:: Load (Schema &myschema, char *loadpath){
      
       }

      void  GenericDBFile:: MoveFirst (){

       }

     void  GenericDBFile:: Add (Record &addme){
      
       }

     int  GenericDBFile:: GetNext (Record &fetchme){

       }

     int  GenericDBFile:: GetNext (Record &fetchme, CNF &cnf, Record &literal){

       }

       GenericDBFile::~GenericDBFile (){

       };
