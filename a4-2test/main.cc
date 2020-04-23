#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include <limits.h>
#include <cstring>
#include "Meta.h"
#include <chrono>
#include "Statistics.h"

using namespace std;

int main() {

    /*SortInfo* sortInfo=new SortInfo;
    sortInfo->runLength=2;
    OrderMaker* orderMaker=new(OrderMaker);

    char cur_dir[PATH_MAX];
    char dbfile_dir[PATH_MAX];
    char table_path[PATH_MAX];
    char catalog_path[PATH_MAX];
    char tempfile_path[PATH_MAX];
    if (getcwd(cur_dir, sizeof(cur_dir)) != NULL) {
        clog <<"current working dir:" << cur_dir << endl;
        strcpy(dbfile_dir,cur_dir);
        strcpy(table_path,cur_dir);
        strcpy(catalog_path,cur_dir);
        strcpy(tempfile_path,cur_dir);
        strcat(dbfile_dir,"/test/test.bin");
        strcat(table_path,"/test/nation.tbl");
        strcat(catalog_path,"/test/catalog");
        strcat(tempfile_path,"/test/tempfile");
    } else {
        cerr << "error while getting curent dir" << endl;
        return 1;
    }


//    for part table
    strcpy(table_path,"/home/deepak/Desktop/dbi/tpch-dbgen/1GB/customer.tbl");
    Schema nation (catalog_path, (char*)"customer");
    orderMaker->numAtts=1;
    orderMaker->whichTypes[0]=String;
    orderMaker->whichAtts[0]=6;
    sortInfo->myOrder=orderMaker;

//  for nation table
//    Schema nation (catalog_path, (char*)"nation");
//    orderMaker->numAtts=1;
//    orderMaker->whichTypes[0]=String;
//    orderMaker->whichAtts[0]=1;
//    sortInfo->myOrder=orderMaker;



    DBFile* dbFile=new DBFile();

//    dbFile->Open(dbfile_dir);

    cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF.\n";
        exit (1);
    }
    Record literal;
    CNF sort_pred;
    sort_pred.GrowFromParseTree (final, &nation, literal); // constructs CNF predicate

    dbFile->Create(dbfile_dir,sorted,sortInfo);
    dbFile->Load(nation,table_path);
    dbFile->MoveFirst();

    Record record;
    int counter = 0;
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    while (dbFile->GetNext (record,sort_pred,literal) == 1) {
        if(counter==0){
            cout<< "first record:" <<endl;
//            record.Print(&nation);
        }
//        record.Print(&nation);
        counter ++;
    }
    cout<< "last record:" <<endl;
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

    std::cout << "Time taken for the query = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;
    //    record.Print(&nation);

    cout << " scanned " << counter << " recs \n";
    dbFile->Close();*/

    Statistics s;
    char *relName[] = {"supplier","partsupp"};


    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "s_suppkey",10000);

    s.AddRel(relName[1],800000);
    s.AddAtt(relName[1], "ps_suppkey", 10000);

    s.CopyRel("supplier","supplier2");

    cout << "done" <<endl;
}