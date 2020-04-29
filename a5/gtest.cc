#include "gtest/gtest.h"
#include <iostream>
#include <cstring>
#include <climits>
#include <string>
#include <algorithm>

#include "Errors.h"
#include "Main.h"
#include "ParseTree.h"
#include "QueryPlan.h"
#include "Statistics.h"
#include "Ddl.h"

extern "C" {
  int yyparse(void);     // defined in y.tab.c
  struct YY_BUFFER_STATE* yy_scan_string(const char*);
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* deoutput;
extern struct AttrList *newattrs; //Use this to build Attribute* array and record schema

using namespace std;

char* catalog_path = "catalog";
char* dbfile_dir = "bin/";
char* tpch_dir = "../DATA/1G";

char *fileName = "Statistics.txt";
Statistics stats;
Ddl dataDef;
QueryPlan plan(&stats);


TEST (DATABASE, CREATE_TABLE) {
	char* cnf = "CREATE TABLE gtestTable(attribute_value INTEGER, attribute_value STRING) AS HEAP;";
	yy_scan_string(cnf);
	yyparse();
	stats.Read(fileName);
	ASSERT_EQ(true, dataDef.createTable());
}

TEST (DATABASE, DROP_TABLE) {
	char* cnf = "DROP TABLE gtestTable;";
	yy_scan_string(cnf);
	yyparse();
	stats.Read(fileName);
	ASSERT_EQ(true, dataDef.dropTable());
}

int main(int argc, char *argv[]) {
	testing::InitGoogleTest(&argc, argv); 
	return RUN_ALL_TESTS ();
}
