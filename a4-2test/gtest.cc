#include "gtest/gtest.h"
#include "ParseTree.h"
#include "Optimizer.h"

using namespace std;

extern "C" {
  int yyparse(void);   // defined in y.tab.c
}

char* catalog_path = "catalog";
char* dbfile_dir = "";
char* tpch_dir = "";

Statistics stats;

TEST (OPTIMIZER_TEST, CHECK_JOIN_QUERY_COUNT) {
	Optimizer* optimizer = new Optimizer(&stats);
	ASSERT_EQ(optimizer->joinQueryCount, 2);
}


TEST (OPTIMIZER_TEST, CHECK_SELECT_QUERY_COUNT) {
	Optimizer* optimizer = new Optimizer(&stats);
	ASSERT_EQ(optimizer->selectQueryCount, 1);
}


int main (int argc, char *argv[]) {
	if (yyparse() != 0) {
		std::cout << "Invalid CNF.\n";
		exit (1);
	}
	char *fileName = "Statistics.txt";
	stats.LoadRelStats();
	stats.Write(fileName);
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS ();
}

