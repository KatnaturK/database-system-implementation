#include <iostream>

#include "ParseTree.h"
#include "Optimizer.h"

using namespace std;

char* catalog_path = "catalog";
char* dbfile_dir = "";
char* tpch_dir = "";

extern "C" {
  int yyparse(void);   // defined in y.tab.c
}

int main (int argc, char* argv[]) {
  if (yyparse() != 0) {
    std::cout << "Can't parse your CNF.\n";
    exit (1);
  }
  Statistics stats;
  char *statsFile = "Statistics.txt";
  stats.LoadRelStats();
  stats.Write(statsFile);
  // statistics.Read(statsFile);
  Optimizer optimizer(&stats);
  return 0;
}
