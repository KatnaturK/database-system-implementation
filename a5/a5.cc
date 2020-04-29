#include <iostream>

#include "Main.h"

using namespace std;

char* catalog_path = "catalog";
char* dbfile_dir = "";
char* tpch_dir = "";

int main (int argc, char* argv[]) {
  Main it;
  it.run();
  return 0;
}
