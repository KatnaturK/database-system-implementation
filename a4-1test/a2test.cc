#include "test.h"

void initializeDBFiles(relation *rel) {
    DBFile dbfile;

    dbfile.Create (rel->path(), heap, NULL);

    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
    cout << " tpch file will be loaded from " << tbl_path << endl;

    dbfile.Load (*(rel->schema ()), tbl_path);
    dbfile.Close();
}

int main () {

    setup ();

    relation *rel_ptr[] = {s, n, r, c, p, ps, o, li};

    for (int i = 0 ; i < 8 ; i++) {
        initializeDBFiles(rel_ptr[i]);
    }

    cleanup ();
}