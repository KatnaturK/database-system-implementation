
#include <iostream>
#include "Record.h"
#include "File.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {
	char* filename = "datagen/tpch-dbgen/region.tbl";

	// // try to parse the CNF
	// cout << "Enter in your CNF: ";
	// if (yyparse() != 0) {
	// 	cout << "Can't parse your CNF.\n";
	// 	exit (1);
	// }

	// suck up the schema from the file
	// Schema lineitem ("catalog", "lineitem");

	// // grow the CNF expression from the parse tree 
	// CNF myComparison;
	// Record literal;
	// myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// // print out the comparison to the screen
	// myComparison.Print ();

	// now open up the text file and start procesing it
	// FILE *tableFile = fopen ("datagen/tpch-dbgen/lineitem.tbl", "r");

	// Record temp;
	// Schema mySchema ("catalog", "lineitem");

	File myFile;
	myFile.Open(1, filename);

	cout << myFile.Close() << "\n";

	Page* currentPage;
	char* bits;
	// cout << sizeof(*bits) << "\n";
	myFile.GetPage(currentPage, -10);
	currentPage->ToBinary(bits);
	cout << bits << "\n";

	// FILE *tempFile = fopen("temp.tbl", "wb");
	// fwrite(bits, 1, sizeof(bits), tempFile);

	// char *bits = literal.GetBits ();
	// cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	// literal.Print (&supplier);

		// read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
	// while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
	// 	counter++;
	// 	if (counter % 10000 == 0) {
	// 		cerr << counter << "\n";
	// 	}

	// 	if (comp.Compare (&temp, &literal, &myComparison))
	// 		temp.Print (&mySchema);

	// }

}


