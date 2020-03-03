#include "GenericDBFile.h"

GenericDBFile::GenericDBFile () {} 

int GenericDBFile:: Create (char *filePath, fileTypeEnum fileEnum, void *startup) {}

int GenericDBFile:: Open(char *filePath) {}

int GenericDBFile:: Close () {}

void GenericDBFile:: Load (Schema &fileSchema, char *loadPath) {}

void GenericDBFile:: MoveFirst () {}

void GenericDBFile:: Add (Record &rec) {}

int GenericDBFile:: GetNext (Record &fetchMe) {}

int GenericDBFile:: GetNext (Record &fetchMe, CNF &cnf, Record &literal) {}

GenericDBFile::~GenericDBFile () {};
