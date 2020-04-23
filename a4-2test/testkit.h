#ifndef ONEMOREDB_TESTKIT_H
#define ONEMOREDB_TESTKIT_H

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>

#include "Function.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;



#endif //ONEMOREDB_TESTKIT_H
