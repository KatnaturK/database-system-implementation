#ifndef STATISTICS_
#define STATISTICS_

#include "ParseTree.h"
#include <string>
#include <map>

using namespace std;

class Statistics {

public:
    Statistics();

    Statistics(Statistics &copyMe);

    ~Statistics();

    map<string, int> *relationMap;
    map<string, map<string, int> > *attrMap;
    bool joinFlag1 = false;
    bool joinFlag2 = false;
    string joinLeftRel, joinRightRel;

    void AddRel(char *relName, int numTuples);

    void AddAtt(char *relName, char *attName, int numDistincts);

    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);

    void Write(char *fromWhere);

    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);

    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif