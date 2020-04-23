#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
using namespace std;

class RelationStatistics {
private:
    long tupleCount;
    map<string,int> attributes;
    string groupName;
    int groupLength;
    
public:
    RelationStatistics(long inTupleCount, string inGroupName) : tupleCount(inTupleCount), groupName(inGroupName) {
        groupLength = 1;
    }

    RelationStatistics(RelationStatistics &copyMe) {

         tupleCount = copyMe.GetTupleCount();
         map<string,int> *relAttrPtr = copyMe.GetRelationAttributes();
         map<string,int>::iterator mapIterator;

         for( mapIterator = relAttrPtr->begin(); mapIterator != relAttrPtr->end(); mapIterator++)
            attributes[mapIterator->first] = mapIterator->second;
         
		 groupLength = copyMe.groupLength;
         groupName = copyMe.groupName;
    } 

    ~RelationStatistics() { 
		attributes.clear();
	}

    void UpdateCount(int count) {
        tupleCount = count;
    }

    void UpdateData(string str, int distinctCount) {
        attributes[str] = distinctCount;
    }
    map<string,int> * GetRelationAttributes() { 
        return &attributes;
    }

    long GetTupleCount() { 
        return tupleCount;
    }
    
	string GetGroupName() {
        return groupName;
    }

    int GetGroupLength() {
        return groupLength;
    }

    void SetGroupDetails(string inGroupName,int inGroupLength) {
        groupName = inGroupName;
        groupLength = inGroupLength;
    }
};

class Statistics {
private:
    map<string, RelationStatistics*> relStatsMap;

public:
	Statistics();
	Statistics(Statistics &copyMe); // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	void CopyRel(char *oldName, char *newName);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	void Read(char *fromWhere);
	void Write(char *fromWhere);
    void LoadAllStatistics();

	double Assess(struct OrList *orList, map<string,long> &distinctValues);
	bool AtrrtibutePresent(char *attributeValue, char *relNames[], int numToJoin, map<string,long> &distinctValues);
	bool GivesError(struct AndList *parseTree, char *relNames[], int numToJoin, map<string,long> &distinctValues);

	map<string,RelationStatistics*>* getRelStatsMap() {
		return &relStatsMap;
	}
};

#endif
