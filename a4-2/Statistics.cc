#include "Statistics.h"
#include <iostream>
#include <map>
#include <set>
#include <stdlib.h>
#include <fstream>
#include <math.h>
#include <string.h>


Statistics::Statistics() {
    relationMap = new map<string, int>();
    attrMap = new map<string, map<string, int> >();
}


Statistics::Statistics(Statistics &copyMe) {
    relationMap = new map<string, int>(*(copyMe.relationMap));
    attrMap = new map<string, map<string, int> >(*(copyMe.attrMap));
}


Statistics::~Statistics() {
    delete relationMap;
    delete attrMap;
}

void Statistics::AddRel(char *relName, int numTuples) {
    string relation(relName);

    auto result = relationMap->insert(pair<string, int>(relation, numTuples));

    //replace existing
    if (!result.second) {
        relationMap->erase(result.first);
        relationMap->insert(pair<string, int>(relation, numTuples));
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    string relation(relName);
    string attr(attName);

    if (numDistincts == -1) {
        int numTuples = relationMap->at(relation);
        (*attrMap)[relation][attr] = numTuples;
    } else {
        (*attrMap)[relation][attr] = numDistincts;
    }
}

void Statistics::CopyRel(char *oldName1, char *newName1) {
    string oldName(oldName1);
    string newName(newName1);


    int oldNumTuples = (*relationMap)[oldName];
    (*relationMap)[newName] = oldNumTuples;

    map<string, int> &oldAttrMap = (*attrMap)[oldName];

    for (auto oldAttrInfo = oldAttrMap.begin();
         oldAttrInfo != oldAttrMap.end(); ++oldAttrInfo) {
        string newAtt = newName;
        newAtt += "." + oldAttrInfo->first;
        (*attrMap)[newName][newAtt] = oldAttrInfo->second;
    }
}

void Statistics::Read(char *fromWhere) {
    relationMap->clear();
    attrMap->clear();

    ifstream file_exists(fromWhere);
    if (!file_exists) {
        cerr << "The give file_path '" << fromWhere << "' doest not exist";
        return;
    }

    string fileName(fromWhere);
    ifstream statFile;
    statFile.open(fileName.c_str(), ios::in);

    string str;
    statFile >> str;
    int relationCount = atoi(str.c_str());


    for (int i = 0; i < relationCount; i++) {
        statFile >> str;

        size_t index = str.find_first_of(':');
        string relationName = str.substr(0, index);
        string numOfTupleStr = str.substr(index + 1);

        (*relationMap)[relationName] = atoi(numOfTupleStr.c_str());
    }

    statFile >> str;
    string relName, attrName, distinctCount;
    statFile >> relName >> attrName >> distinctCount;

    while (!statFile.eof()) {
        int distinctCountInt = atoi(distinctCount.c_str());
        (*attrMap)[relName][attrName] = distinctCountInt;
        statFile >> relName >> attrName >> distinctCount;
    }
    statFile.close();
}

void Statistics::Write(char *fromWhere) {
    string fileName(fromWhere);
    //delete existing file
    remove(fromWhere);

    ofstream statFile;
    statFile.open(fileName.c_str(), ios::out);

    statFile << relationMap->size() << "\n";

    for (auto entry = relationMap->begin(); entry != relationMap->end(); entry++)
        statFile << entry->first.c_str() << ":" << entry->second << "\n";

    statFile << attrMap->size() << "\n";

    for (auto relItr = attrMap->begin(); relItr != attrMap->end(); ++relItr)
        for (auto attrItr = relItr->second.begin(); attrItr != relItr->second.end(); ++attrItr)
            statFile << (*relItr).first.c_str() << " " << (*attrItr).first.c_str() << " " << (*attrItr).second << "\n";

    statFile.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    struct AndList *curAnd;
    struct OrList *curOr;

    map<string, int> opratorMap;
    curAnd = parseTree;
    while (curAnd != NULL) {
        curOr = curAnd->left;
        while (curOr != NULL) {
            opratorMap[curOr->left->left->value] = curOr->left->code;
            curOr = curOr->rightOr;
        }
        curAnd = curAnd->rightAnd;
    }

    double result = Estimate(parseTree, relNames, numToJoin);

    map<string, int>::iterator opMapItr, countMapItr;
    set<string> joinAttrSet;
    if (joinFlag2) {
        for (opMapItr = opratorMap.begin(); opMapItr != opratorMap.end(); opMapItr++) {
            for (int i = 0; (i < relationMap->size()) && (i < numToJoin); i++) {
                if (relNames[i] == NULL)
                    continue;
                int count = ((*attrMap)[relNames[i]]).count(opMapItr->first);
                if (count == 0)
                    continue;
                else if (count == 1) {
                    for (countMapItr = (*attrMap)[relNames[i]].begin();
                         countMapItr != (*attrMap)[relNames[i]].end(); countMapItr++) {
                        if ((opMapItr->second == LESS_THAN) || (opMapItr->second == GREATER_THAN)) {
                            (*attrMap)[joinLeftRel + "_" +
                                       joinRightRel][countMapItr->first] = (int) round(
                                    (double) (countMapItr->second) / 3.0);
                        } else if (opMapItr->second == EQUALS) {
                            if (opMapItr->first == countMapItr->first)
                                (*attrMap)[joinLeftRel + "_" + joinRightRel][countMapItr->first] = 1;
                            else
                                (*attrMap)[joinLeftRel + "_" + joinRightRel][countMapItr->first] = min(
                                        (int) round(result), countMapItr->second);
                        }
                    }
                    break;
                } else if (count > 1) {
                    for (countMapItr = (*attrMap)[relNames[i]].begin();
                         countMapItr != (*attrMap)[relNames[i]].end(); countMapItr++) {
                        if (opMapItr->second == EQUALS) {
                            if (opMapItr->first == countMapItr->first) {
                                (*attrMap)[joinLeftRel + "_" +
                                           joinRightRel][countMapItr->first] = count;
                            } else
                                (*attrMap)[joinLeftRel + "_" +
                                           joinRightRel][countMapItr->first] = min(
                                        (int) round(result), countMapItr->second);
                        }
                    }
                    break;
                }
                joinAttrSet.insert(relNames[i]);
            }
        }

        if (joinAttrSet.count(joinLeftRel) == 0) {
            for (auto entry = (*attrMap)[joinLeftRel].begin();
                 entry != (*attrMap)[joinLeftRel].end(); entry++) {
                (*attrMap)[joinLeftRel + "_" + joinRightRel][entry->first] = entry->second;

            }
        }
        if (joinAttrSet.count(joinRightRel) == 0) {
            for (auto entry = (*attrMap)[joinRightRel].begin();
                 entry != (*attrMap)[joinRightRel].end(); entry++) {
                (*attrMap)[joinLeftRel + "_" + joinRightRel][entry->first] = entry->second;

            }
        }
        (*relationMap)[joinLeftRel + "_" + joinRightRel] = round(result);

        relationMap->erase(joinLeftRel);
        relationMap->erase(joinRightRel);
        attrMap->erase(joinLeftRel);
        attrMap->erase(joinRightRel);

    } else {
        // find the rel for which the attr belongs to
        string relName;
        for (auto entry = opratorMap.begin(); entry != opratorMap.end(); entry++) {
            for (auto mapEntry = attrMap->begin(); mapEntry != attrMap->end(); ++mapEntry)
                if ((*attrMap)[mapEntry->first].count(entry->first) > 0) {
                    relName = mapEntry->first;
                    break;
                }
        }
        (*relationMap)[relName] = round(result);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {

    struct AndList *curAnd;
    struct OrList *curOr;
    string leftRel, rightRel, leftAttr, rightAttr, prevAttr;
    bool isPrevious = false;
    bool isAnotherPrevious = false;
    double result = 0.0;
    double andResult = 1.0;
    double orResult = 1.0;

    curAnd = parseTree;

    while (curAnd != NULL) {
        curOr = curAnd->left;
        orResult = 1.0;

        while (curOr != NULL) {
            ComparisonOp *compOp = curOr->left;
            joinFlag1 = false;
            leftAttr = compOp->left->value;

            if (leftAttr == prevAttr)
                isPrevious = true;
            prevAttr = leftAttr;

            for (auto iterator = attrMap->begin();
                 iterator != attrMap->end(); iterator++) {
                if ((*attrMap)[iterator->first].count(leftAttr) > 0) {
                    leftRel = iterator->first;
                    break;
                }
            }


            if (compOp->right->code == NAME) {
                joinFlag1 = true;
                joinFlag2 = true;
                rightAttr = compOp->right->value;

                for (auto iterator = attrMap->begin();
                     iterator != attrMap->end(); ++iterator) {
                    if ((*attrMap)[iterator->first].count(rightAttr) > 0) {
                        rightRel = iterator->first;
                        break;
                    }
                }
            }

            if (joinFlag1) {
                if (compOp->code == EQUALS)
                    orResult *= (1.0 - (1.0 / max((*attrMap)[leftRel][compOp->left->value],
                                                  (*attrMap)[rightRel][compOp->right->value])));
                joinLeftRel = leftRel;
                joinRightRel = rightRel;
            } else {
                if (isPrevious) {
                    if (!isAnotherPrevious) {
                        orResult = 1.0 - orResult;
                        isAnotherPrevious = true;
                    }
                    if (compOp->code == GREATER_THAN || compOp->code == LESS_THAN)
                        orResult += (1.0 / 3.0);

                    if (compOp->code == EQUALS)
                        orResult += (1.0 / ((*attrMap)[leftRel][compOp->left->value]));

                } else {
                    if (compOp->code == GREATER_THAN || compOp->code == LESS_THAN)
                        orResult *= (2.0 / 3.0);

                    if (compOp->code == EQUALS)
                        orResult *= (1.0 - (1.0 / (*attrMap)[leftRel][compOp->left->value]));
                }
            }
            curOr = curOr->rightOr;
        }

        if (!isPrevious)
            orResult = 1.0 - orResult;

        isPrevious = false;
        isAnotherPrevious = false;

        andResult *= orResult;
        curAnd = curAnd->rightAnd;
    }

    double rightCount;
    if (rightRel.empty())
        rightCount = 0;
    else
        rightCount = (*relationMap)[rightRel];


    if (joinFlag2)
        result = (*relationMap)[joinLeftRel] * rightCount * andResult;
    else
        result = (*relationMap)[leftRel] * andResult;

    return result;
}