#ifndef NODE_H
#define NODE_H

#include <iostream>
#include <string>
#include <vector>
#include <stdio.h>
#include "ParseTree.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Schema.h"
#include "Function.h"
#include "Pipe.h"
#include "RelOp.h"

enum OpType {
    SELECT_FILE, SELECT_PIPE, PROJECT, JOIN, GROUPBY, DUPLICATE_REMOVAL, SUM
};

static int PIPE_ID = 0;

static int generatePipeId() {
    return PIPE_ID++;
}


class Operator {
protected:
    int pipeId = generatePipeId();
    OpType opType;
    Schema *outputSchema = nullptr;

public:
    Operator *left = nullptr, *right = nullptr;
//    Pipe outPipe = Pipe(50);

    virtual void print() = 0;

    int getPipeID() { return pipeId; };

    Schema *getSchema() { return outputSchema; };

    OpType getType() { return opType; }
};

class SelectFileOperator : public Operator {
private:
    CNF cnf;
    Record literal;
public:
    SelectFileOperator(AndList *selectList, Schema *schema, string relName);

    void print() override;
};

class SelectPipeOperator : public Operator {
private:
    CNF cnf;
    Record literal;

public:
    SelectPipeOperator(Operator *child, AndList *selectList);

    void print();
};

class ProjectOperator : public Operator {
private:
    int *keepMe;
    NameList *attsLeft;

public:
    ProjectOperator(Operator *child, NameList *attrsLeft);

    void print();
};

class JoinOperator : public Operator {
private:
    CNF cnf;
    Record literal;
public:
    JoinOperator(Operator *leftChild, Operator *rightChild, AndList *joinList);

    void print();
};

class DuplicateRemovalOperator : public Operator {
public:
    DuplicateRemovalOperator(Operator *child);

    void print();
};

class SumOperator : public Operator {
private:
    FuncOperator *funcOperator;
    Function function;

public:
    SumOperator(Operator *child, FuncOperator *func);

    void print();
};


class GroupByOperator : public Operator {
private:
    FuncOperator *funcOperator;
    OrderMaker groupOrder;
    Function function;

    void getOrder(NameList *groupingAtts);

    void createOutputSchema();

public:
    GroupByOperator(Operator *child, NameList *groupingAtts, FuncOperator *func);

    void print();
};

#endif