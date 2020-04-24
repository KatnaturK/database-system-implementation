#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <algorithm>
#include <climits>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "Comparison.h"
#include "Defs.h"
#include "Function.h"
#include "ParseTree.h"
#include "Schema.h"
#include "Statistics.h"

#define MAX_ATTRIBUTE_COUNT 100
#define MAX_RELATION_COUNT 12

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

using namespace std;

class OptimizerNode;

class Optimizer {
public:
  Optimizer() {
   isUsedPrev = NULL;
   joinQueryCount = 0;
   selectQueryCount = 0;
  };
  Optimizer(Statistics* stats);
  ~Optimizer() {}

  int joinQueryCount;
  int selectQueryCount;
  OptimizerNode* opRootNode;
  Statistics* stats;
  AndList* isUsedPrev;
  vector<OptimizerNode*> nodes;

private:
  Optimizer(const Optimizer&);
  Optimizer& operator = (const Optimizer&);
  
  static void concatList(AndList*& left, AndList*& right);

  void constructLeafNodes();
  int evalOrder(vector<OptimizerNode*> opNodeOperands, Statistics stats, int bestFound);
  void processJoins();
  void ProcessSums();
  void processProjects();
  void processDistinct();
  void processWrite();
  void printNodes(ostream& os = cout);
  void printNodesInOrder(OptimizerNode* opNode, ostream& os = cout);
  void recycleList(AndList* alist) { concatList(isUsedPrev, alist); }
  void sortJoins();
};

class OptimizerNode {
  friend class BinaryNode;
  friend class DedupNode;
  friend class GroupByNode;
  friend class JoinNode;
  friend class Optimizer;
  friend class ProjectNode;
  friend class SumNode;
  friend class UnaryNode;
  friend class WriteNode;

public:
  int childNodeCount;
  virtual ~OptimizerNode();

protected:
  OptimizerNode(const string& op, Schema* outSchema, Statistics* stats);
  OptimizerNode(const string& op, Schema* outSchema, char* relation, Statistics* stats);
  OptimizerNode(const string& op, Schema* outSchema, char* relations[], size_t num, Statistics* stats);

  int outPipeId;
  static int pipeId;
  double processingEstimation, processingCost;
  size_t relationCount;
  string operand;
  Schema* outSchema;
  char* relations[MAX_RELATION_COUNT];
  Statistics* stat;

  virtual void print(ostream& os = cout) const;
  virtual void printOperation(ostream& os = cout) const = 0;
  virtual void printPipe(ostream& os) const = 0;
  virtual void printChildren(ostream& os) const = 0;

  static AndList* pushSelection(AndList*& alist, Schema* target);
  static bool containedIn(OrList* ors, Schema* target);
  static bool containedIn(ComparisonOp* cmp, Schema* target);  
};

class LeafNode: public OptimizerNode {
  LeafNode (AndList*& boolean, AndList*& pushed, char* relation, char* alias, Statistics* stats);
  
  friend class Optimizer;
  Record recordLiteral;

  bool hasCNF() {return !selfOperand.isEmpty();}
  void printOperation(ostream& os = cout) const;
  void printPipe(ostream& os) const;
  void printChildren(ostream& os) const {}

public:
  CNF selfOperand;
};

class UnaryNode: public OptimizerNode {
  friend class Optimizer;

protected:
  UnaryNode(const string& operand, Schema* outSchema, OptimizerNode* opNode, Statistics* stats);
  virtual ~UnaryNode() { delete childOpNode; }

  int inPipeId;

  void printPipe(ostream& os) const;
  void printChildren(ostream& os) const { childOpNode->print(os); }
  
public:
  OptimizerNode* childOpNode;
};

class BinaryNode: public OptimizerNode {
  friend class Optimizer;

protected:
  BinaryNode(const string& operand, OptimizerNode* l, OptimizerNode* r, Statistics* stats);
  virtual ~BinaryNode() { delete leftOpNode; delete rightOpNode; }

  int inLeftPipeId, inRightPipeId;

  void printPipe(ostream& os) const;
  void printChildren(ostream& os) const
  { leftOpNode->print(os); rightOpNode->print(os); }

public:
  OptimizerNode* leftOpNode;
  OptimizerNode* rightOpNode;
};

class ProjectNode: private UnaryNode {
  ProjectNode(NameList* atts, OptimizerNode* opNode);

  friend class Optimizer;
  int retainedAtts[MAX_ATTRIBUTE_COUNT];
  int inputAttsCount, numAttsOut;
  
  void printOperation(ostream& os = cout) const;  
};

class DedupNode: private UnaryNode {
  DedupNode(OptimizerNode* opNode);

  friend class Optimizer;
  OrderMaker dedupOrderMaker;

  void printOperation(ostream& os = cout) const {}
};

class GroupByNode: private UnaryNode {
  GroupByNode(NameList* attsList, FuncOperator* parseTree, OptimizerNode* opNode);

  friend class Optimizer;
  OrderMaker orderMakerGrp;
  Function function;
  Schema* inSchema;

  Schema* resultSchema(NameList* attsList, FuncOperator* parseTree, OptimizerNode* opNode);
  void printOperation(ostream& os = cout) const;
};

class JoinNode: private BinaryNode {
  JoinNode(AndList*& boolean, AndList*& pushed, OptimizerNode* l, OptimizerNode* r, Statistics* stats);

  friend class Optimizer;
  CNF* selOperand;
  Record recordLiteral;

  void printOperation(ostream& os = cout) const;
};

class SelectPipeNode: private UnaryNode {
  SelectPipeNode(CNF selfOperand, char* relation, char* alias, OptimizerNode* opNode);

  friend class Optimizer;

  CNF selfOperand;
  Record recordLiteral;

  void printOperation(ostream& os = cout) const;
};

class SumNode: private UnaryNode {
  SumNode(FuncOperator* parseTree, OptimizerNode* opNode);

  friend class Optimizer;
  Function function;
  Schema* inSchema;

  Schema* resultSchema(FuncOperator* parseTree, OptimizerNode* opNode);
  void printOperation(ostream& os = cout) const;
};

class WriteNode: private UnaryNode {
  WriteNode(FILE* outSchema, OptimizerNode* opNode);

  friend class Optimizer;
  FILE* outFile;

  void print(ostream& os = cout) const {}
  void printOperation(ostream& os = cout) const;
};

#endif
