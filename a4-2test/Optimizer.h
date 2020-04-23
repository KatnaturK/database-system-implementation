#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <iostream>
#include <vector>

#include "Comparison.h"
#include "Function.h"
#include "ParseTree.h"
#include "Schema.h"
#include "Statistics.h"

#define MAX_ATTRIBUTE_COUNT 100
#define MAX_RELATION_COUNT 12

using namespace std;

class OptimizerNode;

class Optimizer {
public:
  Optimizer(Statistics* stats);
  ~Optimizer() {}

private:
  Optimizer(const Optimizer&);
  Optimizer& operator = (const Optimizer&);
  
  int joinQueryCount;
  int selectQeryCount;
  OptimizerNode* opRootNode;
  Statistics* stats;
  AndList* isUsedPrev;
  vector<OptimizerNode*> nodes;

  void constructLeafNodes();
  void processJoins();
  void sortJoins();
  void ProcessSums();
  void processProjects();
  void makeDistinct();
  void makeWrite();
  int evalOrder(vector<OptimizerNode*> opNodeOperands, Statistics stats, int bestFound);
  void printNodes(ostream& os = cout);
  void printNodesInOrder(OptimizerNode* opNode, ostream& os = cout);
  void recycleList(AndList* alist) { concatList(isUsedPrev, alist); }
  static void concatList(AndList*& left, AndList*& right);
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

  virtual void print(ostream& os = cout, size_t level = 0) const;
  virtual void printOperator(ostream& os = cout, size_t level = 0) const;
  virtual void printSchema(ostream& os = cout, size_t level = 0) const;
  virtual void printAnnot(ostream& os = cout, size_t level = 0) const = 0;
  virtual void printPipe(ostream& os, size_t level = 0) const = 0;
  virtual void printChildren(ostream& os, size_t level = 0) const = 0;

  static AndList* pushSelection(AndList*& alist, Schema* target);
  static bool containedIn(OrList* ors, Schema* target);
  static bool containedIn(ComparisonOp* cmp, Schema* target);  
};

class LeafNode: public OptimizerNode {
  LeafNode (AndList*& boolean, AndList*& pushed, char* relation, char* alias, Statistics* stats);
  
  friend class Optimizer;
  Record recordLiteral;

  bool hasCNF() {return !selfOperand.isEmpty();}
  void printAnnot(ostream& os = cout, size_t level = 0) const;
  void printPipe(ostream& os, size_t level) const;
  void printChildren(ostream& os, size_t level) const {}

public:
  CNF selfOperand;
};

class UnaryNode: public OptimizerNode {
  friend class Optimizer;

protected:
  UnaryNode(const string& operand, Schema* outSchema, OptimizerNode* opNode, Statistics* stats);
  virtual ~UnaryNode() { delete childOpNode; }

  int inPipeId;

  void printPipe(ostream& os, size_t level) const;
  void printChildren(ostream& os, size_t level) const { childOpNode->print(os, level+1); }
  
public:
  OptimizerNode* childOpNode;
};

class BinaryNode: public OptimizerNode {
  friend class Optimizer;

protected:
  BinaryNode(const string& operand, OptimizerNode* l, OptimizerNode* r, Statistics* stats);
  virtual ~BinaryNode() { delete leftOpNode; delete rightOpNode; }

  int inLeftPipeId, inRightPipeId;

  void printPipe(ostream& os, size_t level) const;
  void printChildren(ostream& os, size_t level) const
  { leftOpNode->print(os, level+1); rightOpNode->print(os, level+1); }

public:
  OptimizerNode* leftOpNode;
  OptimizerNode* rightOpNode;
};

class ProjectNode: private UnaryNode {
  ProjectNode(NameList* atts, OptimizerNode* opNode);

  friend class Optimizer;
  int retainedAtts[MAX_ATTRIBUTE_COUNT];
  int inputAttsCount, numAttsOut;
  
  void printAnnot(ostream& os = cout, size_t level = 0) const;  
};

class DedupNode: private UnaryNode {
  DedupNode(OptimizerNode* opNode);

  friend class Optimizer;
  OrderMaker dedupOrder;

  void printAnnot(ostream& os = cout, size_t level = 0) const {}
};

class GroupByNode: private UnaryNode {
  GroupByNode(NameList* attsList, FuncOperator* parseTree, OptimizerNode* opNode);

  friend class Optimizer;
  OrderMaker orderMakerGrp;
  Function function;

  Schema* resultSchema(NameList* attsList, FuncOperator* parseTree, OptimizerNode* opNode);
  void printAnnot(ostream& os = cout, size_t level = 0) const;
};

class JoinNode: private BinaryNode {
  JoinNode(AndList*& boolean, AndList*& pushed, OptimizerNode* l, OptimizerNode* r, Statistics* stats);

  friend class Optimizer;
  CNF selOperand;
  Record recordLiteral;

  void printAnnot(ostream& os = cout, size_t level = 0) const;
};

class SelectPipeNode: private UnaryNode {
  SelectPipeNode(CNF selfOperand, char* relation, char* alias, OptimizerNode* opNode);

  friend class Optimizer;

  CNF selfOperand;
  Record recordLiteral;

  void printAnnot(ostream& os = cout, size_t level = 0) const;
};

class SumNode: private UnaryNode {
  SumNode(FuncOperator* parseTree, OptimizerNode* opNode);

  friend class Optimizer;
  Function function;

  Schema* resultSchema(FuncOperator* parseTree, OptimizerNode* opNode);
  void printAnnot(ostream& os = cout, size_t level = 0) const;
};

class WriteNode: private UnaryNode {
  WriteNode(FILE* outSchema, OptimizerNode* opNode);

  friend class Optimizer;
  FILE* outFile;

  void print(ostream& os = cout, size_t level = 0) const {}
  void printAnnot(ostream& os = cout, size_t level = 0) const;
};

#endif
