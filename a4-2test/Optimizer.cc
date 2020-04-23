#include <algorithm>
#include <climits>
#include <cstring>
#include <string>

#include "Defs.h"
#include "Errors.h"
#include "Optimizer.h"
#include "Stl.h"

#define popVector(vel, el1, el2)                \
  OptimizerNode* el1 = vel.back();                  \
  vel.pop_back();                               \
  OptimizerNode* el2 = vel.back();                  \
  vel.pop_back();

#define constructNode(pushed, recycler, nodeType, newNode, params)           \
  AndList* pushed;                                                      \
  nodeType* newNode = new nodeType params;                              \
  concatList(recycler, pushed);

#define freeAll(freeList)                                        \
  for (size_t __ii = 0; __ii < freeList.size(); ++__ii) {        \
    --freeList[__ii]->pipeId; free(freeList[__ii]);  } 

#define makeAttr(newAttr, name1, type1)                 \
  newAttr.name = name1; newAttr.myType = type1;

#define indent(level) (string(3*(level), ' ') + "-> ")
#define annot(level) (string(3*(level+1), ' ') + "* ")

using std::endl;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

// // from parser
// extern FuncOperator* finalFunction;
// extern TableList* tables;
// extern AndList* boolean;
// extern NameList* groupingAtts;
// extern NameList* attsToSelect;
// extern int distinctAtts;
// extern int distinctFunc;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query


Optimizer::Optimizer(Statistics* inStats): stats(inStats), isUsedPrev(NULL), selectQeryCount(0), joinQueryCount(0) {
  constructLeafNodes(); 
  processJoins();
  ProcessSums();
  processProjects();
  makeDistinct();
  makeWrite();
  swap(boolean, isUsedPrev);
  FATALIF(isUsedPrev, "WHERE clause syntax error.");
  printNodes();
}

/**
 * 
*/
void Optimizer::printNodes(ostream& os) {
  os << "Number of selects: " << selectQeryCount << endl;
  os << "Number of joins: " << joinQueryCount << endl;
  if (groupingAtts) {
    os << "GROUPING ON ";
    for (NameList* att = groupingAtts; att; att = att->next)
      os << att->name << " ";
    os << endl;
  }
  os << "PRINTING TREE IN ORDER:\n\n";
  OptimizerNode* opNode = opRootNode;
  printNodesInOrder(opNode, os);
}

void Optimizer::printNodesInOrder(OptimizerNode* opNode, ostream& os) {
  if (opNode) {
    switch(opNode->childNodeCount) {
      case 0:
        opNode->print(os);
        break;
      
      case 1: {
        UnaryNode* temp = (UnaryNode*)opNode;
        printNodesInOrder(temp->childOpNode, os);
        opNode->print(os);
        break;
      }

      case 2: {
        BinaryNode* temp = (BinaryNode*)opNode;
        printNodesInOrder(temp->leftOpNode, os);
        printNodesInOrder(temp->rightOpNode, os);
        opNode->print(os);
        break;
      }
    }
  }
}

void OptimizerNode::print(ostream& os, size_t level) const {
  os << "************" << endl;
  printOperator(os, level);
  printPipe(os, level);
  printSchema(os, level);
  printAnnot(os, level);
  os << "\n";
}

void OptimizerNode::printOperator(ostream& os, size_t level) const {
  os << operand << " operation" << endl;
}

void OptimizerNode::printSchema(ostream& os, size_t level) const {
  os << "Output schema:" << endl;
  outSchema->print(os);
}

void LeafNode::printPipe(ostream& os, size_t level) const {
  os << "Output pipe: " << outPipeId << endl;
}

void UnaryNode::printPipe(ostream& os, size_t level) const {
  os << "Input pipe: " << inPipeId << endl;
  os << "Output pipe: " << outPipeId << endl;
}

void BinaryNode::printPipe(ostream& os, size_t level) const {
  os << "Left input pipe: " << inLeftPipeId << endl;
  os << "Right input pipe: " << inRightPipeId << endl;
  os << "Output pipe: " << outPipeId << endl;
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/

/**
 * 
*/
void Optimizer::constructLeafNodes() {
  for (TableList* table = tables; table; table = table->next) {
    stats->CopyRel(table->tableName, table->aliasAs);
    constructNode(pushed, isUsedPrev, LeafNode, newLeaf, (boolean, pushed, table->tableName, table->aliasAs, stats));
    if(newLeaf->hasCNF()) {
      OptimizerNode* selectPipeNode = new SelectPipeNode(newLeaf->selfOperand, table->tableName, table->aliasAs, newLeaf);
      selectQeryCount++;
      nodes.push_back(selectPipeNode);
    } else {
      nodes.push_back(newLeaf);
    }
  }
}

void LeafNode::printAnnot(ostream& os, size_t level) const {}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/

/**
 * Sorting and processing JOIN operations based upon the processing cost statistics. 
*/
void Optimizer::processJoins() {
  sortJoins();
  while (nodes.size()>1) {
    popVector(nodes, left, right);
    constructNode(pushed, isUsedPrev, JoinNode, newJoinNode, (boolean, pushed, left, right, stats));
    joinQueryCount++;
    nodes.push_back(newJoinNode);
  }
  opRootNode = nodes.front();
}

void Optimizer::sortJoins() {
  vector<OptimizerNode*> operands(nodes);
  sort(operands.begin(), operands.end());
  int minProcessingCost = INT_MAX, processingCost;
  do {         
    if ((processingCost = evalOrder(operands, *stats, minProcessingCost))<minProcessingCost && processingCost>0) {
      minProcessingCost = processingCost; 
      nodes = operands; 
    }
  } while (next_permutation(operands.begin(), operands.end()));
}

int Optimizer::evalOrder(vector<OptimizerNode*> operands, Statistics inStats, int bestFound) {
  vector<JoinNode*> freeNodeList;
  AndList* needsRecycling = NULL;
  while (operands.size()>1) {    
    popVector(operands, left, right);
    constructNode(pushed, needsRecycling, JoinNode, newJoinNode, (boolean, pushed, left, right, &inStats));
    operands.push_back(newJoinNode);
    freeNodeList.push_back(newJoinNode);
    if (newJoinNode->processingEstimation<=0 || newJoinNode->processingCost>bestFound) 
      break;
  }
  int processingCost = operands.back()->processingCost;
  freeAll(freeNodeList);
  concatList(boolean, needsRecycling); 
  return operands.back()->processingEstimation<0 ? -1 : processingCost;
}

void Optimizer::concatList(AndList*& left, AndList*& right) {
  if (!left) { 
    swap(left, right); 
    return;
  }
  AndList *pre = left, *cur = left->rightAnd;
  for (; cur; pre = cur, cur = cur->rightAnd);
  pre->rightAnd = right;
  right = NULL;
}

void JoinNode::printAnnot(ostream& os, size_t level) const {
  os << "CNF: " << endl;
  selOperand.Print();
}

void SumNode::printAnnot(ostream& os, size_t level) const {
  os << "Function: "; (const_cast<Function*>(&function))->Print();
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/

/**
 * Processing SUM operations based upon the processing cost statistics. 
*/
void Optimizer::ProcessSums() {
  if (groupingAtts) {
    FATALIF (!finalFunction, "Grouping without aggregation functions!");
    FATALIF (distinctAtts, "No dedup after aggregate!");
    if (distinctFunc) opRootNode = new DedupNode(opRootNode);
    opRootNode = new GroupByNode(groupingAtts, finalFunction, opRootNode);
  } else if (finalFunction) {
    opRootNode = new SumNode(finalFunction, opRootNode);
  }
}

SumNode::SumNode(FuncOperator* parseTree, OptimizerNode* opNode):
  UnaryNode("SUM", resultSchema(parseTree, opNode), opNode, NULL) {
  function.GrowFromParseTree (parseTree, *opNode->outSchema);
}

Schema* SumNode::resultSchema(FuncOperator* parseTree, OptimizerNode* opNode) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  fun.GrowFromParseTree (parseTree, *opNode->outSchema);
  return new Schema ("", 1, atts[fun.resultType()]);
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/

void Optimizer::processProjects() {
  if (attsToSelect && !finalFunction && !groupingAtts) opRootNode = new ProjectNode(attsToSelect, opRootNode);
}

void Optimizer::makeDistinct() {
  if (distinctAtts) opRootNode = new DedupNode(opRootNode);
}

void Optimizer::makeWrite() {
  opRootNode = new WriteNode(stdout, opRootNode);
}




int OptimizerNode::pipeId = 0;

OptimizerNode::OptimizerNode(const string& op, Schema* out, Statistics* inStats):
  operand(op), outSchema(out), relationCount(0), processingEstimation(0), processingCost(0), stat(inStats), outPipeId(pipeId++) {}

OptimizerNode::OptimizerNode(const string& op, Schema* out, char* rName, Statistics* inStats):
  operand(op), outSchema(out), relationCount(0), processingEstimation(0), processingCost(0), stat(inStats), outPipeId(pipeId++) {
  if (rName) relations[relationCount++] = strdup(rName);
}

OptimizerNode::OptimizerNode(const string& op, Schema* out, char* rNames[], size_t num, Statistics* inStats):
  operand(op), outSchema(out), relationCount(0), processingEstimation(0), processingCost(0), stat(inStats), outPipeId(pipeId++) {
  for (; relationCount<num; ++relationCount)
    relations[relationCount] = strdup(rNames[relationCount]);
}

OptimizerNode::~OptimizerNode() {
  delete outSchema;
  for (size_t i=0; i<relationCount; ++i)
    delete relations[i];
}

AndList* OptimizerNode::pushSelection(AndList*& alist, Schema* target) {
  AndList header; header.rightAnd = alist;  // make a list header to
  // avoid handling special cases deleting the first list element
  AndList *cur = alist, *pre = &header, *result = NULL;
  for (; cur; cur = pre->rightAnd)
    if (containedIn(cur->left, target)) {   // should push
      pre->rightAnd = cur->rightAnd;
      cur->rightAnd = result;        // *move* the node to the result list
      result = cur;        // prepend the new node to result list
    } else pre = cur;
  alist = header.rightAnd;  // special case: first element moved
  return result;
}

bool OptimizerNode::containedIn(OrList* ors, Schema* target) {
  for (; ors; ors=ors->rightOr)
    if (!containedIn(ors->left, target)) return false;
  return true;
}

bool OptimizerNode::containedIn(ComparisonOp* cmp, Schema* target) {
  Operand *left = cmp->left, *right = cmp->right;
  return (left->code!=NAME || target->Find(left->value)!=-1) &&
         (right->code!=NAME || target->Find(right->value)!=-1);
}

LeafNode::LeafNode(AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* inStats):
  OptimizerNode("SELECT FILE", new Schema(catalog_path, relName, alias), relName, inStats) {
  childNodeCount = 0;
  pushed = pushSelection(boolean, outSchema);
  processingEstimation = stat->Estimate(pushed, relations, relationCount);
  selfOperand.GrowFromParseTree(pushed, outSchema, recordLiteral);
}

UnaryNode::UnaryNode(const string& opName, Schema* out, OptimizerNode* opNode, Statistics* inStats):
  OptimizerNode (opName, out, opNode->relations, opNode->relationCount, inStats), childOpNode(opNode), inPipeId(opNode->outPipeId) {
    childNodeCount = 1;
  }

BinaryNode::BinaryNode(const string& opName, OptimizerNode* l, OptimizerNode* r, Statistics* inStats):
  OptimizerNode (opName, new Schema(*l->outSchema, *r->outSchema), inStats),
  leftOpNode(l), rightOpNode(r) {
  childNodeCount = 2;
  inLeftPipeId = leftOpNode->outPipeId;
  inRightPipeId = rightOpNode->outPipeId;
  for (size_t i=0; i<l->relationCount;)
    relations[relationCount++] = strdup(l->relations[i++]);
  for (size_t j=0; j<r->relationCount;)
    relations[relationCount++] = strdup(r->relations[j++]);
}

ProjectNode::ProjectNode(NameList* atts, OptimizerNode* opNode):
  UnaryNode("PROJECT", NULL, opNode, NULL), inputAttsCount(opNode->outSchema->GetNumAtts()), numAttsOut(0) {
  Schema* cSchema = opNode->outSchema;
  Attribute resultAtts[MAX_ATTRIBUTE_COUNT];
  FATALIF (cSchema->GetNumAtts()>MAX_ATTRIBUTE_COUNT, "Too many attributes.");
  for (; atts; atts=atts->next, numAttsOut++) {
    FATALIF ((retainedAtts[numAttsOut]=cSchema->Find(atts->name))==-1,
             "Projecting non-existing attribute.");
    makeAttr(resultAtts[numAttsOut], atts->name, cSchema->FindType(atts->name));
  }
  outSchema = new Schema ("", numAttsOut, resultAtts);
}

DedupNode::DedupNode(OptimizerNode* opNode):
  UnaryNode("DEDUPLICATION", opNode->outSchema, opNode, NULL), dedupOrder(opNode->outSchema) {}

JoinNode::JoinNode(AndList*& boolean, AndList*& pushed, OptimizerNode* l, OptimizerNode* r, Statistics* inStats):
  BinaryNode("JOIN", l, r, inStats) {
  pushed = pushSelection(boolean, outSchema);
  processingEstimation = stat->Estimate(pushed, relations, relationCount);
  processingCost = l->processingCost + processingEstimation + r->processingCost;
  selOperand.GrowFromParseTree(pushed, outSchema, recordLiteral);
}

SelectPipeNode::SelectPipeNode(CNF selfOperand, char* relation, char* alias, OptimizerNode* opNode):
  UnaryNode("SELECT PIPE", new Schema(catalog_path, relation, alias), opNode, NULL), selfOperand(selfOperand) {}

GroupByNode::GroupByNode(NameList* gAtts, FuncOperator* parseTree, OptimizerNode* opNode):
  UnaryNode("GROUP BY", resultSchema(gAtts, parseTree, opNode), opNode, NULL) {
  orderMakerGrp.growFromParseTree(gAtts, opNode->outSchema);
  function.GrowFromParseTree (parseTree, *opNode->outSchema);
}

Schema* GroupByNode::resultSchema(NameList* gAtts, FuncOperator* parseTree, OptimizerNode* opNode) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  Schema* cSchema = opNode->outSchema;
  fun.GrowFromParseTree (parseTree, *cSchema);
  Attribute resultAtts[MAX_ATTRIBUTE_COUNT];
  FATALIF (1+cSchema->GetNumAtts()>MAX_ATTRIBUTE_COUNT, "Too many attributes.");
  makeAttr(resultAtts[0], "sum", fun.resultType());
  int numAtts = 1;
  for (; gAtts; gAtts=gAtts->next, numAtts++) {
    FATALIF (cSchema->Find(gAtts->name)==-1, "Grouping by non-existing attribute.");
    makeAttr(resultAtts[numAtts], gAtts->name, cSchema->FindType(gAtts->name));
  }
  return new Schema ("", numAtts, resultAtts);
}

WriteNode::WriteNode(FILE* out, OptimizerNode* opNode):
  UnaryNode("WRITE OUT", opNode->outSchema, opNode, NULL), outFile(out) {}




void SelectPipeNode::printAnnot(ostream& os, size_t level) const {
  os << "SELECTION CNF: " << endl;
  selfOperand.Print(); 
}

void ProjectNode::printAnnot(ostream& os, size_t level) const {
  // os << keepMe[0];
  // for (size_t i=1; i<numAttsOut; ++i) os << ',' << keepMe[i];
  // os << endl;
}


void GroupByNode::printAnnot(ostream& os, size_t level) const {
  os << "OrderMaker: "; (const_cast<OrderMaker*>(&orderMakerGrp))->Print();
  os << "Function: "; (const_cast<Function*>(&function))->Print();
}

void WriteNode::printAnnot(ostream& os, size_t level) const {
  os << "Output to " << outFile << endl;
}
