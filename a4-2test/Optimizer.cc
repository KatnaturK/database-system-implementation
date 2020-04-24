#include "Optimizer.h"

#define popVector(vel, el1, el2)                \
  OptimizerNode* el1 = vel.back();                  \
  vel.pop_back();                               \
  OptimizerNode* el2 = vel.back();                  \
  vel.pop_back();

#define constructNode(pushed, recycler, nodeType, newNode, params)           \
  AndList* pushed;                                                      \
  nodeType* newNode = new nodeType params;                              \
  concatList(recycler, pushed);


Optimizer::Optimizer(Statistics* inStats): stats(inStats), isUsedPrev(NULL), selectQueryCount(0), joinQueryCount(0) {
  constructLeafNodes(); 
  processJoins();
  ProcessSums();
  processProjects();
  processDistinct();
  processWrite();
  swap(boolean, isUsedPrev);
  if (isUsedPrev) {
    cout << "ERROR !!! - WHERE clause syntax error." << endl;
		exit(-1);
	}
  printNodes();
}

/**
 * Optimizer Node constructor and functions.
*/
int OptimizerNode::pipeId = 0;

OptimizerNode::OptimizerNode(const string& inOperand, Schema* outSchema, Statistics* inStats):
  operand(inOperand), outSchema(outSchema), relationCount(0), processingEstimation(0), processingCost(0), stat(inStats), outPipeId(++pipeId) {}

OptimizerNode::OptimizerNode(const string& inOperand, Schema* outSchema, char* inRelation, Statistics* inStats):
  operand(inOperand), outSchema(outSchema), relationCount(0), processingEstimation(0), processingCost(0), stat(inStats), outPipeId(++pipeId) {
  if (inRelation) relations[relationCount++] = strdup(inRelation);
}

OptimizerNode::OptimizerNode(const string& inOperand, Schema* outSchema, char* inRelations[], size_t num, Statistics* inStats):
  operand(inOperand), outSchema(outSchema), relationCount(0), processingEstimation(0), processingCost(0), stat(inStats), outPipeId(++pipeId) {
  for (; relationCount<num; ++relationCount)
    relations[relationCount] = strdup(inRelations[relationCount]);
}

OptimizerNode::~OptimizerNode() {
  delete outSchema;
  for (size_t i=0; i<relationCount; ++i)
    delete relations[i];
}

AndList* OptimizerNode::pushSelection(AndList*& inAndList, Schema* inSchema) {
  AndList andListHeader; 
  AndList *cur = inAndList, *pre = &andListHeader, *result = NULL;
  andListHeader.rightAnd = inAndList;  
  for (; cur; cur = pre->rightAnd)
    if (containedIn(cur->left, inSchema)) { 
      pre->rightAnd = cur->rightAnd;
      cur->rightAnd = result;
      result = cur;
    } else pre = cur;
  inAndList = andListHeader.rightAnd; 
  return result;
}

bool OptimizerNode::containedIn(OrList* ors, Schema* target) {
  for (; ors; ors=ors->rightOr)
    if (!containedIn(ors->left, target)) return false;
  return true;
}

bool OptimizerNode::containedIn(ComparisonOp* ce, Schema* schema) {
  Operand *leftOperand = ce->left, *rightOperand = ce->right;
  return (leftOperand->code!=NAME || schema->Find(leftOperand->value)!=-1) &&
         (rightOperand->code!=NAME || schema->Find(rightOperand->value)!=-1);
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/

/**
 * Unary and Binary Node constructors. 
*/
UnaryNode::UnaryNode(const string& inOperand, Schema* inOutSchema, OptimizerNode* inOpNode, Statistics* inStats):
  OptimizerNode (inOperand, inOutSchema, inOpNode->relations, inOpNode->relationCount, inStats), childOpNode(inOpNode), inPipeId(inOpNode->outPipeId) {
    childNodeCount = 1;
}

void UnaryNode::printPipe(ostream& os) const {
  os << "Input pipe: " << inPipeId << endl;
  os << "Output pipe: " << outPipeId << endl;
}

BinaryNode::BinaryNode(const string& inOperand, OptimizerNode* inLeftOpNode, OptimizerNode* inRightOpNode, Statistics* inStats):
  OptimizerNode (inOperand, new Schema(*inLeftOpNode->outSchema, *inRightOpNode->outSchema), inStats),
  leftOpNode(inLeftOpNode), rightOpNode(inRightOpNode) {
  childNodeCount = 2;
  inLeftPipeId = leftOpNode->outPipeId;
  inRightPipeId = rightOpNode->outPipeId;
  for (size_t i=0; i<inLeftOpNode->relationCount;)
    relations[relationCount++] = strdup(inLeftOpNode->relations[i++]);
  for (size_t j=0; j<inRightOpNode->relationCount;)
    relations[relationCount++] = strdup(inRightOpNode->relations[j++]);
}

void BinaryNode::printPipe(ostream& os) const {
  os << "Left input pipe: " << inLeftPipeId << endl;
  os << "Right input pipe: " << inRightPipeId << endl;
  os << "Output pipe: " << outPipeId << endl;
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Leaf Node functions.
*/
LeafNode::LeafNode(AndList*& currAndList, AndList*& pushedAndList, char* inRelation, char* inAlias, Statistics* inStats):
  OptimizerNode("SELECT FILE", new Schema(catalog_path, inRelation, inAlias), inRelation, inStats) {
  childNodeCount = 0;
  pushedAndList = pushSelection(currAndList, outSchema);
  processingEstimation = stat->Estimate(pushedAndList, relations, relationCount);
  selfOperand.GrowFromParseTree(pushedAndList, outSchema, recordLiteral);
}

void Optimizer::constructLeafNodes() {
  for (TableList* table = tables; table; table = table->next) {
    stats->CopyRel(table->tableName, table->aliasAs);
    constructNode(pushed, isUsedPrev, LeafNode, newLeaf, (boolean, pushed, table->tableName, table->aliasAs, stats));
    if(newLeaf->hasCNF()) {
      OptimizerNode* selectPipeNode = new SelectPipeNode(newLeaf->selfOperand, table->tableName, table->aliasAs, newLeaf);
      selectQueryCount++;
      nodes.push_back(selectPipeNode);
    } else {
      nodes.push_back(newLeaf);
    }
  }
}

void LeafNode::printPipe(ostream& os) const {
  os << "Input Pipe: " << 0 << endl;
  os << "Output Pipe: " << outPipeId << endl;
}

void LeafNode::printAnnot(ostream& os) const {}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Processing SELECT operations based upon the processing cost statistics. 
*/
SelectPipeNode::SelectPipeNode(CNF inSelfOperand, char* inRelation, char* inAlias, OptimizerNode* inOpNode):
  UnaryNode("SELECT PIPE", new Schema(catalog_path, inRelation, inAlias), inOpNode, NULL), selfOperand(inSelfOperand) {}

void SelectPipeNode::printAnnot(ostream& os) const {
  os << "SELECTION CNF: " << endl;
  selfOperand.Print(outSchema); 
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Processing PROJECT operations based upon the processing cost statistics. 
*/
ProjectNode::ProjectNode(NameList* inAtts, OptimizerNode* inOpNode):
  UnaryNode("PROJECT", NULL, inOpNode, NULL), inputAttsCount(inOpNode->outSchema->GetNumAtts()), numAttsOut(0) {
  Schema* schema = inOpNode->outSchema;
  Attribute resultAtts[MAX_ATTRIBUTE_COUNT];
  if (schema->GetNumAtts() > MAX_ATTRIBUTE_COUNT) {
    cout << "ERROR !!! - Too many attributes." << endl;
		exit(-1);
	}
  for (; inAtts; inAtts=inAtts->next, numAttsOut++) {
    if ( (retainedAtts[numAttsOut] = schema->Find(inAtts->name)) == -1) {
      cout << "ERROR !!! - Projecting non-existing attribute." << endl;
		  exit(-1);
	  }
    resultAtts[numAttsOut].name = inAtts->name; 
    resultAtts[numAttsOut].myType = schema->FindType(inAtts->name);
  }
  outSchema = new Schema ("", numAttsOut, resultAtts);
}

void Optimizer::processProjects() {
  if (attsToSelect && !finalFunction && !groupingAtts) 
    opRootNode = new ProjectNode(attsToSelect, opRootNode);
}

void ProjectNode::printAnnot(ostream& os) const {
  // os << retainedAtts[0];
  // for (size_t i=1; i<numAttsOut; ++i) 
  //   os << ',' << retainedAtts[i];
  // os << endl;
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Sorting and processing JOIN operations based upon the processing cost statistics. 
*/
JoinNode::JoinNode(AndList*& currAndList, AndList*& pushedAndList, OptimizerNode* inLeftOpNode, OptimizerNode* inRightOpNode, Statistics* inStats):
  BinaryNode("JOIN", inLeftOpNode, inRightOpNode, inStats) {
  pushedAndList = pushSelection(currAndList, outSchema);
  processingEstimation = stat->Estimate(pushedAndList, relations, relationCount);
  processingCost = inLeftOpNode->processingCost + processingEstimation + inRightOpNode->processingCost;
  selOperand = new CNF();
  selOperand->GrowFromParseTree(pushedAndList, outSchema, recordLiteral);
}

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

  for (int i = 0; i < freeNodeList.size(); ++i) {
    --freeNodeList[i]->pipeId; 
    free(freeNodeList[i]);
  }

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

void JoinNode::printAnnot(ostream& os) const {
  os << "CNF: " << endl;
  selOperand->Print(outSchema);
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Processing SUM operations based upon the processing cost statistics. 
*/
SumNode::SumNode(FuncOperator* inParseTree, OptimizerNode* inOpNode):
  UnaryNode("SUM", resultSchema(inParseTree, inOpNode), inOpNode, NULL) {
  function.GrowFromParseTree (inParseTree, *inOpNode->outSchema);
}

Schema* SumNode::resultSchema(FuncOperator* inParseTree, OptimizerNode* inOpNode) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  fun.GrowFromParseTree (inParseTree, *inOpNode->outSchema);
  return new Schema ("", 1, atts[fun.resultType()]);
}

void Optimizer::ProcessSums() {
  if (groupingAtts) {
    if (!finalFunction) {
      cout << "ERROR !!! - Grouping without aggregation functions!" << endl;
		  exit(-1);
	  }
    else if (distinctAtts) {
      cout << "ERROR !!! - No dedup after aggregate!" << endl;
		  exit(-1);
	  }
    if (distinctFunc) opRootNode = new DedupNode(opRootNode);
      opRootNode = new GroupByNode(groupingAtts, finalFunction, opRootNode);
  } else if (finalFunction) {
    opRootNode = new SumNode(finalFunction, opRootNode);
  }
}

void SumNode::printAnnot(ostream& os) const {
  os << "FUNCTION "; (const_cast<Function*>(&function))->Print();
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Processing PROJECT operations based upon the processing cost statistics. 
*/
GroupByNode::GroupByNode(NameList* inAtts, FuncOperator* inParseTree, OptimizerNode* inOpNode):
  UnaryNode("GROUP BY", resultSchema(inAtts, inParseTree, inOpNode), inOpNode, NULL) {
  orderMakerGrp.growFromParseTree(inAtts, inOpNode->outSchema);
  function.GrowFromParseTree (inParseTree, *inOpNode->outSchema);
}

Schema* GroupByNode::resultSchema(NameList* inAtts, FuncOperator* inParseTree, OptimizerNode* inOpNode) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  Schema* schema = inOpNode->outSchema;
  fun.GrowFromParseTree (inParseTree, *schema);
  Attribute resultAtts[MAX_ATTRIBUTE_COUNT];
  if (1 + schema->GetNumAtts() > MAX_ATTRIBUTE_COUNT) {
    cout << "ERROR !!! - Too many attributes." << endl;
    exit(-1);
  }
  resultAtts[0].name = "sum"; 
  resultAtts[0].myType = fun.resultType();
  int numAtts = 1;
  for (; inAtts; inAtts=inAtts->next, numAtts++) {
    if (schema->Find(inAtts->name) == -1) {
      cout << "ERROR !!! - Grouping by non-existing attribute." << endl;
      exit(-1);
    }
    resultAtts[numAtts].name = inAtts->name; 
    resultAtts[numAtts].myType = schema->FindType(inAtts->name);
    
  }
  return new Schema ("", numAtts, resultAtts);
}

void GroupByNode::printAnnot(ostream& os) const {
  os << "OrderMaker: "; (const_cast<OrderMaker*>(&orderMakerGrp))->Print(outSchema);
  for(NameList* att = groupingAtts; att; att = att->next) {
    os << "		" << "Att " << att->name << endl;
  }
  os << "GROUPING ON " << endl;
  for(NameList* att = groupingAtts; att; att = att->next) {
    os << "		" << "Att " << att->name << endl;
  }
  os << "FUNCTION "; (const_cast<Function*>(&function))->Print();
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Processing DISTINCT operations based upon the processing cost statistics. 
*/
DedupNode::DedupNode(OptimizerNode* inOpNode):
  UnaryNode("DEDUPLICATION", inOpNode->outSchema, inOpNode, NULL), dedupOrderMaker(inOpNode->outSchema) {}

void Optimizer::processDistinct() {
  if (distinctAtts) 
    opRootNode = new DedupNode(opRootNode);
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * Writing the final resul based upon the processing cost statistics. 
*/
WriteNode::WriteNode(FILE* outFile, OptimizerNode* inOpNode):
  UnaryNode("WRITE OUT", inOpNode->outSchema, inOpNode, NULL), outFile(outFile) {}

void Optimizer::processWrite() {
  opRootNode = new WriteNode(stdout, opRootNode);
}

void WriteNode::printAnnot(ostream& os) const {
  os << "Output to " << outFile << endl;
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/


/**
 * 
*/
void Optimizer::printNodes(ostream& os) {
  os << "Number of selects: " << selectQueryCount << endl;
  os << "Number of joins: " << joinQueryCount << endl;
  if (groupingAtts) {
    os << "GROUPING ON ";
    for (NameList* att = groupingAtts; att; att = att->next)
      os << att->name << " ";
    os << endl;
    for(NameList* att = groupingAtts; att; att = att->next) {
      os << "		" << "Att " << att->name << endl;
    }
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

void OptimizerNode::print(ostream& os) const {
  os << " *********** " << endl;
  os << operand << " operation" << endl;
  printPipe(os);
  os << "Output schema:" << endl;
  outSchema->print(os);
  printAnnot(os);
  os << "\n";
}
/*** x *** x *** x *** x *** x *** x *** x *** x *** x ***/