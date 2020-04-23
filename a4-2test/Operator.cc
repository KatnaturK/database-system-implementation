#include "Operator.h"

using namespace std;


string funcToString(FuncOperator *funcOperator) {
    string result;
    if (funcOperator) {
        if (funcOperator->leftOperator) {
            result.append(funcToString(funcOperator->leftOperator));
        }
        if (funcOperator->leftOperand) {
            result.append(funcOperator->leftOperand->value);
        }
        switch (funcOperator->code) {
            case 42:
                result.append("*");
                break;
            case 43:
                result.append("+");
                break;
            case 44:
                result.append("/");
                break;
            case 45:
                result.append("-");
                break;
        }
        if (funcOperator->right) {
            result.append(funcToString(funcOperator->right));
        }
    }
    return result;
}

SelectFileOperator::SelectFileOperator(AndList *selectList, Schema *schema, string relName) {
    this->opType = SELECT_FILE;
    this->outputSchema = schema;

    schema->Print();
    cnf.GrowFromParseTree(selectList, schema, literal);
}

void SelectFileOperator::print() {
    cout << endl << "Operation: Select File" << endl;
    cout << "Output pipe: " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "SELECTION CNF:" << endl;
    this->cnf.Print();
}

SelectPipeOperator::SelectPipeOperator(Operator *child, AndList *selectList) {
    this->opType = SELECT_PIPE;
    this->left = child;
    this->outputSchema = child->getSchema();
    cnf.GrowFromParseTree(selectList, outputSchema, literal);
};


void SelectPipeOperator::print() {
    cout << endl << "Operation: Select Pipe" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "SELECTION CNF:" << endl;
    this->cnf.Print();
};

ProjectOperator::ProjectOperator(Operator *child, NameList *attrsLeft) {
    this->opType = PROJECT;
    this->left = child;
    this->attsLeft = attrsLeft;
    this->outputSchema = child->getSchema()->Project(attsLeft, keepMe);
};


void ProjectOperator::print() {
    cout << endl << "Operation: Project" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Attributes to Keep:" << endl;
    NameList *tmpList = attsLeft;
    while (tmpList) {
        cout << tmpList->name << " ";
        tmpList = tmpList->next;
    }
    cout << endl;
};

JoinOperator::JoinOperator(Operator *leftChild, Operator *rightChild, AndList *joinList) {
    this->opType = JOIN;
    this->left = leftChild;
    this->right = rightChild;

    //create output schema
    int resNumAttrs = left->getSchema()->GetNumAtts() + right->getSchema()->GetNumAtts();
    Attribute *resAtts = new Attribute[resNumAttrs];

    for (int i = 0; i < left->getSchema()->GetNumAtts(); i++) {
        resAtts[i].name = left->getSchema()->GetAtts()[i].name;
        resAtts[i].myType = left->getSchema()->GetAtts()[i].myType;
    }

    for (int j = 0; j < right->getSchema()->GetNumAtts(); j++) {
        resAtts[j + left->getSchema()->GetNumAtts()].name = right->getSchema()->GetAtts()[j].name;
        resAtts[j + left->getSchema()->GetNumAtts()].myType = right->getSchema()->GetAtts()[j].myType;
    }

    outputSchema = new Schema("join", resNumAttrs, resAtts);


    cnf.GrowFromParseTree(joinList, leftChild->getSchema(), rightChild->getSchema(), literal);
};

void JoinOperator::print() {
    cout << endl << "Operation: Join" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Input Pipe " << this->right->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Outpt Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Join CNF:" << endl;
    this->cnf.Print();
};

DuplicateRemovalOperator::DuplicateRemovalOperator(Operator *child) {
    this->opType = DUPLICATE_REMOVAL;
    this->left = child;
    this->outputSchema = child->getSchema();
};

void DuplicateRemovalOperator::print() {
    cout << endl << "Operation: DuplicateRemoval" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
};

SumOperator::SumOperator(Operator *child, FuncOperator *func) {
    this->opType = SUM;
    this->left = child;
    this->funcOperator = func;
    this->function.GrowFromParseTree(func, *child->getSchema());

    //create output schema
    Attribute *atts = new Attribute[1];
    atts[0].name = "SUM";
    if (function.returnsInt == 0) {
        atts[0].myType = Double;
    } else {
        atts[0].myType = Int;
    }
    outputSchema = new Schema("SUM", 1, atts);

};

void SumOperator::print() {
    cout << endl << "Operation: Sum" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Function:" << endl;
    cout << funcToString(this->funcOperator) << endl;
}

GroupByOperator::GroupByOperator(Operator *child, NameList *groupingAtts, FuncOperator *func) {
    this->opType = GROUPBY;
    this->left = child;
    this->funcOperator = func;
    this->function.GrowFromParseTree(func, *child->getSchema());
    getOrder(groupingAtts);
    createOutputSchema();
}

void GroupByOperator::createOutputSchema() {
    Attribute *atts = new Attribute[groupOrder.numAtts + 1];
    atts[0].name = "SUM";
    stringstream output;
    if (function.returnsInt == 0) {
        atts[0].myType = Double;
    } else {
        atts[0].myType = Int;
    }
    Attribute *childAtts = left->getSchema()->GetAtts();
    for (int i = 0; i < groupOrder.numAtts; ++i) {
        atts[i + 1].name = childAtts[groupOrder.whichAtts[i]].name;
        atts[i + 1].myType = childAtts[groupOrder.whichAtts[i]].myType;
    }
    outputSchema = new Schema("group", groupOrder.numAtts + 1, atts);
}

void GroupByOperator::getOrder(NameList *groupingAtts) {
    Schema *inputSchema = left->getSchema();
    while (groupingAtts) {
        groupOrder.whichAtts[groupOrder.numAtts] = inputSchema->Find(groupingAtts->name);
        groupOrder.whichTypes[groupOrder.numAtts] = inputSchema->FindType(groupingAtts->name);
        ++groupOrder.numAtts;
        groupingAtts = groupingAtts->next;
    }
}


void GroupByOperator::print() {
    cout << endl << "Operation: GroupBy" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "OrderMaker:" << endl;
    groupOrder.Print();
    cout << endl << "Function:" << endl;
    cout << funcToString(this->funcOperator) << endl;
};


