#include "gtest/gtest.h"
#include "Statistics.h"
#include "ParseTree.h"

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;

void PrintOperand(struct Operand *pOperand) {
	if(pOperand!=NULL)
		cout<<pOperand->value<<" ";
	else
		return;
}

void PrintComparisonOp(struct ComparisonOp *pCom) {
        if(pCom!=NULL) {
			PrintOperand(pCom->left);
			switch(pCom->code) {
				case 1: cout<<" < "; break;
				case 2: cout<<" > "; break;
				case 3: cout<<" = ";
			}
			PrintOperand(pCom->right);
        } else
            return;
}

void PrintOrList(struct OrList *pOr) {
        if(pOr !=NULL) {
			struct ComparisonOp *pCom = pOr->left;
			PrintComparisonOp(pCom);

			if(pOr->rightOr) {
				cout<<" OR ";
				PrintOrList(pOr->rightOr);
            }
        } else
            return;
}

void PrintAndList(struct AndList *pAnd) {
	if(pAnd !=NULL) {
		struct OrList *pOr = pAnd->left;
		PrintOrList(pOr);
		if(pAnd->rightAnd) {
			cout<<" AND ";
			PrintAndList(pAnd->rightAnd);
		}
	} else
		return;
}

Statistics s;
char *fileName = "Statistics.txt";
char *relName[] = {"lineitem"};
map<string,long> distinctValues;
	
void setup() {

	s.AddRel(relName[0],6001215);
	s.AddAtt(relName[0], "l_returnflag",3);
	s.AddAtt(relName[0], "l_discount",11);
	s.AddAtt(relName[0], "l_shipmode",7);

		
	char *cnf = "(l_returnflag = 'R') AND (l_discount < 0.04 OR l_shipmode = 'MAIL')";

	yy_scan_string(cnf);
	yyparse();
}

TEST (STATISTICS, CHECK_ATTRIBUTE_PRESENT) {
	setup();
	ASSERT_TRUE( s.GivesError(final, relName, 1, distinctValues) );
}

TEST (STATISTICS, CHECK_ERROR_FOUND) {
	setup();
	ASSERT_TRUE( s.AtrrtibutePresent(final->left->left->left->value, relName, 1, distinctValues) );
}

int main (int argc, char *argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS ();
}
