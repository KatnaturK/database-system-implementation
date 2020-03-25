#include "gtest/gtest.h"
#include "test.h"

TEST (RELATIONAL_OPERATION, SELECT_GROUP_BY) {
	setup ();

	char *pred_s = "(s_suppkey = s_suppkey)";
	init_SF_s (pred_s, 100);
	char *pred_ps = "(ps_suppkey = ps_suppkey)";
	init_SF_ps (pred_ps, 100);

	Join J;
	Pipe _s_ps (pipesz);
	CNF cnf_p_ps;
	Record lit_p_ps;
	get_cnf ("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

	int outAtts = sAtts + psAtts;
	Attribute s_nationkey = {"s_nationkey", Int};
	Attribute ps_supplycost = {"ps_supplycost", Double};
	Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
	Schema join_sch ("join_sch", outAtts, joinatt);

	GroupBy G;
	Pipe _out (pipesz);
	Function func;
	char *str_sum = "(ps_supplycost)";
	get_cnf (str_sum, &join_sch, func);
	func.Print ();
	OrderMaker grp_order;
	grp_order.numAtts=1;
	int n = join_sch.GetNumAtts();
	Attribute *myAtts=join_sch.GetAtts();
	for(int i=0;i<n;i++) {
		if(i==3) {
			grp_order.whichAtts[0]=i;
			grp_order.whichTypes[0]=Int;
		}
	}	

	G.Use_n_Pages (1);
	SF_s.Run (dbf_s, _s, cnf_s, lit_s); 
	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
	J.Run (_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
	G.Run (_s_ps, _out, grp_order, func);
	SF_s.WaitUntilDone();
	SF_ps.WaitUntilDone ();
	J.WaitUntilDone ();
	G.WaitUntilDone ();

	Schema sum_sch ("sum_sch", 1, &DA);
	int result = clear_pipe (_out, &sum_sch, true);

	dbf_s.Close ();
	dbf_ps.Close ();

	ASSERT_EQ (result, 25);
}

TEST (RELATIONAL_OPERATION, SELECT_PIPE_1) {
	setup ();

	char *pred_ps = "(ps_supplycost < 1.03)";
	init_SF_ps (pred_ps, 100);
	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone ();
	int result = clear_pipe (_ps, ps->schema (), true);
	dbf_ps.Close ();
	
	ASSERT_EQ (result, 21);
}

TEST (RELATIONAL_OPERATION, SELECT_PIPE_2) {
	setup ();

	char *pred_ps = "(ps_supplycost < 1.04)";
	init_SF_ps (pred_ps, 100);
	SF_ps.Run (dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone ();
	int result = clear_pipe (_ps, ps->schema (), true);
	dbf_ps.Close ();
	
	ASSERT_EQ (result, 31);
}

TEST (RELATIONAL_OPERATION, PROJECT) {
	setup ();
	
	char *pred_p = "(p_retailprice > 931.00) AND (p_retailprice < 931.4)";
	init_SF_p (pred_p, 100);
	Project project;
	Pipe outPipe (pipesz);
	int keepMe[] = {0,1,7};
	int numAttsIn = pAtts;
	int numAttsOut = 3;
	project.Use_n_Pages (buffsz);
	SF_p.Run (dbf_p, _p, cnf_p, lit_p);
	project.Run (_p, outPipe, keepMe, numAttsIn, numAttsOut);
	SF_p.WaitUntilDone ();
	project.WaitUntilDone ();
	Attribute att3[] = {IA, SA, DA};
	Schema out_sch ("out_sch", numAttsOut, att3);
	int result = clear_pipe (outPipe, &out_sch, true);
	dbf_p.Close ();
	
	ASSERT_EQ (result, 22);
}

TEST (RELATIONAL_OPERATION, SUM_TEST) {
	setup ();

	char *pred_n = "(n_regionkey > 3)";
	init_SF_n (pred_n, 100);
	Sum T;
	Pipe _out (1);
	Function func;
	char *str_sum = "(n_regionkey)";
	get_cnf (str_sum, n->schema (), func);
	func.Print ();
	T.Use_n_Pages (1);
	SF_n.Run (dbf_n, _n, cnf_n, lit_n);
	T.Run (_n, _out, func);
	SF_n.WaitUntilDone ();
	T.WaitUntilDone ();

	Schema out_sch ("out_sch", 1, &DA);
	int result = clear_pipe (_out, &out_sch, true);

	cout << "\n\n query3 returned " << result << " records \n";

	dbf_n.Close ();
	
	ASSERT_EQ (result, 1);
}

int main (int argc, char *argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS ();
}
