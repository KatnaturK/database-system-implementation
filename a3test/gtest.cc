#include "gtest/gtest.h"
#include "test.h"

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
	
	char *pred_s = "(s_suppkey = s_suppkey)";
	init_SF_s (pred_s, 100);
	Sum T;
	Pipe outPipe (1);
	Function func;
	char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
	get_cnf (str_sum, s->schema (), func);
	func.Print ();
	T.Use_n_Pages (1);
	SF_s.Run (dbf_s, _s, cnf_s, lit_s);
	T.Run (_s, outPipe, func);
	SF_s.WaitUntilDone ();
	T.WaitUntilDone ();
	Schema out_sch ("out_sch", 1, &DA);
	int result = clear_pipe (outPipe, &out_sch, true);
	dbf_s.Close ();
	
	ASSERT_EQ (result, 1);
}


int main (int argc, char *argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS ();
}
