#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "BigQ.h"
#include "Function.h"


class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	pthread_t thread;
	Record *buffer;
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;

	static void *select_file_helper (void *arg);
	void *select_file_function ();

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {

	private:
	pthread_t thread;
	Record *buffer;
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;

	static void *select_pipe_helper (void *arg);
	void *select_pipe_function ();

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class Project : public RelationalOp { 

	private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	Record *buffer;

	static void *project_helper (void *arg);
	void *project_function ();

	public:

	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};


class DuplicateRemoval : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;
	int runlen;

	static void *duplicate_helper (void *arg);
	void *duplicate_function ();

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class Sum : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};


class WriteOut : public RelationalOp {
	private:
	pthread_t thread;
	Record *buffer;
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;

	static void *writeout_helper (void *arg);
	void *writeout_function ();

	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
#endif
