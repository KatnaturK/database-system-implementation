#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <sstream>
#include <stdlib.h>
#include <vector>

class RelationalOp {
public:
    int n_pages=10;
    // blocks the caller until the particular relational operator
    // has run to completion
    virtual void WaitUntilDone() = 0;

    // tell us how much internal memory the operation can use
    virtual void Use_n_Pages(int n) = 0;
};


class SelectFile : public RelationalOp {
private:
    pthread_t thread;

public:
    void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

    class OpArgs {
    public:
        DBFile *inFile;
        Pipe *outPipe;
        CNF *selOp;
        Record *literal;
        ComparisonEngine *compEng;

        OpArgs(DBFile &inFile1, Pipe &outPipe1, CNF &selOp1, Record &literal1) {
            literal = &literal1;
            inFile = &inFile1;
            outPipe = &outPipe1;
            selOp = &selOp1;
        }
    };

};

class SelectPipe : public RelationalOp {
private:
    pthread_t thread;

public:
    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

    class OpArgs {
    public:
        Pipe *inPipe;
        Pipe *outPipe;
        CNF *selOp;
        Record *literal;
        ComparisonEngine *compEng;

        OpArgs(Pipe &inPipe1, Pipe &outPipe1, CNF &selOp1, Record &literal1) {
            literal = &literal1;
            inPipe = &inPipe1;
            outPipe = &outPipe1;
            selOp = &selOp1;
        }
    };
};

class Project : public RelationalOp {
private:
    pthread_t thread;

public:
    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

    class OpArgs {
    public:
        Pipe *inPipe;
        Pipe *outPipe;
        int *keepMe;
        int numAttsInput;
        int numAttsOutput;

        OpArgs(Pipe &inPipe1, Pipe &outPipe1, int *keepMe1, int numAttsInput1, int numAttsOutput1) {
            inPipe = &inPipe1;
            outPipe = &outPipe1;
            keepMe = keepMe1;
            numAttsInput = numAttsInput1;
            numAttsOutput = numAttsOutput1;
        }
    };
};

class Join : public RelationalOp {
private:
    pthread_t thread;

public:
    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

    static void flushList(vector<Record *> &v) {
        for (auto r : v) {
            delete r;
            r = nullptr;
        }
        v.clear();
    }

    class OpArgs {
    public:
        Pipe *inPipeL;
        Pipe *inPipeR;
        Pipe *outPipe;
        CNF *selOp;
        Record *literal;
        ComparisonEngine *compEng;
        int n_pages;

        OpArgs(Pipe &inPipeL1, Pipe &inPipeR1, Pipe &outPipe1, CNF &selOp1, Record &literal1,int n_pages1) {
            inPipeL = &inPipeL1;
            inPipeR = &inPipeR1;
            outPipe = &outPipe1;
            selOp = &selOp1;
            literal = &literal1;
            n_pages=n_pages1;
        }
        void vectorCleanup(vector<Record *> &v);
    };
};

class DuplicateRemoval : public RelationalOp {
private:
    pthread_t thread;

public:
    int n_pages;

    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

    class OpArgs {
    public:
        Pipe *inPipe;
        Pipe *outPipe;
        Schema *mySchema;
        int n_pages;
        ComparisonEngine *compEng;

        OpArgs(Pipe &inPipe1, Pipe &outPipe1, Schema &mySchema1,int n_pages1) {
            inPipe = &inPipe1;
            outPipe = &outPipe1;
            mySchema = &mySchema1;
            n_pages=n_pages1;
        }
    };
};

class Sum : public RelationalOp {
private:
    pthread_t thread;

public:
    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

    class OpArgs {
    public:
        Pipe *inPipe;
        Pipe *outPipe;
        Function *function;
        int n_pages;

        OpArgs(Pipe &inFile1, Pipe &outPipe1, Function &function1) {
            inPipe = &inFile1;
            outPipe = &outPipe1;
            function = &function1;
        }
    };
};

class GroupBy : public RelationalOp {
private:
    pthread_t thread;
public:
    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

    class OpArgs {
    public:
        Pipe *inPipe;
        Pipe *outPipe;
        Function *function;
        OrderMaker *orderMaker;
        int n_pages;
        ComparisonEngine *compEng;

        OpArgs(Pipe &inFile1, Pipe &outPipe1, OrderMaker &groupAtts, Function &function1,int n_pages1) {
            inPipe = &inFile1;
            outPipe = &outPipe1;
            orderMaker = &groupAtts;
            function = &function1;
            n_pages=n_pages1;
        }
    };
};

class WriteOut : public RelationalOp {
private:
    pthread_t thread;

public:
    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    static void *workerThread(void *arg);

//    void writeOut(Record &rec);

    class OpArgs {
    public:
        Pipe *inPipe;
        FILE *outPipe;;
        Schema *schema;
        int n_pages;

        OpArgs(Pipe &inPipe1, FILE *outPipe1, Schema &mySchema) {
            inPipe = &inPipe1;
            outPipe = outPipe1;
            schema = &mySchema;
        }
    };

};

#endif
