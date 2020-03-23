#include "RelOp.h"
#include <iostream>

// ***** SELECT FILE ******* //

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inFile = &inFile;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    this->buffer = new Record;
    pthread_create(&thread, NULL, select_file_helper, (void*)this);
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
    // only one temp record being used (no pages)
    return;
}

void* SelectFile::select_file_helper (void* arg) {
    SelectFile *s = (SelectFile *)arg;
    s->select_file_function();
}

void* SelectFile::select_file_function () {
    cout << "starting select file\n";
    inFile->MoveFirst();
    int count = 0;
    while (inFile->GetNext(*buffer, *selOp, *literal)) {
        count++;
        outPipe->Insert(buffer);
    }
    cout << "select file count : " << count << "\n";

    outPipe->ShutDown();
    pthread_exit(NULL);
}

// ***** SELECT PIPE ******* //

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    this->buffer = new Record;
    pthread_create(&thread, NULL, select_pipe_helper, (void*)this);
}

void SelectPipe::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
    // only one temp record being used (no pages)
    return;
}

void* SelectPipe::select_pipe_helper (void* arg) {
    SelectPipe *s = (SelectPipe *)arg;
    s->select_pipe_function();
}

void* SelectPipe::select_pipe_function () {
    cout << "starting select pipe\n";
    ComparisonEngine ce;
    int count = 0;
    while (inPipe->Remove(buffer)) {
        if (ce.Compare(buffer, literal, selOp)) {
            count++;
            outPipe->Insert(buffer);
        }
    }

    cout << "select pipe count : " << count << "\n";
    outPipe->ShutDown();
    pthread_exit(NULL);
}

// ***** PROJECT ******* //

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->keepMe = keepMe;
    this->numAttsInput = numAttsInput;
    this->numAttsOutput = numAttsOutput;
    this->buffer = new Record;
    pthread_create(&thread, NULL, project_helper, (void*)this);
}

void Project::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int runlen) {
    // only one temp record being used (no pages)
    return;
}

void* Project::project_helper (void* arg) {
    Project *s = (Project *)arg;
    s->project_function();
}

void* Project::project_function () {
    cout << "starting project\n";
    int count = 0;
    while (inPipe->Remove(buffer)) {
        count++;
        buffer->Project(keepMe, numAttsOutput, numAttsInput);
        outPipe->Insert(buffer);
    }

    cout << "project count : " << count << "\n";
    outPipe->ShutDown();
    pthread_exit(NULL);
}

// ***** WRITE OUT ******* //

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    this->inPipe = &inPipe;
    this->outFile = outFile;
    this->mySchema = &mySchema;
    this->buffer = new Record;
    pthread_create(&thread, NULL, writeout_helper, (void*)this);
}

void WriteOut::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {
    // only one temp record being used (no pages)
    return;
}

void* WriteOut::writeout_helper (void* arg) {
    WriteOut *s = (WriteOut *)arg;
    s->writeout_function();
}

void* WriteOut::writeout_function () {
    cout << "starting write out\n";
    int n = mySchema->GetNumAtts();
    Attribute *attributes = mySchema->GetAtts();

    int count = 0;
    while (inPipe->Remove(buffer)) {
        for (int i = 0 ; i < n ; i++) {
            fprintf(outFile, "%s:[", attributes[i].name);
            int pos = ((int *) buffer->bits)[i+1];

            if (attributes[i].myType == Int) {
                int *myInt = (int *) &(buffer->bits[pos]);
                fprintf(outFile, "%d", *myInt);
            }
            else if (attributes[i].myType == Double) {
                double *myDouble = (double *) &(buffer->bits[pos]);
                fprintf(outFile, "%f", *myDouble);
            }
            else if (attributes[i].myType == String) {
                char *myString = (char *) &(buffer->bits[pos]);
                fprintf(outFile, "%s", myString);
            }
            fprintf(outFile, "%c", ']');
            if (i != n-1)
                fprintf(outFile, "%c", ',');
        }
        count++;
        fprintf(outFile, "%c", '\n');        
    }

    cout << "writeout count : " << count << "\n";
    fclose(outFile);
    pthread_exit(NULL);
}

// ***** DUPLICATE REMOVAL ******* //

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->mySchema = &mySchema;
    pthread_create(&thread, NULL, duplicate_helper, (void*)this);
}

void DuplicateRemoval::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
    this->runlen = runlen;
    return;
}

void* DuplicateRemoval::duplicate_helper (void* arg) {
    DuplicateRemoval *s = (DuplicateRemoval *)arg;
    s->duplicate_function();
}

void* DuplicateRemoval::duplicate_function () {
    cout << "starting duplicate removal\n";
    OrderMaker sortorder(mySchema);
    Pipe *sortedOutPipe = new Pipe(100);
    BigQ sortQ(*inPipe, *sortedOutPipe, sortorder, runlen);

    Record prev, curr;
    ComparisonEngine ce;
    sortedOutPipe->Remove(&prev);

    int count = 0;
    while (sortedOutPipe->Remove(&curr)) {
        if (ce.Compare(&prev, &curr, &sortorder)) {
            count++;
            outPipe->Insert(&prev);
            prev.Consume(&curr);
        }         
    }
    outPipe->Insert(&prev);
    count++;

    cout << "after duplicate removal count : " << count << "\n";
    outPipe->ShutDown();
    pthread_exit(NULL);
}

// ***** SUM ******* //

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->func = &computeMe;
    pthread_create(&thread, NULL, sum_helper, (void*)this);
}

void Sum::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void Sum::Use_n_Pages (int runlen) {
    return;
}

void* Sum::sum_helper (void* arg) {
    Sum *s = (Sum *)arg;
    s->sum_function();
}

void Sum::add_result (Type rtype, int &isum, int ires, double &dsum, double dres) {
    if (rtype == Int) {
        isum += ires;
    } else if (rtype == Double) {
        dsum += dres;
    }
}

void* Sum::sum_function () {
    cout << "starting sum\n";
    int intsum = 0, intres = 0;
    double doublesum = 0, doubleres = 0;
    Type resultType;

    Record rec, resultRec;

    while (inPipe->Remove(&rec)) {
        resultType = func->Apply(rec, intres, doubleres);
        add_result(resultType, intsum, intres, doublesum, doubleres);
    }

    Attribute attr = {(char *)"sum", resultType};
    Schema res_schema((char *)"sum_result_schema", 1, &attr);
    char sum_string[20];

    if (resultType == Int) {
        sprintf(sum_string, "%d|", intsum);
        cout << "after sum value : " << intsum << "\n";
    }
    else if (resultType == Double) {
        sprintf(sum_string, "%f|", doublesum);
        cout << "after sum value : " << doublesum << "\n";
    }

    resultRec.ComposeRecord(&res_schema, sum_string);

    outPipe->Insert(&resultRec);

    outPipe->ShutDown();
    pthread_exit(NULL);
}


// ***** JOIN ******* //

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inPipeL = &inPipeL;
    this->inPipeR = &inPipeR;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    pthread_create(&thread, NULL, joinRoutine, (void*) this);
}

void Join::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int n) {
	this->ruLength = n;
}

void* Join::joinRoutine(void* routine) {
    ((Join *) routine)->PerformJoin ();
}

void* Join::PerformJoin () {
    OrderMaker orderL, orderR;
	this->selOp->GetSortOrders (orderL, orderR);

    if (orderL.numAtts && orderR.numAtts && orderL.numAtts == orderR.numAtts) {

        Pipe pipeL(100), pipeR(100);
		BigQ *bigQL = new BigQ (*(this->inPipeL), pipeL, orderL, this->ruLength);
		BigQ *bigQR = new BigQ (*(this->inPipeR), pipeR, orderR, this->ruLength);

        vector<Record *> vectorL, vectorR;
		Record *rcdL = new Record (), *rcdR = new Record ();
		ComparisonEngine cmp;

        if (pipeL.Remove (rcdL) && pipeR.Remove (rcdR)) {

            int leftAttr = ((int *) rcdL->bits)[1] / sizeof(int) -1;
			int rightAttr = ((int *) rcdR->bits)[1] / sizeof(int) -1;
			int totalAttr = leftAttr + rightAttr;
			int attrToKeep[totalAttr];
			for (int i = 0; i < leftAttr; i++) attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; i++) attrToKeep[i + leftAttr] = i;
			
            int joinNum, num = 0;
			bool leftOK = true, rightOK = true;

            while (leftOK && rightOK) {
				
                leftOK = false;
                rightOK = false;
				int cmpRst = cmp.Compare (rcdL, &orderL, rcdR, &orderR);

                switch (cmpRst) {
                    case 0: {
                        Record *rcd1 = new Record (), *rcd2 = new Record (); 
                        rcd1->Consume(rcdL);
                        rcd2->Consume(rcdR);
                        vectorL.push_back (rcd1);
                        vectorR.push_back (rcd2);

                        while (pipeL.Remove (rcdL)) {
                            if (0 == cmp.Compare (rcdL, rcd1, &orderL)) {
                                Record *cLMe = new Record ();
                                cLMe->Consume (rcdL);
                                vectorL.push_back (cLMe);
                            } else {
                                leftOK = true;
                                break;
                            }
                        }

                        while (pipeR.Remove (rcdR)) {
                            if (0 == cmp.Compare (rcdR, rcd2, &orderR)) {
                                Record *cRMe = new Record ();
                                cRMe->Consume (rcdR);
                                vectorR.push_back (cRMe);
                            } else {
                                rightOK = true;
                                break;
                            }
                        }

                        Record *lr = new Record, *rr = new Record, *jr = new Record;

                        for (vector<Record *>::iterator itL = vectorL.begin (); itL != vectorL.end (); itL++) {
                            lr->Consume (*itL);
                            for (vector<Record *>::iterator itR = vectorR.begin (); itR != vectorR.end (); itR++) {
                                if (1 == cmp.Compare (lr, *itR, this->literal, this->selOp)) {
                                    joinNum++;
                                    rr->Copy (*itR);
                                    jr->MergeRecords (lr, rr, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
                                    this->outPipe->Insert (jr);
                                }
                            }
                        }

                        for (vector<Record *>::iterator it = vectorL.begin (); it != vectorL.end( ); it++) 
		                    if (!*it) delete *it;
                        for (vector<Record *>::iterator it = vectorR.begin (); it != vectorR.end( ); it++)
		                    if (!*it) delete *it;
		                vectorL.clear();
		                vectorR.clear();
                        break;
                    }
                    case 1:
                        leftOK = true;
					    if (pipeR.Remove (rcdR)) rightOK = true;
					    break;

                    case -1:
                        rightOK = true;
					    if (pipeL.Remove (rcdL)) leftOK = true;
					    break;

                }
            }
        }
    } else {

        int n_pages = 10;
        Record *rcdL = new Record, *rcdR = new Record;
        Page pageR;
        
        DBFile dbFileL;
        fileTypeEnum fileType = heap;
        dbFileL.Create((char*)"tmpL", fileType, NULL);
        dbFileL.MoveFirst();

        int leftAttr, rightAttr, totalAttr, *attrToKeep;

        if (this->inPipeL->Remove (rcdL) && this->inPipeR->Remove (rcdR)) {

            leftAttr = ((int *) rcdL->bits)[1] / sizeof (int) -1;
			rightAttr = ((int *) rcdR->bits)[1] / sizeof (int) -1;
			totalAttr = leftAttr + rightAttr;
			attrToKeep = new int[totalAttr];
			for (int i = 0; i< leftAttr; i++) attrToKeep[i] = i;
            for (int i = 0; i< rightAttr; i++) attrToKeep[i + leftAttr] = i;

            do {
                dbFileL.Add (*rcdL);
            }while (this->inPipeL->Remove (rcdL));

            vector<Record *> vectorR;
            ComparisonEngine cmp;
			bool rMore = true;
			int joinNum = 0;

            while (rMore) {

                Record *first = new Record ();
                first->Copy (rcdR);
                pageR.Append (rcdR);
                vectorR.push_back (first);

                int rPages = 0;
                rMore = false;

                while (this->inPipeR->Remove (rcdR)) {

                    Record *copyMe = new Record ();
					copyMe->Copy (rcdR);

                    if (!pageR.Append (rcdR)) {
                        rPages += 1;
                        if (rPages >= n_pages -1) {
                            rMore = true;
                            break;
                        } else {
                            pageR.EmptyItOut ();
                            pageR.Append (rcdR);
                            vectorR.push_back (copyMe);
                        }
                    } else {
                        vectorR.push_back(copyMe);
                    }
                }

                dbFileL.MoveFirst ();
				int fileRN = 0;

                while (dbFileL.GetNext (*rcdL)) {
                    for (vector<Record *>::iterator it = vectorR.begin (); it!=vectorR.end (); it++) {
                        if (1 == cmp.Compare (rcdL, *it, this->literal, this->selOp)) {
                            joinNum++;
							Record *jr = new Record (), *rr = new Record ();
							rr->Copy (*it);
							jr->MergeRecords (rcdL, rr, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
							this->outPipe->Insert (jr);
                        }
                    }
                }

                for (vector<Record *>::iterator it = vectorR.begin (); it != vectorR.end( ); it++)
                    if (!*it) delete *it;
                vectorR.clear();
            }

            dbFileL.Close();
        }
    }
    this->outPipe->ShutDown ();
}


// ***** GROUP BY ******* //

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->groupAtts = &groupAtts;
    this->func = &computeMe;
    pthread_create(&thread, NULL, groupby_helper, (void*)this);
}

void GroupBy::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
    this->runlen = runlen;
    return;
}

void* GroupBy::groupby_helper (void* arg) {
    GroupBy *s = (GroupBy *)arg;
    s->groupby_function();
}

void GroupBy::add_result (Type rtype, int &isum, int ires, double &dsum, double dres) {
    if (rtype == Int) {
        isum += ires;
    } else if (rtype == Double) {
        dsum += dres;
    }
}

void GroupBy::create_sum_record (Record &gAttsRec, Record &resRec, Type rtype, int isum, double dsum) {
    Attribute attr = {(char *)"sum", rtype};
    Schema res_schema((char *)"sum_result_schema", 1, &attr);
    char sum_string[20];

    if (rtype == Int) {
        sprintf(sum_string, "%d|", isum);
        cout << "after sum value : " << isum << "\n";
    }
    else if (rtype == Double) {
        sprintf(sum_string, "%f|", dsum);
        cout << "after sum value : " << dsum << "\n";
    }

    Record sumRec;
    sumRec.ComposeRecord(&res_schema, sum_string);

    int numAtts = groupAtts->numAtts + 1;
    int sumAtts[numAtts];
    sumAtts[0] = 0;
    for (int i = 1 ; i < numAtts ; i++) {
        sumAtts[i] = groupAtts->whichAtts[i-1];
    }

    resRec.MergeRecords(&sumRec, &gAttsRec, 1, numAtts-1, sumAtts, numAtts, 1);
}

void* GroupBy::groupby_function () {
    cout << "starting group by\n";

    Pipe *sortedOutPipe = new Pipe(100);
    BigQ sortQ(*inPipe, *sortedOutPipe, *groupAtts, runlen);

    int intsum = 0, intres = 0;
    double doublesum = 0, doubleres = 0;
    Type resultType;

    bool groupChanged = false;

    Record prev, curr;
    ComparisonEngine ce;

    sortedOutPipe->Remove(&prev);
    resultType = func->Apply(prev, intres, doubleres);
    add_result(resultType, intsum, intres, doublesum, doubleres);

    Record resultRec;

    while (inPipe->Remove(&curr)) {
        if (ce.Compare(&prev, &curr, groupAtts)) {
            groupChanged = true;
            create_sum_record(prev, resultRec, resultType, intsum, doublesum);
            outPipe->Insert(&resultRec);
        } 
        else {
            func->Apply(prev, intres, doubleres);
            add_result(resultType, intsum, intres, doublesum, doubleres);
        }
    }

    outPipe->ShutDown();
    pthread_exit(NULL);
}