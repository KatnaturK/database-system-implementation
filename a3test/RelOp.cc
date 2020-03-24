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
    // cout << "starting select file\n";
    inFile->MoveFirst();
    int count = 0;
    while (inFile->GetNext(*buffer, *selOp, *literal)) {
        count++;
        outPipe->Insert(buffer);
    }
    // cout << "select file count : " << count << "\n";

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
    // cout << "starting select pipe\n";
    ComparisonEngine ce;
    int count = 0;
    while (inPipe->Remove(buffer)) {
        if (ce.Compare(buffer, literal, selOp)) {
            count++;
            outPipe->Insert(buffer);
        }
    }

    // cout << "select pipe count : " << count << "\n";
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
    // cout << "starting project\n";
    int count = 0;
    while (inPipe->Remove(buffer)) {
        count++;
        buffer->Project(keepMe, numAttsOutput, numAttsInput);
        outPipe->Insert(buffer);
    }

    // cout << "project count : " << count << "\n";
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
    // cout << "starting write out\n";
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

    // cout << "writeout count : " << count << "\n";
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
    // cout << "starting duplicate removal\n";
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

    // cout << "after duplicate removal count : " << count << "\n";
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
    // cout << "starting sum\n";
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
        // cout << "after sum value : " << intsum << "\n";
    }
    else if (resultType == Double) {
        sprintf(sum_string, "%f|", doublesum);
        // cout << "after sum value : " << doublesum << "\n";
    }

    resultRec.ComposeRecord(&res_schema, sum_string);

    outPipe->Insert(&resultRec);

    outPipe->ShutDown();
    pthread_exit(NULL);
}


// ***** JOIN ******* //

void* Join::joinRoutine (void* routine) {
    ((Join *) routine)->PerformJoin ();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
    this->leftInPipe = &inPipeL;
    this->rightInPipe = &inPipeR;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    this->runLength = runLength > 0 ? runLength : 1;
    pthread_create (&joinThread, NULL, joinRoutine, (void *)this);
}
	
void Join::WaitUntilDone () {
	pthread_join (joinThread, NULL);
}

void Join::Use_n_Pages (int n) {
	this->runLength = n;
}

void* Join::PerformJoin () {
    OrderMaker leftOrderMarker, rightOrderMarker;
	this->selOp->GetSortOrders (leftOrderMarker, rightOrderMarker);

    if (leftOrderMarker.numAtts && rightOrderMarker.numAtts && leftOrderMarker.numAtts == rightOrderMarker.numAtts) {

        Pipe leftPipe(100), rightPipe(100);
		BigQ *bigQL = new BigQ (*(this->leftInPipe), leftPipe, leftOrderMarker, this->runLength);
		BigQ *bigQR = new BigQ (*(this->rightInPipe), rightPipe, rightOrderMarker, this->runLength);

        vector<Record *> leftRecordVector, rightRecordVector;
		Record *leftRecord = new Record (), *rightRecord = new Record ();
		ComparisonEngine ce;

        if (leftPipe.Remove (leftRecord) && rightPipe.Remove (rightRecord)) {

            int leftAttr = ((int *) leftRecord->bits)[1] / sizeof (int) -1, rightAttr = ((int *) rightRecord->bits)[1] / sizeof (int) -1;
			int totalAttr = leftAttr + rightAttr;
			int attrToKeep[totalAttr];
			for (int i = 0; i < leftAttr; ++i) attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; ++i) attrToKeep[i + leftAttr] = i;
			
            int  counter = 0, joinCounter;
			bool leftTuple = true, rightTuple = true;

            while (leftTuple && rightTuple) {
				
                leftTuple = false;
                rightTuple = false;
				int ceTuples = ce.Compare (leftRecord, &leftOrderMarker, rightRecord, &rightOrderMarker);

                switch (ceTuples) {
                    case 0: {
                        ++counter;
                        Record *record1 = new Record (), *record2 = new Record (); 
                        record1->Consume(leftRecord);
                        record2->Consume(rightRecord);
                        leftRecordVector.push_back (record1);
                        rightRecordVector.push_back (record2);

                        while (leftPipe.Remove (leftRecord)) {
                            if (!ce.Compare (leftRecord, record1, &leftOrderMarker)) {
                                Record *tmpRecord = new Record ();
                                tmpRecord->Consume (leftRecord);
                                leftRecordVector.push_back (tmpRecord);
                            } else {
                                leftTuple = true;
                                break;
                            }
                        }

                        while (rightPipe.Remove (rightRecord)) {
                            if (!ce.Compare (rightRecord, record2, &rightOrderMarker)) {
                                Record *tmpRecord = new Record ();
                                tmpRecord->Consume (rightRecord);
                                rightRecordVector.push_back (tmpRecord);
                            } else {
                                rightTuple = true;
                                break;
                            }
                        }

                        Record *leftSet = new Record, *rightSet = new Record, *finalSet = new Record;

                        for (vector<Record *>::iterator leftRecIt = leftRecordVector.begin (); leftRecIt != leftRecordVector.end (); leftRecIt++) {
                            leftSet->Consume (*leftRecIt);
                            for (vector<Record *>::iterator rightRecIt = rightRecordVector.begin (); rightRecIt != rightRecordVector.end (); rightRecIt++) {
                                if (ce.Compare (leftSet, *rightRecIt, this->literal, this->selOp)) {
                                    joinCounter++;
                                    rightSet->Copy (*rightRecIt);
                                    finalSet->MergeRecords (leftSet, rightSet, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
                                    this->outPipe->Insert (finalSet);
                                }
                            }
                        }

                        for (vector<Record *>::iterator it = leftRecordVector.begin (); it != leftRecordVector.end( ); it++) 
		                    if (!*it) delete *it;
                        for (vector<Record *>::iterator it = rightRecordVector.begin (); it != rightRecordVector.end( ); it++)
		                    if (!*it) delete *it;
		                leftRecordVector.clear();
		                rightRecordVector.clear();
                        break;
                    }
                    case 1:
                        leftTuple = true;
					    if (rightPipe.Remove (rightRecord)) rightTuple = true;
					    break;

                    case -1:
                        rightTuple = true;
					    if (leftPipe.Remove (leftRecord)) leftTuple = true;
					    break;

                }
            }
        }
    } else {

        int pageLength = 10;
        Record *leftRecord = new Record, *rightRecord = new Record;
        Page rightPage;
        
        DBFile leftDBFile;
        fileTypeEnum fileType = heap;
        leftDBFile.Create((char*)"tmpL", fileType, NULL);
        leftDBFile.MoveFirst();

        int leftAttr, rightAttr, totalAttr, *attrToKeep;

        if (this->leftInPipe->Remove (leftRecord) && this->rightInPipe->Remove (rightRecord)) {

            leftAttr = ((int *) leftRecord->bits)[1] / sizeof (int) -1, rightAttr = ((int *) rightRecord->bits)[1] / sizeof (int) -1;
			totalAttr = leftAttr + rightAttr;
			attrToKeep = new int[totalAttr];
			for (int i = 0; i< leftAttr; i++) attrToKeep[i] = i;
            for (int i = 0; i< rightAttr; i++) attrToKeep[i + leftAttr] = i;

            do {
                leftDBFile.Add (*leftRecord);
            }while (this->leftInPipe->Remove (leftRecord));

            vector<Record *> recordVector;
            ComparisonEngine ce;
			bool rightTuple = true;
			int joinCounter = 0;

            while (rightTuple) {

                Record *first = new Record ();
                first->Copy (rightRecord);
                rightPage.Append (rightRecord);
                recordVector.push_back (first);

                int rightPageCount = 0;
                rightTuple = false;

                while (this->rightInPipe->Remove (rightRecord)) {

                    Record *copyMe = new Record ();
					copyMe->Copy (rightRecord);

                    if (!rightPage.Append (rightRecord)) {
                        rightPageCount += 1;
                        if (rightPageCount >= pageLength -1) {
                            rightTuple = true;
                            break;
                        } else {
                            rightPage.EmptyItOut ();
                            rightPage.Append (rightRecord);
                            recordVector.push_back (copyMe);
                        }
                    } else {
                        recordVector.push_back(copyMe);
                    }
                }

                leftDBFile.MoveFirst ();
				int fileRN = 0;

                while (leftDBFile.GetNext (*leftRecord)) {
                    for (vector<Record *>::iterator it = recordVector.begin (); it!=recordVector.end (); it++) {
                        if (ce.Compare (leftRecord, *it, this->literal, this->selOp)) {
                            ++joinCounter;
							Record *jr = new Record (), *rr = new Record ();
							rr->Copy (*it);
							jr->MergeRecords (leftRecord, rr, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
							this->outPipe->Insert (jr);
                        }
                    }
                }

                for (vector<Record *>::iterator it = recordVector.begin (); it != recordVector.end( ); it++)
                    if (!*it) delete *it;
                recordVector.clear();
            }

            leftDBFile.Close();
        }
    }
    this->outPipe->ShutDown ();
}


// ***** GROUP BY ******* //

void* GroupBy::groupByRoutine (void* routine) {
    ((GroupBy *) routine)->PerformGroupBy ();
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->func = &computeMe;
    pthread_create(&thread, NULL, groupByRoutine, (void *)this);
}

void GroupBy::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
    this->runlen = runlen;
    return;
}

void* GroupBy::PerformGroupBy () {

	Pipe resultPipe(100);
	BigQ *bigQ = new BigQ (*(this->inPipe), resultPipe, *(this->groupAtts), this->runlen);
	int intRight;  double doubleRight;
	Type type;
	Attribute doubleAtt = {"double", Double};
	Attribute attr;
	attr.name = (char *)"sum";
	attr.myType = type;
	Schema *schema = new Schema((char *)"dummy", 1, &attr);

	int numAttsToKeep = this->groupAtts->numAtts + 1;
	int *attsToKeep = new int[numAttsToKeep];
	attsToKeep[0] = 0; 
	for(int i = 1; i < numAttsToKeep; i++)
		attsToKeep[i] = this->groupAtts->whichAtts[i-1];

	ComparisonEngine ce;
	Record *tmpRecord = new Record ();

	if( resultPipe.Remove (tmpRecord)) {
		bool moreRecords = true;

		while(moreRecords) {
			moreRecords = false;
			type = this->func->Apply(*tmpRecord, intRight, doubleRight);
			double sum = 0;
			sum += (intRight+doubleRight);
			Record *r = new Record ();
			Record *lastRecord = new Record;
			lastRecord->Copy (tmpRecord);
			
            while (resultPipe.Remove (r)) {
				if (!ce.Compare(lastRecord, r, this->groupAtts)){
					
                    type = this->func->Apply (*r, intRight, doubleRight);
					sum += (intRight+doubleRight);
				} else {
					
                    tmpRecord->Copy (r);
					moreRecords = true;
					break;
				}
			}

            char sum_string[20];
            sprintf(sum_string, "%f|", sum);
            Record *sumRec = new Record ();
            sumRec->ComposeRecord (schema, sum_string);

			Record *tupleRecord = new Record;
			tupleRecord->MergeRecords (sumRec, lastRecord, 1, this->groupAtts->numAtts, attsToKeep,  numAttsToKeep, 1);
			this->outPipe->Insert (tupleRecord);
		}
	}
	this->outPipe->ShutDown ();
}
