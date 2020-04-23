#include "RelOp.h"

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    OpArgs *opArgs = new OpArgs(inFile, outPipe, selOp, literal);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *SelectFile::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp;

    while (opArgs->inFile->GetNext(temp)) {
        if (opArgs->compEng->Compare(&temp, opArgs->literal, opArgs->selOp)) {
            opArgs->outPipe->Insert(&temp);
        }
    }
    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void SelectFile::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
    n_pages = runlen;
}


void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, selOp, literal);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *SelectPipe::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp;

    while (opArgs->inPipe->Remove(&temp)) {
        if (opArgs->compEng->Compare(&temp, opArgs->literal, opArgs->selOp)) {
            opArgs->outPipe->Insert(&temp);
        }
    }
    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}


void SelectPipe::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, keepMe, numAttsInput, numAttsOutput);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *Project::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp;

    while (opArgs->inPipe->Remove(&temp)) {
        temp.Project(opArgs->keepMe, opArgs->numAttsOutput, opArgs->numAttsInput);
        opArgs->outPipe->Insert(&temp);
    }

    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Project::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    OpArgs *opArgs = new OpArgs(inPipeL, inPipeR, outPipe, selOp, literal,n_pages);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *Join::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;

    Record *tmpL = new Record();
    Record *tmpR = new Record();

    vector<Record *> leftList, rightList;
    OrderMaker orderMakerL, orderMakerR;

    if (opArgs->selOp->GetSortOrders(orderMakerL, orderMakerR)) {

        int leftAttrCount = 0, rightAttrCount = 0, totalAttrCount = 0;
        int * keepMe = NULL;
        Pipe* outPipeL= new Pipe(200);
        Pipe* outPipeR=new Pipe(200);
        BigQ bigQL = BigQ(*(opArgs->inPipeL), *outPipeL, orderMakerL, opArgs->n_pages);
        BigQ bigQR = BigQ(*(opArgs->inPipeR), *outPipeR, orderMakerR, opArgs->n_pages);
        bool leftEmpty = false, rightEmpty = false;

        if (!outPipeL->Remove(tmpL)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            leftAttrCount = ((int *) tmpL->bits)[1] / sizeof(int) - 1;
//            cout << "here .. "<<leftAttrCount<<endl;
        }

        if (!outPipeR->Remove(tmpR)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            rightAttrCount = ((int *) tmpR->bits)[1] / sizeof(int) - 1;
//            cout << "here .. "<<rightAttrCount<<endl;
        }

        totalAttrCount = leftAttrCount + rightAttrCount;
        keepMe = new int[totalAttrCount];
        for (int i = 0; i < leftAttrCount; ++i) {
            keepMe[i] = i;
        }
        for (int i = leftAttrCount; i < totalAttrCount; ++i) {
            keepMe[i] = i - leftAttrCount;
        }

        leftList.emplace_back(tmpL);
        tmpL = new Record();
        rightList.emplace_back(tmpR);
        tmpR = new Record();

        if (!outPipeL->Remove(tmpL)) {
            leftEmpty = true;
        }
        if (!outPipeR->Remove(tmpR)) {
            rightEmpty = true;
        }

        bool leftBigger = false, rightBigger = false;

        while (!leftEmpty && !rightEmpty) {

            if (!leftBigger) {
                while (!opArgs->compEng->Compare(leftList.back(), tmpL, &orderMakerL)) {
                    leftList.emplace_back(tmpL);
                    tmpL = new Record();
                    if (!outPipeL->Remove(tmpL)) {
                        leftEmpty = true;
                        break;
                    }
                }
            }

            if (!rightBigger) {
                while (!opArgs->compEng->Compare(rightList.back(), tmpR, &orderMakerR)) {
                    rightList.emplace_back(tmpR);
                    tmpR = new Record();
                    if (!outPipeR->Remove(tmpR)) {
                        rightEmpty = true;
                        break;
                    }
                }
            }

            if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) > 0) {
                flushList(rightList);
                leftBigger = true;
                rightBigger = false;
            } else if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) < 0) {
                flushList(leftList);
                leftBigger = false;
                rightBigger = true;
            } else {
                for (auto tempL : leftList) {
                    for (auto tempR : rightList) {
                        if (opArgs->compEng->Compare(tempL, tempR, opArgs->literal, opArgs->selOp)) {
                            Record mergedRec, cpRecR;
                            cpRecR.Copy(tempR);
                            mergedRec.MergeRecords(tempL, &cpRecR, leftAttrCount, rightAttrCount, keepMe, totalAttrCount,
                                                   leftAttrCount);
                            opArgs->outPipe->Insert(&mergedRec);
                        }
                    }
                }

                flushList(leftList);
                flushList(rightList);
                leftBigger = false;
                rightBigger = false;
            }

            if (!leftEmpty && !leftBigger) {
                leftList.emplace_back(tmpL);
                tmpL = new Record();
                if (!outPipeL->Remove(tmpL)) {
                    leftEmpty = true;
                }
            }
            if (!rightEmpty && !rightBigger) {
                rightList.emplace_back(tmpR);
                tmpR = new Record();
                if (!outPipeR->Remove(tmpR)) {
                    rightEmpty = true;
                }
            }
        }

        if (!leftList.empty() && !rightList.empty()) {
            while (!leftEmpty) {
                if (!leftBigger) {
                    while (!opArgs->compEng->Compare(leftList.back(), tmpL, &orderMakerL)) {
                        leftList.emplace_back(tmpL);
                        tmpL = new Record();
                        if (!outPipeL->Remove(tmpL)) {
                            leftEmpty = true;
                            break;
                        }
                    }
                }
                if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) > 0) {
                    flushList(rightList);
                    leftBigger = true;
                    rightBigger = false;
                    break;
                } else if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) < 0) {
                    flushList(leftList);
                    leftBigger = false;
                    rightBigger = true;
                } else {
                    for (auto tempL : leftList) {
                        for (auto tempR : rightList) {
                            if (opArgs->compEng->Compare(tempL, tempR, opArgs->literal, opArgs->selOp)) {
                                Record mergedRec, cpRecR;
                                cpRecR.Copy(tempR);
                                mergedRec.MergeRecords(tempL, &cpRecR, leftAttrCount, rightAttrCount, keepMe,
                                                       totalAttrCount, leftAttrCount);
                                opArgs->outPipe->Insert(&mergedRec);
                            }
                        }
                    }

                    flushList(leftList);
                    flushList(rightList);
                    leftBigger = false;
                    rightBigger = false;
                    break;
                }

                if (!leftEmpty && !leftBigger) {
                    leftList.emplace_back(tmpL);
                    tmpL = new Record();
                    if (!outPipeL->Remove(tmpL)) {
                        leftEmpty = true;
                    }
                }
            }
            while (!rightEmpty) {
                if (!rightBigger) {
                    while (!opArgs->compEng->Compare(rightList.back(), tmpR, &orderMakerR)) {
                        rightList.emplace_back(tmpR);
                        tmpR = new Record();
                        if (!outPipeR->Remove(tmpR)) {
                            rightEmpty = true;
                            break;
                        }
                    }
                }
                if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) > 0) {
                    flushList(rightList);
                    leftBigger = true;
                    rightBigger = false;
                } else if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) < 0) {
                    flushList(leftList);
                    leftBigger = false;
                    rightBigger = true;
                    break;
                } else {
                    for (auto tempL : leftList) {
                        for (auto tempR : rightList) {
                            if (opArgs->compEng->Compare(tempL, tempR, opArgs->literal, opArgs->selOp)) {
                                Record mergedRec, cpRecR;
                                cpRecR.Copy(tempR);
                                mergedRec.MergeRecords(tempL, &cpRecR, leftAttrCount, rightAttrCount, keepMe,
                                                       totalAttrCount, leftAttrCount);
                                opArgs->outPipe->Insert(&mergedRec);
                            }
                        }
                    }

                    flushList(leftList);
                    flushList(rightList);
                    leftBigger = false;
                    rightBigger = false;
                    break;
                }

                if (!rightEmpty && !rightBigger) {
                    rightList.emplace_back(tmpR);
                    tmpR = new Record();
                    if (!outPipeR->Remove(tmpR)) {
                        rightEmpty = true;
                    }
                }
            }
        }
    }
    else {
        int leftAttrCount = 0, rightAttrCount = 0, totalAttrCount = 0;
        int *keepMe = NULL;

        if (!opArgs->inPipeL->Remove(tmpL)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            leftAttrCount = *((int *) tmpL->bits);
            leftList.emplace_back(tmpL);
            tmpL = new Record();
        }

        if (!opArgs->inPipeR->Remove(tmpR)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            rightAttrCount = *((int *) tmpR->bits);
            rightList.emplace_back(tmpR);
            tmpR = new Record();
        }

        totalAttrCount = leftAttrCount + rightAttrCount;
        keepMe = new int[totalAttrCount];

        for (int i = 0; i < leftAttrCount; ++i) {
            keepMe[i] = i;
        }
        for (int i = leftAttrCount; i < totalAttrCount; ++i) {
            keepMe[i] = i - leftAttrCount;
        }

        while (opArgs->inPipeL->Remove(tmpL)) {
            leftList.emplace_back(tmpL);
            tmpL = new Record();
        }
        while (opArgs->inPipeR->Remove(tmpR)) {
            rightList.emplace_back(tmpR);
            tmpR = new Record();
        }
        for (auto tempL : leftList) {
            for (auto tempR : rightList) {
                if (opArgs->compEng->Compare(tempL, tempR, opArgs->literal, opArgs->selOp)) {
                    Record mergedRec;
                    mergedRec.MergeRecords(tmpL, tmpR, leftAttrCount, rightAttrCount, keepMe, totalAttrCount,
                                           leftAttrCount);
                    opArgs->outPipe->Insert(&mergedRec);
                }
            }
        }
    }

    flushList(leftList);
    flushList(rightList);
    delete tmpL;
    tmpL = NULL;
    delete tmpR;
    tmpR = NULL;


    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Join::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, mySchema,n_pages);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *DuplicateRemoval::workerThread(void *arg){
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp1;
    Record temp2;
    OrderMaker orderMaker = OrderMaker(opArgs->mySchema);


    Pipe *tempPipe = new Pipe(100);
    BigQ bigQ = BigQ(*(opArgs->inPipe), *tempPipe, orderMaker, opArgs->n_pages);


    if (tempPipe->Remove(&temp1)) {
        while (tempPipe->Remove(&temp2)) {
            if (opArgs->compEng->Compare(&temp1, &temp2, &orderMaker)) {
                opArgs->outPipe->Insert(&temp1);
                temp1.Consume(&temp2);
            }
        }
        opArgs->outPipe->Insert(&temp1);
    }

    tempPipe->ShutDown();
    delete tempPipe;

    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, computeMe);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

//void *Sum::workerThread(void *arg) {
//    OpArgs *opArgs = (OpArgs *) arg;
//
//    int totaltotalSumInt = 0;
//    double totaltotalSumDoub = 0.0;
//
//    int tempatIntue = 0;
//    double tempatDoubleue = 0.0;
//
//    Record tempord;
////    Function *function = function;
//    Type type;
//
//    ostringstream outResult;
//    string sumResult;
//    Record tempordResult;
//
//    //type check only for first tempord
//    if(opArgs->inPipe->Remove(&tempord))
//        type = opArgs->function->Apply(tempord, tempatIntue, tempatDoubleue);
//
//    if (type == Int) {
//        totaltotalSumInt += tempatIntue;
//        while (opArgs->inPipe->Remove(&tempord)) {
//            totaltotalSumInt += tempatIntue;
//        }
//    }else{
//        totaltotalSumDoub += tempatDoubleue;
//        while (opArgs->inPipe->Remove(&tempord)) {
//            totaltotalSumDoub += tempatDoubleue;
//        }
//    }
////    while (inPipe->Remove(&tempord)) {
////        type = function->Apply(tempord, tempatIntue, tempatDoubleue);
////        if (type == Int) {
////            totalSumInt += intAttrVal;
////        } else {
////            totalSumDoub += doubleAttrVal;
////        }
////    }
//
//    // create output tempord
//    if (type == Int) {
//        outResult << totaltotalSumInt;
//        sumResult = outResult.str();
//        sumResult.append("|");
//
//        Attribute IA = {"int", Int};
//        Schema output_schema("output_schema", 1, &IA);
//        tempordResult.ComposeRecord(&output_schema, sumResult.c_str());
//    } else {
//
//        outResult << totaltotalSumDoub;
//        sumResult = outResult.str();
//        sumResult.append("|");
//
//        Attribute DA = {"double", Double};
////        parameters.myType = Double;
//        Schema output_schema("output_schema", 1, &DA);
//        tempordResult.ComposeRecord(&output_schema, sumResult.c_str());
//    }
////    cout<<tempordResult.GetLength();
//    opArgs->outPipe->Insert(&tempordResult);
//    opArgs->outPipe->ShutDown();
//    pthread_exit(NULL);
//}
void *Sum::workerThread(void *arg)  {
    OpArgs *s = (OpArgs*)arg;
    Record temp1, temp;

    int totalSumInt = 0;
    int atInt = 0;

    Attribute parameters;
    parameters.name = "SUM";
    stringstream output;

    double totalSumDoub = 0.0;
    double atDouble = 0.0;

    if (s->function->returnsInt == 1) {
        Type dataType = Int;
        while (s->inPipe->Remove(&temp)) {
            dataType = s->function->Apply(temp, atInt, atDouble);
            totalSumInt = totalSumInt + atInt;
        }
        output << totalSumInt << "|";
    }
    else if (s->function->returnsInt == 0) {
        Type dataType = Double;
        while (s->inPipe->Remove(&temp)) {
            dataType = s->function->Apply(temp, atInt, atDouble);
            totalSumDoub = totalSumDoub + atDouble;
        }
        parameters.myType = Double;
        output << totalSumDoub << "|";
    }
    else {
        exit(1);
    }

    Schema scheme_out("out_shema", 1, &parameters);
    temp1.ComposeRecord(&scheme_out, output.str().c_str());
    s->outPipe->Insert(&temp1);

    s->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Sum::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int runlen) {
    n_pages = runlen;
//todo
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, groupAtts, computeMe,n_pages);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *GroupBy::workerThread(void *arg) {
    OpArgs* opArgs = (OpArgs*)arg;

    Record *temp = new Record;
    Record *spare = new Record;
    
    Record arrayTemp[2];
    Record *start = NULL, *end = NULL;

    Type type;
    Pipe *pipe = new Pipe(100);
    BigQ bigq(*opArgs->inPipe, *pipe, *opArgs->orderMaker, 10);

    int along = (opArgs->orderMaker->numAtts)+1;

    int whichG = 0, totalSumInt = 0, tIntRec, paramA[along];
    double totalSumDoub=0, tDoubRec;

    paramA[0] = 0;

    for(int i = 1; i < along; i++)
        paramA[i] = opArgs->orderMaker->whichAtts[i-1];

    while(pipe->Remove(&arrayTemp[whichG%2]) == 1)
    {
        start = end;
        end = &arrayTemp[whichG%2];
        if(start != NULL && end != NULL)
        {
            if(opArgs->compEng->Compare(start, end, opArgs->orderMaker) != 0)
            {
                opArgs->function->Apply(*start, tIntRec, tDoubRec);
                if(opArgs->function->returnsInt == 1)
                {
                    type = Int;
                    totalSumInt = totalSumInt + tIntRec;
                }
                else
                {
                    type = Double;
                    totalSumDoub = totalSumDoub + tDoubRec;
                }
                int startint = ((int *)start->bits)[1]/sizeof(int) - 1;

                Attribute parameters;
                stringstream output;
                if(type==Int){
                    parameters.name="int";
                    parameters.myType=Int;
                    output << totalSumInt << "|";
                }
                else{
                    parameters.name="double";
                    parameters.myType=Double;
                    output << totalSumDoub << "|";
                }
                Schema out_sch ("sum", 1, &parameters);
                spare->ComposeRecord(&out_sch,output.str().c_str());
                temp->MergeRecords(spare, start, 1, startint, paramA, along, 1);
                opArgs->outPipe->Insert(temp);
                totalSumInt = 0;
                totalSumDoub = 0;
            }
            else
            {
                opArgs->function->Apply(*start, tIntRec, tDoubRec);
                if(opArgs->function->returnsInt == 1)
                {
                    type = Int;
                    totalSumInt = totalSumInt + tIntRec;
                }
                else
                {
                    type = Double;
                    totalSumDoub = totalSumDoub + tDoubRec;
                }
            }
        }
        whichG++;
    }

    opArgs->function->Apply(*end, tIntRec, tDoubRec);
    if(opArgs->function->returnsInt == 1)
    {
        type = Int;
        totalSumInt = totalSumInt + tIntRec;
    }
    else
    {
        type = Double;
        totalSumDoub = totalSumDoub + tDoubRec;
    }
    int startint = ((int *)start->bits)[1]/sizeof(int) - 1;

    Attribute parameters;
    stringstream output;
    if(type==Int){
        parameters.name="int";
        parameters.myType=Int;
        output << totalSumInt << "|";
    }
    else{
        parameters.name="double";
        parameters.myType=Double;
        output << totalSumDoub << "|";
    }
    Schema out_sch ("sum", 1, &parameters);
    spare->ComposeRecord(&out_sch,output.str().c_str());
    temp->MergeRecords(spare, end, 1, startint, paramA, along, 1);
    opArgs->outPipe->Insert(temp);

    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void GroupBy::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    OpArgs *opArgs = new OpArgs(inPipe, outFile, mySchema);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *WriteOut::workerThread(void *arg) {
    OpArgs *opsArgs = (OpArgs *)arg;
    Record temp;

    while (opsArgs->inPipe->Remove(&temp)) {
//        opsArgs->writeOut(temp);
        int numAtts = opsArgs->schema->GetNumAtts();
        Attribute *parameters = opsArgs->schema->GetAtts();
        for (int i = 0; i < numAtts; i++) {

            int towards = ((int *)temp.bits)[i + 1];

            if (parameters[i].myType == Int) {
                int *defInt = (int *) &(temp.bits[towards]);
                fprintf(opsArgs->outPipe, "%d", *defInt);

            }
            else if (parameters[i].myType == Double) {
                double *defdoub = (double *) &(temp.bits[towards]);
                fprintf(opsArgs->outPipe, "%f", *defdoub);

            }
            else if (parameters[i].myType == String) {
                char *defStr = (char *) &(temp.bits[towards]);
                fprintf(opsArgs->outPipe, "%s", defStr);
            }
            fprintf(opsArgs->outPipe, "|");
        }

        fprintf(opsArgs->outPipe, "\n");
    }
    fclose(opsArgs->outPipe);
    opsArgs->inPipe->ShutDown();
    pthread_exit(NULL);
}


void WriteOut::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int n) {
    n_pages = n;
}
