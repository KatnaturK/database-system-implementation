#include "Statistics.h"

Statistics::Statistics() {}

Statistics::Statistics(Statistics &copyMe) {

    map<string,RelationStatistics*> *relStatsPtr = copyMe.getRelStatsMap();
    map<string,RelationStatistics*>::iterator relStatsItr;
    RelationStatistics *relStats;

    for( relStatsItr=relStatsPtr->begin(); relStatsItr!=relStatsPtr->end(); relStatsItr++) {        
        relStats = new RelationStatistics(*relStatsItr->second);
        relStatsMap[relStatsItr->first] = relStats;
    }
}

Statistics::~Statistics() {
    map<string,RelationStatistics*>::iterator relStatsItr;
    RelationStatistics *relStats = NULL;

    for(relStatsItr = relStatsMap.begin(); relStatsItr != relStatsMap.end(); relStatsItr++) {
        relStats = relStatsItr->second;
        delete relStats;
        relStats = NULL;
    }
    relStatsMap.clear();
}

void Statistics::AddRel(char *relName, int numTuples) {
    map<string,RelationStatistics*>::iterator relStatsItr;
    RelationStatistics *relStats;
    relStatsItr = relStatsMap.find(string(relName));
    
    if(relStatsItr!=relStatsMap.end()) {
        
        relStatsMap[string(relName)]->UpdateCount(numTuples);
        relStatsMap[string(relName)]->SetGroupDetails(relName,1);
    
    } else {
        relStats= new RelationStatistics(numTuples,string(relName));
        relStatsMap[string(relName)]=relStats;
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    map<string,RelationStatistics*>::iterator relStatsItr;
    relStatsItr = relStatsMap.find(string(relName));
    
    if(relStatsItr!=relStatsMap.end()) {
        relStatsMap[string(relName)]->UpdateData(string(attName),numDistincts);
    }  
}

void Statistics::CopyRel(char *oldName, char *newName) {
    string oldRelation = string(oldName);
    string newRelation = string(newName);
    if( strcmp(oldName,newName) == 0)  
        return;

    map<string,RelationStatistics*>::iterator relStatsItr1;
    map<string,RelationStatistics*>::iterator relStatsItr2;
    relStatsItr2 = relStatsMap.find(newRelation);
    
    if(relStatsItr2 != relStatsMap.end()) {
        
        delete relStatsItr2->second;
        string temp=relStatsItr2->first;
        relStatsItr2++;
        relStatsMap.erase(temp);
    }

    relStatsItr1 = relStatsMap.find(oldRelation);
    RelationStatistics *relStats;
    
    if(relStatsItr1 != relStatsMap.end()) {
        RelationStatistics* tmpRelation = new RelationStatistics( relStatsMap[string(oldName)]->GetTupleCount(), newRelation);
        relStats = relStatsMap[oldRelation];
        map<string,int>::iterator tableiter = relStats->GetRelationAttributes()->begin();
        for(;tableiter != relStats->GetRelationAttributes()->end(); tableiter++) {
            string temp = newRelation + "." + tableiter->first;
            tmpRelation->UpdateData(temp, tableiter->second);
        }
        relStatsMap[string(newName)] = tmpRelation;
    
    } else {
        cout << "\nStatistics :: CopyRel --> (Msg) Invalid relation name: "<< oldName <<endl;
        exit(1);
    }
}

void Statistics::Read(char *fromWhere) {
    
    FILE *file = NULL;
    file = fopen(fromWhere, "r");
    char readCharArr[200];    
    while(file != NULL && fscanf(file, "%s", readCharArr) != EOF) {
        if(strcmp(readCharArr, "BEGIN") == 0) {
            int rowCount = 0, groupNo = 0, distinctCount = 0;
            char relationNameArr[200], groupNameArr[200], attributeNameArr[200];;
            fscanf(file, "%s %ld %s %d", relationNameArr, &rowCount, groupNameArr, &groupNo);                   
            AddRel(relationNameArr, rowCount);
            relStatsMap[string(relationNameArr)]->SetGroupDetails(groupNameArr,groupNo);
            fscanf(file, "%s", attributeNameArr);      

            while(strcmp(attributeNameArr,"END") != 0) {
                fscanf(file, "%d", &distinctCount);
                AddAtt(relationNameArr, attributeNameArr, distinctCount);                
                fscanf(file, "%s", attributeNameArr);                                                                
            }                                                
        }                
    }
}

void Statistics::Write(char *fromWhere) {

     map<string,RelationStatistics*>::iterator relStatsItr; 
     map<string,int>::iterator mapItr;
     map<string,int> *map;

     FILE *file;
     file = fopen(fromWhere, "w");
     relStatsItr = relStatsMap.begin();
     
     for( ; relStatsItr != relStatsMap.end(); relStatsItr++) {
         fprintf(file,"\n");
         fprintf(file, "Atrribute: %s | Tuple Count: %ld | Groups: %s | Group Size: %d\n", relStatsItr->first.c_str(), relStatsItr->second->GetTupleCount(), relStatsItr->second->GetGroupName().c_str(), relStatsItr->second->GetGroupLength());
         map = relStatsItr->second->GetRelationAttributes();
         mapItr = map->begin();
         
         for( ; mapItr != map->end(); mapItr++)
            fprintf(file,"Attribute: %s | Count: %d\n", mapItr->first.c_str(), mapItr->second);
         fprintf(file,"\n");      
     }
     fclose(file);     
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
  
    double estimation = Estimate(parseTree,relNames,numToJoin);
    long result =(long)((estimation-floor(estimation)) >= 0.5 ? ceil(estimation) : floor(estimation));
    string groupNm = "" ;
    int groupCnt = numToJoin;

    for(int i=0;i<groupCnt;i++)
        groupNm = groupNm + "," + relNames[i];

    map<string,RelationStatistics*>::iterator relStatsItr = relStatsMap.begin();
    for(int i = 0; i < numToJoin; i++) {
        relStatsMap[relNames[i]]->SetGroupDetails(groupNm, groupCnt);
        relStatsMap[relNames[i]]->UpdateCount(result);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
    
    double tupleEstimation = 1;
    map<string,long> distinctValues;

    if(!GivesError(parseTree,relNames,numToJoin,distinctValues)) {
        cout<<"\nStatistics :: Estimate --> (Msg) Input Parameters invalid for Estimation";
        return -1.0;

    } else {

        string groupNm = "";
        map<string,long>::iterator tupleItr;
        map<string,long> values;
        int groupCnt = numToJoin;

        for(int i=0;i<groupCnt;i++)
            groupNm = groupNm + "," + relNames[i];

        for(int i=0;i<numToJoin;i++)
            values[relStatsMap[relNames[i]]->GetGroupName()] = relStatsMap[relNames[i]]->GetTupleCount();
        
        tupleEstimation = 1000.0;

        while(parseTree != NULL) {
            tupleEstimation*=Assess(parseTree->left,distinctValues);
            parseTree=parseTree->rightAnd;
        }

        tupleItr=values.begin();
        for(;tupleItr!=values.end();tupleItr++)
            tupleEstimation*=tupleItr->second;
    }

    tupleEstimation = tupleEstimation / 1000.0;
    return tupleEstimation;
}

double Statistics::Assess(struct OrList *orList, map<string,long> &distinctValues) {
    
    struct ComparisonOp *comparisionOp;
    map<string,double> atrributeMap;

    while(orList != NULL) {
        
        comparisionOp = orList->left;
        string key = string(comparisionOp->left->value);

        if(atrributeMap.find(key) == atrributeMap.end())
            atrributeMap[key] = 0.0;
        
        if(comparisionOp->code == 1 || comparisionOp->code == 2)
            atrributeMap[key] = atrributeMap[key] + 1.0 / 3;
        else {
            string joinKey = string(comparisionOp->left->value);            
            long maxDistinctValue = distinctValues[joinKey];
            
            if(comparisionOp->right->code == 4) {
               
               string urkey = string(comparisionOp->right->value);
               if(maxDistinctValue<distinctValues[urkey])
                   maxDistinctValue = distinctValues[urkey];
            }

            atrributeMap[key] = atrributeMap[key] + 1.0 / maxDistinctValue;
        }
        orList = orList->rightOr;
    }

    double selectionEstimate = 1.0;
    map<string,double>::iterator attributeItr = atrributeMap.begin();

    for(;attributeItr!=atrributeMap.end();attributeItr++)
        selectionEstimate*=(1.0-attributeItr->second);
    
    return (1.0-selectionEstimate);
}

bool Statistics::AtrrtibutePresent(char *value,char *relNames[], int numToJoin,map<string,long> &distinctValues) {
    int currNum = 0;    
    while(currNum < numToJoin) {
        map<string,RelationStatistics*>::iterator relStatsItr = relStatsMap.find(relNames[currNum]);    

        if(relStatsItr != relStatsMap.end()) {
            
            string key = string(value);
            if(relStatsItr->second->GetRelationAttributes()->find(key) != relStatsItr->second->GetRelationAttributes()->end()) {
                distinctValues[key] = relStatsItr->second->GetRelationAttributes()->find(key)->second;
                return true;
            }
        } else
            return false;
        currNum++;
    }
    return false;
}

bool Statistics::GivesError(struct AndList *parseTree, char *relNames[], int numToJoin,map<string,long> &distinctValues) {

    bool noError = true;
    while(parseTree != NULL && noError) {

        struct OrList *head=parseTree->left;
        while(head!=NULL && noError) {
            
            struct ComparisonOp *comparisionOp = head->left;
            if(comparisionOp->left->code == 4 && comparisionOp->code == 3 && !AtrrtibutePresent(comparisionOp->left->value, relNames, numToJoin, distinctValues)) {
                cout<<"\n"<< comparisionOp->left->value <<" Does Not Exist";
                noError = false;
            }          
            
            if(comparisionOp->right->code == 4 && comparisionOp->code == 3 && !AtrrtibutePresent(comparisionOp->right->value, relNames, numToJoin, distinctValues))
                noError = false;
            
            head = head->rightOr;
        }
        parseTree = parseTree->rightAnd;
    }

    if(!noError)
        return noError;

    map<string,int> tmpRelation;
    for(int i = 0; i < numToJoin; i++) {
        string groupNm = relStatsMap[string(relNames[i])]->GetGroupName();
        
        if(tmpRelation.find(groupNm) != tmpRelation.end())
            tmpRelation[groupNm]--;
        else
            tmpRelation[groupNm] = relStatsMap[string(relNames[i])]->GetGroupLength()-1;
    }

    map<string,int>::iterator tmpRelationItr = tmpRelation.begin();
    for( ; tmpRelationItr != tmpRelation.end(); tmpRelationItr++) {
        if(tmpRelationItr->second != 0) {
            noError = false;
            break;
        }
    }
    return noError;
}

void Statistics::LoadAllStatistics() {
    char *relName[] = {(char*)"supplier",(char*)"partsupp", (char*)"lineitem",
                (char*)"orders",(char*)"customer",(char*)"nation", (char*)"part", (char*)"region"};
    AddRel(relName[0],10000);     //supplier
    AddRel(relName[1],800000);    //partsupp
    AddRel(relName[2],6001215);   //lineitem
    AddRel(relName[3],1500000);   //orders
    AddRel(relName[4],150000);    //customer
    AddRel(relName[5],25);        //nation
    AddRel(relName[6], 200000);   //part
    AddRel(relName[7], 5);        //region
//      AddRel(relName[8], 3);        //mal_test


    AddAtt(relName[0], (char*)"s_suppkey",10000);
    AddAtt(relName[0], (char*)"s_nationkey",25);
    AddAtt(relName[0], (char*)"s_acctbal",9955);
    AddAtt(relName[0], (char*)"s_name",100000);
    AddAtt(relName[0], (char*)"s_address",100000);
    AddAtt(relName[0], (char*)"s_phone",100000);
    AddAtt(relName[0], (char*)"s_comment",10000);


    AddAtt(relName[1], (char*)"ps_suppkey", 10000);
    AddAtt(relName[1], (char*)"ps_partkey", 200000);
    AddAtt(relName[1], (char*)"ps_availqty", 9999);
    AddAtt(relName[1], (char*)"ps_supplycost", 99865);
    AddAtt(relName[1], (char*)"ps_comment", 799123);


    AddAtt(relName[2], (char*) "l_returnflag",3);
    AddAtt(relName[2], (char*)"l_discount",11);
    AddAtt(relName[2], (char*)"l_shipmode",7);
    AddAtt(relName[2], (char*)"l_orderkey",1500000);
    AddAtt(relName[2], (char*)"l_receiptdate",0);
    AddAtt(relName[2], (char*)"l_partkey",200000);
    AddAtt(relName[2], (char*)"l_suppkey",10000);
    AddAtt(relName[2], (char*)"l_linenumbe",7);
    AddAtt(relName[2], (char*)"l_quantity",50);
    AddAtt(relName[2], (char*)"l_extendedprice",933900);
    AddAtt(relName[2], (char*)"l_tax",9);
    AddAtt(relName[2], (char*)"l_linestatus",2);
    AddAtt(relName[2], (char*)"l_shipdate",2526);
    AddAtt(relName[2], (char*)"l_commitdate",2466);
    AddAtt(relName[2], (char*)"l_shipinstruct",4);
    AddAtt(relName[2], (char*)"l_comment",4501941);


    AddAtt(relName[3], (char*)"o_custkey",150000);
    AddAtt(relName[3], (char*)"o_orderkey",1500000);
    AddAtt(relName[3], (char*)"o_orderdate",2406);
    AddAtt(relName[3], (char*)"o_totalprice",1464556);
    AddAtt(relName[3], (char*)"o_orderstatus", 3);
    AddAtt(relName[3], (char*)"o_orderpriority", 5);
    AddAtt(relName[3], (char*)"o_clerk", 1000);
    AddAtt(relName[3], (char*)"o_shippriority", 1);
    AddAtt(relName[3], (char*)"o_comment", 1478684);


    AddAtt(relName[4], (char*)"c_custkey",150000);
    AddAtt(relName[4], (char*)"c_nationkey",25);
    AddAtt(relName[4], (char*)"c_mktsegment",5);
    AddAtt(relName[4], (char*)"c_name", 150000);
    AddAtt(relName[4], (char*)"c_address", 150000);
    AddAtt(relName[4], (char*)"c_phone", 150000);
    AddAtt(relName[4], (char*)"c_acctbal", 140187);
    AddAtt(relName[4], (char*)"c_comment", 149965);

    AddAtt(relName[5], (char*)"n_nationkey",25);
    AddAtt(relName[5], (char*)"n_regionkey",5);
    AddAtt(relName[5], (char*)"n_name",25);
    AddAtt(relName[5], (char*)"n_comment",25);

    AddAtt(relName[6], (char*)"p_partkey",200000);
    AddAtt(relName[6], (char*)"p_size",50);
    AddAtt(relName[6], (char*)"p_container",40);
    AddAtt(relName[6], (char*)"p_name", 199996);
    AddAtt(relName[6], (char*)"p_mfgr", 5);
    AddAtt(relName[6], (char*)"p_brand", 25);
    AddAtt(relName[6], (char*)"p_type", 150);
    AddAtt(relName[6], (char*)"p_retailprice", 20899);
    AddAtt(relName[6], (char*)"p_comment", 127459);


    AddAtt(relName[7], (char*)"r_regionkey",5);
    AddAtt(relName[7], (char*)"r_name",5);
    AddAtt(relName[7], (char*)"r_comment",5);

}