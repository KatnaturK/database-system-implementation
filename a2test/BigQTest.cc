#include <gtest/gtest.h>
#include "BigQ.h"
#include "test.h"

OrderMaker sortOrder;

vector<Record*> produceRecords (char* fileName) {
    Record *record = new Record;
    vector<Record*> records;
	DBFile dbfile;
	dbfile.Open (fileName);
	dbfile.MoveFirst ();
	while (dbfile.GetNext (*record) == 1) {
        Record *temp = new Record;
        temp->Copy (record);
        records.push_back (temp);
	}
	dbfile.Close ();
    return records;
}

TEST(BigQTest, GenerateRuns_CustomerTbl) {
    BigQ bigQ;
    int runCount = 0, runLength = 16;
    File runFile;
    bigQ.initFile(runFile);
    map<int,Page*> overflow;
    vector<Record*> records = produceRecords (c->path ());
    ASSERT_EQ (234, bigQ.generateRuns (records, runCount, runLength, runFile, overflow));
}

TEST(BigQTest, GenerateRuns_NationTbl) {
    BigQ bigQ;
    int runCount = 0, runLength = 16;
    File runFile;
    bigQ.initFile(runFile);
    map<int,Page*> overflow;
    vector<Record*> records = produceRecords (n->path ());
    ASSERT_EQ (1, bigQ.generateRuns (records, runCount, runLength, runFile, overflow));
}

TEST(BigQTest, Phase1Compare) {
    vector<Record*> expectedRecords = produceRecords ("gtests/expected.bin");
    vector<Record*> actualRecords = produceRecords ("gtests/actual.bin");
    sort (actualRecords.begin (), actualRecords.end (), Phase1Compare (&sortOrder));
    ComparisonEngine ce;
    bool isSorted = true;
    for (int i = 0; i < expectedRecords.size (); i++) {
        if (ce.Compare (expectedRecords[i], actualRecords[i], &sortOrder) != 0) {
            isSorted = false;
            break;
        }
    }
    ASSERT_TRUE (isSorted);
}

TEST(BigQTest, Phase2Compare) {
    vector<Record*> expectedRecords = produceRecords ("gtests/expected.bin");
    vector<Record*> actualRecords = produceRecords ("gtests/actual.bin");
    sort (actualRecords.begin (), actualRecords.end (), Phase2Compare (&sortOrder));
    ComparisonEngine ce;
    bool isSorted = true;
    for (int i = 0; i < expectedRecords.size (); i++) {
        if (ce.Compare (expectedRecords[i], actualRecords[i], &sortOrder) != 0) {
            isSorted = false;
            break;
        }
    }
    ASSERT_FALSE (isSorted);
}

int main(int argc, char **argv) {
    setup ();
    r->get_sort_order (sortOrder);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}