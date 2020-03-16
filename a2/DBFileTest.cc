#include <gtest/gtest.h>
#include "DBFile.h"
#include "test.h"

int runlength = 4;
OrderMaker sortOrder;

TEST(DBFileTest, Heap_Create_Equal) {
    DBFile dbfile;
    ASSERT_EQ(1, dbfile.Create( n->path(), heap, NULL));
}

TEST(DBFileTest, Heap_Create_NotEqual) {
    DBFile dbfile;
    EXPECT_EXIT( dbfile.Create( "", heap, NULL), ::testing::ExitedWithCode(1), "BAD!  Open did not work for.*");
}

TEST(DBFileTest, Heap_Open_Equal) {
    DBFile dbfile;
    ASSERT_EQ(1, dbfile.Open( n->path()) );
}

TEST(DBFileTest, Heap_Close_Empty_File) {
    DBFile dbfile;
    dbfile.Create( n->path(), heap, NULL);
    ASSERT_EQ(1, dbfile.Close());
}

TEST(DBFileTest, Heap_Close_Nations_DBFile) {
    DBFile dbfile;
    dbfile.Create( n->path(), heap, NULL);
    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name()); 
    dbfile.Load (*(n->schema ()), tbl_path);
    ASSERT_TRUE( dbfile.Close() != 0);
}

TEST(DBFileTest, Sorted_Create_Equal) {
    DBFile dbfile;
    sortStruct startup = {&sortOrder, runlength};
    ASSERT_EQ(1, dbfile.Create( n->path(), sorted, &startup));
}

TEST(DBFileTest, Sorted_Create_NotEqual) {
    DBFile dbfile;
    sortStruct startup = {&sortOrder, runlength};
    EXPECT_EXIT( dbfile.Create( "", sorted, &startup), ::testing::ExitedWithCode(1), "BAD!  Open did not work for.*");
    ASSERT_EQ( 1, dbfile.Create( n->path(), sorted, &startup));
}

TEST(DBFileTest, Sorted_Open_Equal) {
    DBFile dbfile;
    ASSERT_EQ(1, dbfile.Open( r->path()) );
}


TEST(DBFileTest, Sorted_Close_Empty_File) {
    DBFile dbfile;
    sortStruct startup = {&sortOrder, runlength};
    dbfile.Create( n->path(), sorted, &startup);
    ASSERT_EQ(1, dbfile.Close());
}

TEST(DBFileTest, Sorted_Close_Nations_DBFile) {
    DBFile dbfile;
    sortStruct startup = {&sortOrder, runlength};
    dbfile.Create( n->path(), sorted, &startup);
    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name()); 
    dbfile.Load (*(n->schema ()), tbl_path);
    ASSERT_TRUE( dbfile.Close() != 0);
}

int main(int argc, char **argv) {
    setup ();
    n->get_sort_order (sortOrder);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}