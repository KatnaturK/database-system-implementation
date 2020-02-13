#include <gtest/gtest.h>
#include "DBFile.h"
#include "test.h"

TEST(DBFileTest, Create_Equal) {
    DBFile dbfile;
    ASSERT_EQ(1, dbfile.Create( c->path(), heap, NULL));
    ASSERT_EQ(1, dbfile.Create( li->path(), heap, NULL));
    ASSERT_EQ(1, dbfile.Create( n->path(), heap, NULL));
    ASSERT_EQ(1, dbfile.Create( o->path(), heap, NULL));
    ASSERT_EQ(1, dbfile.Create( p->path(), heap, NULL));
    ASSERT_EQ(1, dbfile.Create( ps->path(), heap, NULL));
    ASSERT_EQ(1, dbfile.Create( r->path(), heap, NULL));
    ASSERT_EQ(1, dbfile.Create( s->path(), heap, NULL));
}

TEST(DBFileTest, Create_NotEqual) {
    DBFile dbfile;
    EXPECT_EXIT( dbfile.Create( "", heap, NULL), ::testing::ExitedWithCode(1), "BAD!  Open did not work for.*");
    ASSERT_EQ( 0, dbfile.Create( n->path(), sorted, NULL));
}

TEST(DBFileTest, Open_Equal) {
    DBFile dbfile;
    ASSERT_EQ(1, dbfile.Open( c->path()) );
    ASSERT_EQ(1, dbfile.Open( li->path()) );
    ASSERT_EQ(1, dbfile.Open( n->path()) );
    ASSERT_EQ(1, dbfile.Open( o->path()) );
    ASSERT_EQ(1, dbfile.Open( p->path()) );
    ASSERT_EQ(1, dbfile.Open( ps->path()) );
    ASSERT_EQ(1, dbfile.Open( r->path()) );
    ASSERT_EQ(1, dbfile.Open( s->path()) );
}

TEST(DBFileTest, Open_NotEqual) {
    DBFile dbfile;
    ASSERT_EQ(0, dbfile.Open("dbfiles/random_file.bin"));
}

TEST(DBFileTest, Close_Empty_File) {
    DBFile dbfile;
    dbfile.Create( n->path(), heap, NULL);
    ASSERT_EQ(0, dbfile.Close());
}

TEST(DBFileTest, Close_Nations_DBFile) {
    DBFile dbfile;
    dbfile.Create( n->path(), heap, NULL);
    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name()); 
    dbfile.Load (*(n->schema ()), tbl_path);
    ASSERT_TRUE( dbfile.Close() != 0);
}

int main(int argc, char **argv) {
    setup ();
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}