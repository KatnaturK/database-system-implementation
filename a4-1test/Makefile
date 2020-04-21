CC = g++ -std=c++11 -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a4-1.out: Comparison.o ComparisonEngine.o Schema.o Record.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o a4-1.out Comparison.o ComparisonEngine.o Schema.o Record.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o -lfl -lpthread
	
a2test.out: Comparison.o ComparisonEngine.o Schema.o Record.o  File.o BigQ.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o a2test.o
	$(CC) -o a2test.out Comparison.o ComparisonEngine.o Schema.o Record.o File.o BigQ.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o Pipe.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o a2test.o -lfl -lpthread

gtest.out: Comparison.o ComparisonEngine.o Schema.o Record.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o gtest.o
	$(CC) -o gtest.out Comparison.o ComparisonEngine.o Schema.o Record.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o gtest.o -lfl -lpthread -lgtest

test.o: test.cc
	$(CC) -g -c test.cc

a2test.o: a2test.cc
	$(CC) -g -c a2test.cc

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc

HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
