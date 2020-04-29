CC = g++ -std=gnu++11 -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

test.out: BigQ.o Comparison.o ComparisonEngine.o DBFile.o Ddl.o File.o Function.o HeapFile.o Interpreter.o QueryPlan.o Pipe.o Record.o RelOp.o Schema.o SortedFile.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out BigQ.o Comparison.o ComparisonEngine.o DBFile.o Ddl.o File.o Function.o HeapFile.o Interpreter.o QueryPlan.o Pipe.o Record.o RelOp.o Schema.o SortedFile.o Statistics.o y.tab.o lex.yy.o test.o -lfl -lpthread

gtest.out: BigQ.o Comparison.o ComparisonEngine.o DBFile.o Ddl.o File.o Function.o HeapFile.o Interpreter.o QueryPlan.o Pipe.o Record.o RelOp.o Schema.o SortedFile.o Statistics.o y.tab.o lex.yy.o gtest.o
	$(CC) -o gtest.out BigQ.o Comparison.o ComparisonEngine.o DBFile.o Ddl.o File.o Function.o HeapFile.o Interpreter.o QueryPlan.o Pipe.o Record.o RelOp.o Schema.o SortedFile.o Statistics.o y.tab.o lex.yy.o gtest.o -lfl -lpthread -lgtest

test.o: test.cc
	$(CC) -g -c test.cc

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Ddl.o: Ddl.cc
	$(CC) -g -c Ddl.cc

File.o: File.cc
	$(CC) -g -c File.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

Interpreter.o: Interpreter.cc
	$(CC) -g -c Interpreter.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc

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

