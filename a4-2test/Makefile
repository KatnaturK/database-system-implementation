CC = g++ -std=c++11 -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a42.out: BigQ.o Comparison.o ComparisonEngine.o DBFile.o File.o Function.o GenericDBFile.o HeapFile.o Optimizer.o Pipe.o Record.o RelOp.o Schema.o SortedFile.o Statistics.o y.tab.o lex.yy.o a42.o
	$(CC) -o a42.out BigQ.o Comparison.o ComparisonEngine.o DBFile.o File.o Function.o GenericDBFile.o HeapFile.o Optimizer.o Pipe.o Record.o RelOp.o Schema.o SortedFile.o Statistics.o y.tab.o lex.yy.o a42.o -lfl -lpthread
	
main:   y.tab.o lex.yy.o main.o
	$(CC) -o main y.tab.o lex.yy.o main.o -lfl
	
a42.o : a42.cc
	$(CC) -g -c a42.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc

HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

Optimizer.o: Optimizer.cc
	$(CC) -g -c Optimizer.cc

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
