#ifndef BIGQ_H
#define BIGQ_H
#pragma once


#include <algorithm>
#include <iostream>
#include <pthread.h>
#include <stdio.h>
#include <string>
#include <queue>
#include <vector>

#include "ComparisonEngine.h"
#include "DBFile.h"
#include "File.h"
#include "Pipe.h"
#include "Record.h"

class DBFile; 

using namespace std;

class BigQ {
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

	static void * bigQRoutine(void *bigQ);

private:
	DBFile * dbFile;
	File runFile;
	int runLength;
	OrderMaker &orderMarker;
	Pipe &inPipe, &outPipe;
	pthread_t bigQthread;
	string tmpFile;
	std::string rndStr (size_t length) {
		auto rndChar = [] () -> char {
			const char chrArr[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
			const size_t index = (sizeof(chrArr) - 1);
			return chrArr[ rand() % index ];
		};
		std::string str (length, 0);
		std::generate_n (str.begin(), length, rndChar);
		return str;
	}
};

#endif
