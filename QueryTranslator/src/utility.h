/*******************************************************************
*
*	Tool:		SPARQL to Spark SQL Compiler
*	version:	0.1
*	Author: 	Mahmudul Hassan 
*	Date: 		September 2018
*
********************************************************************/

#ifndef UTILITY_H
#define UTILITY_H

#include <map> 
#include <string>
#include <fstream>
#include "def.h"


using namespace std;

extern string itos(int num);
extern string dtos(double num);
extern bool replace(string& str, const string& from, const string& to);
extern char* cat_str(const char* op1, const char* op, const char* op2);
extern void replace_uri(string& result, const map<string, string>& prefixes, char* uri);
extern string format_order_by(string str);
extern string format_order_by_xsd(string s);
extern void replace_quote_dtos(string& str);
//extern char* cat2_str(const char* op1, const char* op2, const char* op3, const char* op4, const char* op5);
//extern char* cat3_str(const char* op1, const char* op2, const char* op3, const char* op4, const char* op5);
extern string sym_rmv_from_predicate(string s);
extern string rmv_caret_srt_from_value(string s);
extern string trim(const string& str);
extern string convert_date(string s);
extern string replace_with_underscore(string result);
extern void refine_Predicate(string str, int pos);
extern string replace_with_blank(string result);
extern bool elementExistInMap(map<string, string> mapPair, string element);

extern vector<string> split(const string& s, char delimiter);
extern void readStatFile(ifstream& file);
extern const bool set_contains(set<string> ck_set, string str);
extern bool mva_presents(string str);
extern string cat_str_4(string op1, string op2, string op3, string op4);
extern string replace_doublequote(string str);
extern string ltrim(const string& s);
extern string rtrim(const string& s);
extern string removeLastChar(string s);
extern string v2sCommaDelimited(vector<string> vs);
extern string v2sTabDelimited(vector<string> vs);
extern string v2sANDDelimited(vector<string> vs);
extern bool elementExistInVector(vector<string> v, string s);
extern vector<string> merge_remove_dups(vector<string> vecA, vector<string> vecB);
extern vector<string> remove_dups_vec(vector<string> vec);
extern bool existCommonElementsIn2Vectors(vector<string> vecA, vector<string> vecB);
extern void get_rearrangedExtendedPT();
extern int getIndex_vector(vector<string> v, string s);
extern vector<string> vector_intersection(vector<string> vecA, vector<string> vecB);
#endif /* UTILITY_H */

