/*******************************************************************
 *
 *	Tool:		SPARQL to Spark SQL Compiler
 *	version:	0.1
 *	Author: 	Mahmudul Hassan 
 *	Date: 		September 2018
 *
 ********************************************************************/

#ifndef DEF_H
#define DEF_H

#include <string>
#include <stack>
#include <vector>
#include <map>
#include <unordered_map>
#include <list>
#include <set>


using namespace std;

extern string distinct_stmt;
extern string order_by_stmt;
extern string limit_stmt;
extern string offset_stmt;

extern map<string, string> prefixes;
extern vector<string> project;
extern vector<pair<string, string> > subjects;
extern vector<pair<string, string> > predicates;
extern vector<pair<string, string> > objects;
extern vector<string> filters;

extern vector<string> f_objects;
extern int tabNo;
extern map<string, string> s_tab;
extern map<string, vector<string> > s_to_ps;
extern map<string, unsigned int> s_to_bvc;
extern map<string, unsigned long int> table_statistics;
extern vector<pair<string, string> > s_internalTab;
extern vector<pair<string, unsigned long int> > s_internalTab_rowCount;
extern map<string, string> selection;
extern map<string, vector<string> > projection;
extern map<string, string> lve;
extern map<string, vector<string> > join_var;
extern map<string, vector<string> > join_variable;
extern vector<pair<string, string> > uSubjects;
extern vector<string> mva;
extern vector<string> project_list;
extern vector<pair<string, string> > join_condition;
extern string final_select;
extern string final_filter;
extern vector <string> individualStm;


extern string order_stmt;
#endif /* DEF_H */

