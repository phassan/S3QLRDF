/*******************************************************************
 *
 *	Tool:		SPARQL to Spark SQL Compiler
 *	version:	0.1
 *	Author: 	Mahmudul Hassan 
 *	Date: 		September 2018
 *
 ********************************************************************/

#include <string>
#include <stack>
#include <vector>
#include <map>
#include <unordered_map>
#include <list>
#include <set>

using namespace std;

string distinct_stmt = "";
string order_by_stmt = "";
string limit_stmt = "";
string offset_stmt = "";

map<string, string> prefixes;
vector<string> project;
vector<pair<string, string> > subjects;
vector<pair<string, string> > predicates;
vector<pair<string, string> > objects;
vector<string> filters;

vector<string> f_objects;
unsigned int tabNo = 0;
map<string, string> s_tab;
map<string, vector<string> > s_to_ps;
map<string, unsigned int> s_to_bvc;
map<string, unsigned long int> table_statistics;
vector<pair<string, string> > s_internalTab;
vector<pair<string, unsigned long int> > s_internalTab_rowCount;
map<string, string> selection;
map<string, vector<string> > projection;
map<string, string> lve;
map<string, vector<string> > join_var;
map<string, vector<string> > join_variable;
vector<pair<string, string> > uSubjects;
vector<string> mva;
vector<string> project_list;
vector<pair<string, string> > join_condition;
string final_select;
string final_filter;
vector <string> individualStm;


string order_stmt;

