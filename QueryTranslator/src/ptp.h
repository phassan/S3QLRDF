/*******************************************************************
 *
 *	Tool:		SPARQL to Spark SQL Compiler
 *	version:	0.1
 *	Author: 	Mahmudul Hassan 
 *	Date: 		August 24, 2019
 *  
 ********************************************************************/

#ifndef PTP_H
#define PTP_H

#include <string>
#include <iostream>
#include <unordered_set>
#include <utility>

using namespace std;

extern struct pair_hash {

    template <class T1, class T2>
    size_t operator()(std::pair<T1, T2> const &pair) const {
        size_t h1 = std::hash<T1>()(pair.first);
        size_t h2 = std::hash<T2>()(pair.second);

        return h1 ^ h2;
    }
};

extern void findUniqueSubject();
extern string getFilterTab(string s);
extern void setInternalTab();
extern void get_joinVars ();
extern void refine_Selection();
extern struct ept {
    string subject;
    string exTabName;
    string inTabName;
    unsigned int bvCount;
    unsigned long int rowCount;
    vector<string> proj;
    string seln;
    string lveStmt;
    vector<string> joinVar;
};
extern vector<ept> eptTab;
extern vector<ept> re_eptTab;
extern bool entryComparator(const ept & e1, const ept & e2);
extern void get_ept();
extern void get_rearrangedExtendedPT();
extern void get_join_condition();
extern void get_final_select_statement();
extern void get_final_filter_statement();
extern void get_individul_stm();
extern string get_temp_views();
extern string get_final_SQL_statements();
//extern string get_orderby_col(string os);
#endif /* EPT_H */

