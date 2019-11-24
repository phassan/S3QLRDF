/*******************************************************************
 *
 *	Tool:		SPARQL to Spark SQL Compiler
 *	version:	0.1
 *	Author: 	Mahmudul Hassan 
 *	Date: 		September 2018
 *      Last Modification Date: August 24, 2019
 *
 ********************************************************************/

#include "def.h"
#include "utility.h"
#include <string>
#include <fstream>
#include "ptp.h"
#include <iostream>


using namespace std;

extern int yyparse();
extern int yylineno;

int main(int argc, char * argv[]) {

    ifstream file(STAT_FILE);
    if (file) {
        readStatFile(file);
    } else {
        cerr << "Statistic file not found! or Error opening statistic file!" << endl;
        exit(1);
    }

    string qNo = argv[1];
    string oFile = argv[2];
    
    yyparse();
    setInternalTab();
    refine_Selection();
    findUniqueSubject();
    get_ept();
    get_rearrangedExtendedPT();
    get_join_condition();
    get_final_select_statement();
    get_final_filter_statement();
    get_individul_stm();
    string tabs = get_temp_views();
    string query = get_final_SQL_statements();

    cout << query << endl;
    string queryName = qNo.substr(0, qNo.find("."));
    ofstream outputfile;
    outputfile.open(oFile, fstream::app);
    
    for (unsigned int i = 0; i < 4; ++i) {
        if (outputfile.is_open()) {
            outputfile << "#####\n";
            outputfile << queryName + "(" + itos(i + 1) + ")";
            outputfile << "\n>>>>>\n";
            outputfile << tabs;
            outputfile << "\n>>>>>\n";
            outputfile << query;
            outputfile << "\n";
        }
    }
    outputfile.close();
    cout << "##################################################################################" << endl;

    return 0;
}
