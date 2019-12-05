/*******************************************************************
*
*	Tool:		SPARQL to Spark SQL Compiler
*	version:	0.1
*	Author: 	Mahmudul Hassan 
*	Date: 		September 2018
*      Last Modification Date: August 24, 2019
*
********************************************************************/

%{
#include <iostream>
#include <cstdlib>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <map>
#include <string>
#include <vector>
#include <stack>
#include <utility>
#include <algorithm>
#include <iterator>
#include <list>
#include <regex>
#include "def.h"
#include "utility.h"
#include "ptp.h"


using namespace std;

int num_errors = 0;


extern int yylineno;
extern int yylex();
extern int yyparse();
extern void yyerror(const char*);

%}

%error-verbose

%union
{
    int i;
    double d;
    char *s;
}

%token<d> FLOAT_NUM;
%token<i> INT_NUM;
%token<s> PREFIX AT_PREFIX BASE;
%token<s> IDT VAR URI CSTR INT SURI VALUE NOT DATE URL;
%token<s> SELECT DISTINCT WHERE FILTER LIMIT BOUND SYM OPTIONAL UNION NOT_BOUND OFFSET;
%token<s> LP RP LB RB COLON SEMICOLON DOT REGEX COMMA;
%token<s> AND OR EQU NEQ LT LTE GT GTE;
%token<s> ORDER_BY ASC DESC ORDER_BY_DESC ORDER_BY_XSD LANGMATCHES LANG;
%type<s> primary unary_expr or_expr and_expr equality_expr relational_expr expr;

%%

query:
	body
	| decl body
	;

decl:
	prefix
	| decl prefix
	;

prefix:
        BASE URI line_end
	{
            string s("");
            prefixes[$<s>2] = s;
	}
	|prefix_keyword SYM URI line_end
	{
            prefixes[$<s>3] = $<s>2;
	};

prefix_keyword:
	PREFIX
	| AT_PREFIX;

line_end:
	| DOT;

body:
	SELECT distinct select WHERE LB stmts RB order_by offset limit;

distinct:
	| DISTINCT
	{
            distinct_stmt = "DISTINCT ";
	}
	;

select:
	VAR
	{
            project.push_back($<s>1);
	}
	| select VAR
	{
            project.push_back($<s>2);
	}
	| select COMMA VAR
	{
            project.push_back($<s>3);
	};
        
stmts:
        
	| stmts stmt
        | stmts UNION stmts RB
        {
            cerr << "This version of query translator does not support code generation for unions." << endl;
        }
        | stmts OPTIONAL stmts RB
        {
            cerr << "This version of query translator does not support code generation for optionals." << endl;
        };

stmt:
	subject predicate object line_end
	{
            string subject = subjects.at(subjects.size() - 1).first;
            map<string, vector<string> >::iterator it = s_to_ps.find(subject);
            if (it != s_to_ps.end()) {
                string key = it->first;
                vector<string> value = it->second;
                string p = replace_with_underscore(predicates.at(predicates.size() - 1).first);
                value.push_back(p);
                s_to_ps[key] = value;
                if(mva_presents(p)){
                    map<string, string>::iterator itp;
                    itp = lve.find(key);
                        if(itp != lve.end()) {
                            string val = itp->second + "\n" +"LATERAL VIEW EXPLODE(" + p + ") EXPLODED_NAMES AS " + p + "_lve";
                            lve[key] = val;
                        }
                        else {
                            lve[key] = "LATERAL VIEW EXPLODE(" + p + ") EXPLODED_NAMES AS " + p + "_lve";
                        }
                }
            } else {
                string key = subjects.at(subjects.size() - 1).first;
                vector<string> value;
                string p = replace_with_underscore(predicates.at(predicates.size() - 1).first);
                value.push_back(p);
                s_to_ps[key] = value;
                if(mva_presents(p)){
                    lve[key] = "LATERAL VIEW EXPLODE(" + p + ") EXPLODED_NAMES AS " + p + "_lve";
                }
            }

            string ck_subject_var = subjects.at(subjects.size() - 1).second;
            string ck_object_var = objects.at(subjects.size() - 1).second;

            if (ck_subject_var != "var" || ck_object_var != "var") {
                map<string, unsigned int>::iterator it2 = s_to_bvc.find(subject);
                if (it2 != s_to_bvc.end()) {
                    string key = it2->first;

                    if (ck_object_var != "var") {
                        unsigned int value = it2->second;
                        value = value + 1;
                        s_to_bvc[key] = value;
                    }
                } else {
                    string key = subject;
                    unsigned int value = 1;
                    if (ck_subject_var != "var") {
                        s_to_bvc[key] = value;
                    }
                    if (ck_object_var != "var") {
                        s_to_bvc[key] = value;
                    }
                }
            } else {
                map<string, unsigned int>::iterator it3 = s_to_bvc.find(subject);
                if (it3 == s_to_bvc.end()){
                    s_to_bvc[subject] = 0;
                } else {
                    ;
                }
            }

//  projection
           map<string, vector<string> >::iterator it3 = projection.find(subject);
            if (it3 != projection.end()) {
                string key = it3->first;
                vector<string> value = it3->second;
                if(objects.at(objects.size() - 1).second == "var"){
                    string p = replace_with_underscore(predicates.at(predicates.size() - 1).first);
                    if(mva_presents(p)){
                        string str = p + "_lve";
                        value.push_back(str);
                        projection[key] = value;
                    } else {
                        value.push_back(p);
                        projection[key] = value;
                    }
                }

            } else {
                string key = subjects.at(subjects.size() - 1).first;
                if(subjects.at(subjects.size() - 1).second == "var") {
                    vector<string> value;
                    value.push_back("s");
                    projection[key] = value;                    
                } else {
                    ;
                } 
                if(objects.at(objects.size() - 1).second == "var"){
                    map<string, vector<string>>::iterator itop;
                    itop = projection.find(key);
                    string p = replace_with_underscore(predicates.at(predicates.size() - 1).first); 
                        if(itop != projection.end()) {
                            vector<string> value = itop->second;                           
                            if(mva_presents(p)){
                                string str = p + "_lve";
                                value.push_back(str);
                                projection[key] = value;
                            } else {
                                value.push_back(p);
                                projection[key] = value;
                            }
                        } else {
                            vector<string> value;
                            if(mva_presents(p)){
                                string str = p + "_lve";
                                value.push_back(str);
                                projection[key] = value;
                            } else {
                                value.push_back(p);
                                projection[key] = value;
                            } 
                        }
                } else {
                    ;
                }
            }            
// selection
            map<string, string>::iterator it4 = selection.find(subject);
            if (it4 != selection.end()) {
                string key = it4->first;
                string value = it4->second;
                string p = replace_with_underscore(predicates.at(predicates.size() - 1).first);
                if(objects.at(objects.size() - 1).second == "var"){
                    if(mva_presents(p)){
                        selection[key] = " " + value + " AND " + p + "_lve IS NOT NULL";
                    } else {
                        selection[key] =  " " + value + " AND " + p + " IS NOT NULL";
                    }
                } else {
                    if(mva_presents(p)){
                        selection[key] = " " + value + " AND " + p + "_lve = " + "'" + (objects.at(objects.size() - 1).first) + "'";
                    } else {
                        selection[key] = " " + value + " AND " + p + " = " + "'" + (objects.at(objects.size() - 1).first) + "'";
                    }                    
                }
            } else {
                string key = subjects.at(subjects.size() - 1).first;
                if(subjects.at(subjects.size() - 1).second == "var") {
                 //   selection[key] = "s IS NOT NULL";                    
                } else {
                    selection[key] = cat_str_4("s = ", "'", (subjects.at(subjects.size() - 1).first), "'");
                } 
                if(objects.at(objects.size() - 1).second == "var"){
                    map<string, string>::iterator itos;
                    itos = selection.find(key);
                    string p = replace_with_underscore(predicates.at(predicates.size() - 1).first); 
                    if(itos != selection.end()) {
                        string value = itos->second;                           
                        if(mva_presents(p)){
                            selection[key] = value + " AND " + p + "_lve IS NOT NULL";
                        } else {
                            selection[key] = value + " AND " + p + " IS NOT NULL";
                        }
                    } else {
                        if(mva_presents(p)){
                            selection[key] = p + "_lve IS NOT NULL";
                        } else {
                            selection[key] = p + " IS NOT NULL";
                        } 
                    }
                } else {
                    map<string, string>::iterator itos2;
                    itos2 = selection.find(key);
                    string p = replace_with_underscore(predicates.at(predicates.size() - 1).first); 
                    if(itos2 != selection.end()) {
                        string value = itos2->second;                           
                        if(mva_presents(p)){
                            selection[key] = value + " AND " + p + "_lve = " + "'" + (objects.at(objects.size() - 1).first) + "'";
                        } else {
                            selection[key] = value + " AND " + p + " = " +  "'" + (objects.at(objects.size() - 1).first) + "'";
                        }
                    } else {
                        if(mva_presents(p)){
                            selection[key] = p + "_lve = " + "'" + (objects.at(objects.size() - 1).first) + "'";
                        } else {
                            selection[key] = p + " = " + "'" + (objects.at(objects.size() - 1).first) + "'";
                        } 
                    }
                }
            }            

//  Join variable
           map<string, vector<string> >::iterator jit = join_variable.find(subject);
            if (jit != join_variable.end()) {
                string key = jit->first;
                vector<string> value = jit->second;
                if(objects.at(objects.size() - 1).second == "var"){
                    value.push_back(objects.at(objects.size() - 1).first);
                    join_variable[key] = value;                  
                }
            } else {
                string key = subjects.at(subjects.size() - 1).first;
                if(subjects.at(subjects.size() - 1).second == "var") {
                    vector<string> value;
                    value.push_back(subjects.at(subjects.size() - 1).first);
                    join_variable[key] = value;                    
                } else {
                    ;
                } 
                if(objects.at(objects.size() - 1).second == "var"){
                    map<string, vector<string>>::iterator joit;
                    joit = join_variable.find(key);
                    if(joit != join_variable.end()) {
                        vector<string> value = joit->second;                           
                        value.push_back(objects.at(objects.size() - 1).first);
                        join_variable[key] = value;
                    } else {
                        vector<string> value;
                        value.push_back(objects.at(objects.size() - 1).first);
                        join_variable[key] = value;
                    }
                } else {
                    ;
                }
            }
        }
	| FILTER LP expr RP line_end
	{
           string e($<s>3);
           filters.push_back(e);
	}
        | FILTER REGEX LP expr RP line_end
        {
            string e($<s>4);
            replace_quote_dtos(e);
            filters.push_back(e);
        }
        | FILTER LANGMATCHES LP expr RP line_end
        {
            string e($<s>4);
            replace_quote_dtos(e);
            filters.push_back(e);
        };

subject:
	VAR
	{
            subjects.push_back(make_pair($<s>1, "var"));
            string s($<s>1);
            if(!elementExistInMap(s_tab, $<s>1)) {
                tabNo = tabNo + 1;
                s_tab[$<s>1] = "Table_" + itos(tabNo);
            } else {
                ;
            }

	}
	| URI
	{
            subjects.push_back(make_pair($<s>1, "uri"));
            if(!elementExistInMap(s_tab, $<s>1)) {
                tabNo = tabNo + 1;
                s_tab[$<s>1] = "Table_" + itos(tabNo);
            } else {
                ;
            }           
	}
        | URL
        {
            string s($<s>1);
            subjects.push_back(make_pair(s, "url"));
            if(!elementExistInMap(s_tab, s)) {
                tabNo = tabNo + 1;
                s_tab[s] = "Table_" + itos(tabNo);
            } else {
                ;
            }
        }
	| SURI
	{
            string s($<s>1);
            string s2 = "<" + s + ">";
            subjects.push_back(make_pair(s2, "suri"));
            if(!elementExistInMap(s_tab, s2)) {
                tabNo = tabNo + 1;
                s_tab[s2] = "Table_" + itos(tabNo);
            } else {
                ;
            }
	};

predicate:
	VAR
	{
            predicates.push_back(make_pair($<s>1, "var"));
	}
	| URI
	{
            string result = "";
            char* uri = $<s>1;
            replace_uri(result, prefixes,  uri);
            predicates.push_back(make_pair(result, "uri"));
	}	
	| SURI
	{
            predicates.push_back(make_pair($<s>1, "suri"));
	} 
        | IDT 
        {
            predicates.push_back(make_pair($<s>1, "idt"));
        };

object:
	VAR
	{
            objects.push_back(make_pair($<s>1, "var"));
            f_objects.push_back($<s>1);
	}
	| URI
	{
//            string result = "";
//            char* uri = $<s>1;
//            replace_uri(result, prefixes, uri);
//
//            if(!result.empty()) {
//                 objects.push_back(make_pair(result, "uri"));
//                 f_objects.push_back(result);
//            } else {
//                string s($<s>1);
//                regex reg("[<|>]");
//                s = regex_replace(s, reg, "");
//                objects.push_back(make_pair(s, "uri"));
//                f_objects.push_back(result);
//            }
                
                string s($<s>1);
                objects.push_back(make_pair(s, "uri"));
                f_objects.push_back(s);                
	}
        | URL
        {
            string s($<s>1);
            objects.push_back(make_pair(s, "url"));
            f_objects.push_back(s);
        }
	| CSTR
	{
            string s($<s>1);
            replace_with_blank(s);
            objects.push_back(make_pair(s, "val"));
            f_objects.push_back(s);
	}
	| VALUE
	{
            string s($<s>1);
            s = rmv_caret_srt_from_value(s);
            objects.push_back(make_pair(s, "val"));
            f_objects.push_back(s);
	}
	| SURI
	{
            string s($<s>1);
            string s2 = "<" + s + ">";
            objects.push_back(make_pair(s2, "suri"));
            f_objects.push_back(s2);
	};

expr:
	or_expr
	;
//expr:
//	or_expr
//        | expr COMMA or_expr
//        {
//            $$ = cat2_str("LIKE (", $<s>1, " , " , $<s>3, ")");
//        }
//        | LANG LP expr RP COMMA or_expr 
//        {
//            $$ = cat3_str("LIKE (", $<s>3, " , " , $<s>6, ")");
//        };

or_expr:
	and_expr
	| or_expr OR and_expr
	{
            $$ = cat_str($<s>1, " || ", $<s>3);
	}
	;

and_expr:
	equality_expr
	| and_expr AND equality_expr
	{
            //$$ = cat_str($<s>1, " && ", $<s>3);
            $$ = cat_str($<s>1, " AND ", $<s>3);
	}
	;

equality_expr:
	relational_expr
	| equality_expr EQU relational_expr
	{
            $$ = cat_str($<s>1, " = ", $<s>3);
	}
	| equality_expr NEQ relational_expr
	{
            $$ = cat_str($<s>1, " != ", $<s>3);
	}
	;

relational_expr:
	unary_expr 
	| relational_expr LT unary_expr
	{
            $$ = cat_str($<s>1, " < ", $<s>3);
	}
	| relational_expr LTE unary_expr
	{
            $$ = cat_str($<s>1, " <= ", $<s>3);
	}
	| relational_expr GT unary_expr
	{ 
            $$ = cat_str($<s>1, " > ", $<s>3);
	}
	| relational_expr GTE unary_expr
	{
            $$ = cat_str($<s>1, " >= ", $<s>3);
	}
	;

unary_expr:
	primary
	| NOT primary
	;

primary:
	VAR
	{ 
            string s = getFilterTab($<s>1);
            char* str = (char*) malloc(strlen(s.c_str()) + 1);
            strcpy(str, s.c_str());
            $$ = str;
	}
	| VALUE
        {
           $$ = $<s>1;
        }
	| CSTR
	{
            string s($<s>1);
            //replace_quote(s);
            char* str = (char*) malloc(strlen(s.c_str()) + 1);
            strcpy(str, s.c_str());
            $$ = str;
	}
	| INT_NUM
	{
            string s(itos($<i>1));
            char* str = (char*) malloc(strlen(s.c_str()) + 1);
            strcpy(str, s.c_str());
            $$ = str;
	}
        | FLOAT_NUM
        {
            string s(dtos($<i>1));
            char* str = (char*) malloc(strlen(s.c_str()) + 1);
            strcpy(str, s.c_str());
            $$ = str;
        }
	| LP expr RP
	{
		cerr << "This version of rq2sql does not support nested filter expressions." << endl;
		exit(1);
	}
	| BOUND LP VAR RP  
	{
          //  string s = " IS NOT NULL";
          //  char* str = (char*) malloc(strlen(s.c_str()) + strlen($<s>3) + 1);
          //  strcpy(str, $<s>3);
          //  strcat(str, s.c_str());
          //  $$ = str;
           cerr << "This version of query translator does not support code generation for 'BOUND'." << endl;	
	}
	| NOT_BOUND LP VAR RP 
	{
            cerr << "This version of query translator does not support code generation for 'NOT BOUND'." << endl;
	}
        | SURI
        {
            $$ = $<s>1;
        }
        | DATE
        {
            string s($<s>1);
            s = convert_date(s);
            char* str = (char*) malloc(strlen(s.c_str()) + 1);
            strcpy(str, s.c_str());
            $$ = str; 
        } 
        | URL
        {
            $$ = $<s>1;
        }
	;

order_by:
        | ORDER_BY_DESC
        {
            string s($<s>1); 
            string str = format_order_by(s);
            char *cstr = new char[str.length() + 1];
            order_by_stmt = str;
            strcpy(cstr, str.c_str());
            $<s>$ = cstr;
        }
	| ORDER_BY VAR sort_option
	{            
            char* str = (char*) malloc(strlen($<s>1) + strlen($<s>2) + strlen($<s>3) + 1);
            strcpy(str, $<s>1);
            strcat(str, " ");
            string s($<s>2);
            strcat(str, (s.substr(1, s.size() - 1)).c_str());
            string s2($<s>3);
            strcat(str, " ");
            strcat(str, s2.c_str());
            strcat(str, " ");
            
            order_by_stmt = str;
            $<s>$ = str;
	} 
        | ORDER_BY_XSD
        {
            string s($<s>1);
            string str = format_order_by_xsd(s);
            char *cstr = new char[str.length() + 1];
            order_by_stmt = str;
            strcpy(cstr, str.c_str());
            $<s>$ = cstr;
        }
	;

sort_option:
	{
            char* str = (char*) malloc(strlen("ASC") + 1);
            strcpy(str, "ASC");
            $<s>$ = str;
	}
	| ASC
	| DESC
	;

offset:
	| OFFSET INT_NUM
	{
            const char* istr = itos($<i>2).c_str();
            char* str = (char*) malloc(strlen($<s>1) + strlen(istr) + 1);
            strcpy(str, $<s>1);
            strcat(str, " ");
            strcat(str, istr);

            offset_stmt = str;
            $<s>$ = str; 
	}
	;

limit:
	| LIMIT INT_NUM
	{
            const char* istr = itos($<i>2).c_str();
            char* str = (char*) malloc(strlen($<s>1) + strlen(istr) + 1);
            strcpy(str, $<s>1);
            strcat(str, " ");
            strcat(str, istr);

            limit_stmt = str;
            $<s>$ = str; 
	}
	;

%%

void yyerror(const char* str)
{
	cerr << "Line " << yylineno << ": " << str << "\n";
	yyclearin;
	++num_errors;
}
