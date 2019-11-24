/*******************************************************************
*
*	Tool:		SPARQL to Spark SQL Compiler
*	version:	0.1
*	Author: 	Mahmudul Hassan 
*	Date: 		September 2018
*
********************************************************************/

%{
#include <cstdio>
#include <iostream>
#include "grammar.tab.h"
%}

%option case-insensitive

%%
"SELECT"			{ return SELECT; }			
"DISTINCT"			{ return DISTINCT; }			
"WHERE"				{ return WHERE; }
"BASE"                          { return BASE; }			
"PREFIX"			{ return PREFIX; }
"@PREFIX"			{ return AT_PREFIX; } 						
"FILTER"			{ return FILTER; }			
"UNION"				{ return UNION; }			
"OPTIONAL"			{ return OPTIONAL; }

"ORDER BY DESC"\([^)]*\)        { yylval.s = strdup(yytext); return ORDER_BY_DESC; }
"ORDER BY XSD:"[a-z]*\([a-z]*\([^)]*\)\)  { yylval.s = strdup(yytext); return ORDER_BY_XSD; }
"REGEX"                         { yylval.s = strdup(yytext); return REGEX; }        
"langMatches"                   { yylval.s = strdup(yytext); return LANGMATCHES; }
"ORDER BY"			{ yylval.s = strdup(yytext); return ORDER_BY; }			
"ASC"				{ yylval.s = strdup(yytext); return ASC; }
"DESC"				{ yylval.s = strdup(yytext); return DESC; }
"LIMIT"				{ yylval.s = strdup(yytext); return LIMIT; }
"OFFSET"			{ yylval.s = strdup(yytext); return OFFSET; }
"!bound"                        { yylval.s = strdup(yytext); return NOT_BOUND; }
"bound"				{ return BOUND; }
"lang"                          { yylval.s = strdup(yytext); return LANG; }			
"{"				{ return LB; }				
"}"				{ return RB; }				
"("				{ return LP; }				
")"				{ return RP; }				
";"				{ return SEMICOLON; }
","				{ return COMMA; }
"="				{ return EQU; }				
"!"				{ return NOT; }				
"!="				{ return NEQ; }			
"&&"				{ return AND; }			
"||"				{ return OR; }			
"<"				{ return LT; }				
"<="				{ return LTE; }			
">"				{ return GT; }				
">="				{ return GTE; }	

[1-9][0-9]*			{ yylval.i = atoi(yytext); return INT_NUM; }
[+-]?([0-9]*[.])?[0-9]+         { yylval.d = atof(yytext); return FLOAT_NUM; }                     

[a-z][a-z0-9_]*			{ yylval.s = strdup(yytext); return IDT; }
\?[a-z][a-z0-9_]*		{ yylval.s = strdup(yytext); return VAR; }
[a-z][a-z0-9\-?]*":"[a-z0-9_]+     { yylval.s = strdup(yytext); return SURI; }
[a-z][a-z0-9\-?]*":"		{ yylval.s = strdup(yytext); return SYM; }

\<http[^\>]*\>			{ yylval.s = strdup(yytext); return URI; }
\<[a-z0-9_]*\>			{ yylval.s = strdup(yytext); return URL; }

\"[0-9]{4}-[0-9]{2}-[0-9]{2}\"\^\^[^ )]*  { yylval.s = strdup(yytext); return DATE; }							
\"(\\.|[^"])*\"\^\^[^ )]*	{ yylval.s = strdup(yytext); return VALUE; }							
\"(\\.|[^"])*\"			{ yylval.s = strdup(yytext); return CSTR; }

"\n"				{ yylineno++; }
\.				{ return DOT; }				
[ \t]+				{ /* ignore white space */ }																											
"#".*				{ /* ignore comments */ }
[ \t\v\f\r]+			{ }
.				{ std::cerr << "Lexical Error!\n"; }		
%%

int yywrap() {
	return 1;
}