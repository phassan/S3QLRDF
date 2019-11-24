/*******************************************************************
 *
 *	Tool:		SPARQL to Spark SQL Compiler
 *	version:	0.1
 *	Author: 	Mahmudul Hassan 
 *	Date: 		September 2018
 *
 ********************************************************************/

#include <string>
#include <sstream>
#include <regex>
#include <vector>
#include <string>
#include <map>
#include <stack>
#include <algorithm>
#include <fstream>
#include "def.h"
#include "utility.h"

using namespace std;

string itos(int num) {
    return to_string(num);
}

string dtos(double num) {
    return to_string(num);
}

bool replace(string& str, const string& from, const string& to) {
    size_t start_pos = str.find(from);
    if (start_pos == string::npos)
        return false;
    str.replace(start_pos, from.length(), to);
    return true;
}

char* cat_str(const char* op1, const char* op, const char* op2) {
    char* str = (char*) malloc(strlen(op1) + strlen(op) + strlen(op2) + 1);
    strcpy(str, op1);
    strcat(str, op);
    strcat(str, op2);
    return str;
}

string cat_str_4(string op1, string op2, string op3, string op4) {
    string str = op1 + op2 + op3 + op4;
    return str;
}

//char* cat2_str(const char* op1, const char* op2, const char* op3, const char* op4, const char* op5) {
//    char* str = (char*) malloc(strlen(op1) + strlen(op2) + strlen(op3) + strlen(op4) + + strlen(op5) + 1);
//    strcpy(str, op1);
//    strcat(str, op2);
//    strcat(str, op3);
//    strcat(str, op4);
//    strcat(str, op5);
//    return str;
//}
//
//char* cat3_str(const char* op1, const char* op2, const char* op3, const char* op4, const char* op5) {
//    char* str = (char*) malloc(strlen(op1) + strlen(op2) + strlen(op3) + strlen(op4) + + strlen(op5) + 1);
//    strcpy(str, op1);
//    strcat(str, op2);
//    strcat(str, op3);
//    string s(op4);
//    regex reg("\"");
//    s = regex_replace(s, reg, "");
//    transform(s.begin(), s.end(), s.begin(), ::tolower);
//    s = "'@" + s + "'";
//    const char* cstr = s.c_str();
//    strcat(str, cstr);
//    strcat(str, op5);
//    return str;
//}

void replace_uri(string& result, const map<string, string>& prefixes, char* uri) {
    string str(uri);
    regex reg("[<|>]");
    str = regex_replace(str, reg, "");

    int found;
    string from_str;
    for (map<string, string>::const_iterator it = prefixes.begin(); it != prefixes.end(); ++it) {
        from_str = regex_replace(it->first, reg, "");
        if ((found = str.find(from_str)) != str.npos) {
            size_t last = (from_str).find_last_of(str);
            str.replace(0, last + 1, (it->second));
            result = str;
        }
    }
}

string format_order_by(string str) {

    regex reg("[(|)]");
    str = regex_replace(str, reg, "");
    string s = "ORDER BY " + str.substr(14, str.length() - 1) + " DESC ";

    return s;
}

string format_order_by_xsd(string s) {
    regex reg("[(|)]");
    s = regex_replace(s, reg, "");
    const char *str = s.c_str();
    //    ;
    char const *pc = strchr(str, '?');
    int idx = pc - str;
    string str1(str);
    s = "ORDER BY " + str1.substr(idx + 1, str1.length() - 1) + " ";
    return s;
}

string replace_with_blank(string result) {
    string str;
    regex reg("[-|/|<|>|'|?|\"]");
    str = regex_replace(result, reg, "");
    return str;
}

string replace_with_underscore(string result) {
    string str;
    regex reg("[<|>|\\:|/|\\.]");
    str = regex_replace(result, reg, "_");
    return str;
}

string convert_date(string s) {
    regex reg("[-|\"]");
    s = regex_replace(s, reg, "");
    const char *str = s.c_str();
    //    ;
    char const *pc = strchr(str, '^');
    int idx = pc - str;
    string str1(str);
    s = str1.substr(0, idx);
    return s;

}

void replace_quote_dtos(string& str) {
    regex reg("\"");
    str = regex_replace(str, reg, "\'");
}

string replace_doublequote(string str) {
    regex reg("\"");
    str = regex_replace(str, reg, "");
    return str;
}

string rmv_caret_srt_from_value(string s) {

    const char *str = s.c_str();
    ;
    char const *pc = strchr(str, '^');
    int idx = pc - str;
    string str1(str);
    s = str1.substr(0, idx);
    regex reg("\"");
    s = regex_replace(s, reg, "");
    return s;
}

string sym_rmv_from_predicate(string s) {

    const char *str = s.c_str();
    //    ;
    char const *pc = strchr(str, ':');
    int idx = pc - str;
    string str1(str);
    s = str1.substr(idx + 1, str1.length() - 1);
    return s;
}

string trim(const string& str) {
    size_t first = str.find_first_not_of(' ');
    if (string::npos == first) {
        return str;
    }
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, (last - first + 1));
}

void refine_Predicate(string str, int pos) {

    string s1, s2;
    string::iterator end_pos = remove(str.begin(), str.end(), ' ');
    str.erase(end_pos, str.end());
    const char *str1 = str.c_str();
    char const *pc = strchr(str1, '=');
    int idx = pc - str1;
    string str2(str1);
    s1 = str2.substr(0, idx);
    s2 = str2.substr(idx + 1, str2.length() - 1);
    predicates.at(pos).first = s2;
    predicates.at(pos).second = "suri";

}

bool elementExistInMap(map<string, string> mapPair, string element) {
    map<string, string>::iterator it;
    it = mapPair.find(element);
    if (it != mapPair.end())
        return true;
    else
        return false;
}

vector<string> split(const string& s, char delimiter) {
    vector<string> tokens;
    string token;
    istringstream tokenStream(s);
    while (getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

void readStatFile(ifstream& file) {
    string str;
    while (getline(file, str)) {
        if (str.at(0) == '$') {
            vector<string> splittedLine = split(str, '\t');
            if (splittedLine.size() > 2) {
                string tabName = splittedLine[1];
                unsigned long int count = stoi(splittedLine[2]);
                table_statistics[tabName] = count;
            }
        } else if (str.at(0) == '#') {
            mva = split(str, '\t');
            mva.erase(mva.begin());
        } else
            ; 
    }

}

const bool set_contains(set<string> ck_set, string str) {
    const bool is_in = ck_set.find(str) != ck_set.end();
    return is_in;
}

bool mva_presents(string str) {
    if (std::find(mva.begin(), mva.end(), str) != mva.end())
        return true;
    else
        return false;
}

string ltrim(const string& s) {
    return regex_replace(s, regex("^\\s+"), string(""));
}

string rtrim(const string& s) {
    return regex_replace(s, regex("\\s+$"), string(""));
}

string removeLastChar(string s) {
    string str = rtrim(s);
    return str.substr(0, str.size() - 1);
}

string v2sCommaDelimited(vector<string> vs) {
    const char* const delim = ", ";
    ostringstream oss;
    copy(vs.begin(), vs.end(), ostream_iterator<string>(oss, delim));
    return oss.str();
}

string v2sTabDelimited(vector<string> vs) {
    const char* const delim = "\t";
    ostringstream oss;
    copy(vs.begin(), vs.end(), ostream_iterator<string>(oss, delim));
    return oss.str();
}

string v2sANDDelimited(vector<string> vs) {
    const char* const delim = " AND ";
    ostringstream oss;
    copy(vs.begin(), vs.end(), ostream_iterator<string>(oss, delim));
    string str = rtrim(oss.str());
    return str.substr(0, str.size() - 4);
}

bool elementExistInVector(vector<string> v, string s) {
    if (find(v.begin(), v.end(), s) != v.end())
        return true;
    else
        return false;
}

vector<string> merge_remove_dups(vector<string> vecA, vector<string> vecB) {
    vecA.insert(vecA.end(), vecB.begin(), vecB.end());
    sort(vecA.begin(), vecA.end());
    auto it = unique(vecA.begin(), vecA.end());
    vecA.erase(it, vecA.end());  
    return vecA;
}

vector<string> remove_dups_vec(vector<string> vec) {
    sort(vec.begin(), vec.end());
    auto it = unique(vec.begin(), vec.end());
    vec.erase(it, vec.end());  
    return vec;
}

bool existCommonElementsIn2Vectors(vector<string> vecA, vector<string> vecB) {
    vector<string> vecC;
    sort(vecA.begin(), vecA.end());
    sort(vecB.begin(), vecB.end());
    set_intersection(vecA.begin(), vecA.end(), vecB.begin(), vecB.end(), back_inserter(vecC));
    if (vecC.size() >= 1)
        return true;
    else
        return false;
}

int getIndex_vector(vector<string> v, string s) {
    vector<string>::iterator it = find(v.begin(), v.end(), s);
    int index;
    if (it != v.end()) {
        index = distance(v.begin(), it);
    } else {
        index = -1;
    }
    return index;
}

vector<string> vector_intersection(vector<string> vecA, vector<string> vecB) {
    vector<string> vecC;
    sort(vecA.begin(), vecA.end());
    sort(vecB.begin(), vecB.end());
    set_intersection(vecA.begin(), vecA.end(), vecB.begin(), vecB.end(), back_inserter(vecC));
    return vecC;
}