/*******************************************************************
 *
 *	Tool:		SPARQL to Spark SQL Compiler
 *	version:	0.1
 *	Author: 	Mahmudul Hassan 
 *	Date: 		August 24, 2019
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
#include <iostream>
#include <unordered_set>
#include <utility>

using namespace std;

struct ept {
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

vector<ept> eptTab;
vector<ept> re_eptTab;

struct pair_hash {

    template <class T1, class T2>
    std::size_t operator()(std::pair<T1, T2> const &pair) const {
        std::size_t h1 = std::hash<T1>()(pair.first);
        std::size_t h2 = std::hash<T2>()(pair.second);

        return h1 ^ h2;
    }
};

void findUniqueSubject() {
    unordered_set<pair<string, string>, pair_hash> set;
    set.insert(subjects.begin(), subjects.end());
    uSubjects = vector<pair<string, string> >(set.begin(), set.end());
}

string getFilterTab(string s) {
    string str;
    auto it1 = s_tab.find(s);
    if (it1 != s_tab.end()) {
        str = it1->second + ".s";
    } else {
        vector<string>::iterator it = find(f_objects.begin(), f_objects.end(), s);
        auto it2 = s_tab.find(subjects[distance(f_objects.begin(), it)].first);
        if (it2 != s_tab.end()) {
            string p = replace_with_underscore(predicates[distance(f_objects.begin(), it)].first);
            if (mva_presents(p)) {
                str = it2->second + "." + p + "_lve";
            } else {
                str = it2->second + "." + p;
            }
        } else {
            ;
        }
    }
    return str;
}

void setInternalTab() {
    for (map<string, vector<string> >::iterator it = s_to_ps.begin(); it != s_to_ps.end(); ++it) {
        vector<pair<string, unsigned long int>> tab_rowCount;
        for (unsigned int i = 0; i < it->second.size(); ++i) {
            auto it2 = table_statistics.find(it->second.at(i));
            if (it2 != table_statistics.end()) {
                tab_rowCount.push_back(make_pair(it2->first, it2->second));
            }
        }
        sort(tab_rowCount.begin(), tab_rowCount.end(), [](auto &left, auto &right) {
            return left.second < right.second;
        });
        s_internalTab.push_back(make_pair(it->first, tab_rowCount.front().first));
        s_internalTab_rowCount.push_back(make_pair(it->first, tab_rowCount.front().second));
    }
}

void get_joinVars() {
    join_var = projection;
    for (map<string, vector<string> >::iterator it = join_var.begin(); it != join_var.end(); ++it) {
        if (it->second.at(0) == "s") {
            it->second.at(0) = it->first;
        }
    }
}

void refine_Selection() {
    for (map<string, string>::iterator it = selection.begin(); it != selection.end(); ++it) {
        selection[it->first] = rtrim(ltrim(it->second));
    }
}

bool entryComparator(const ept & e1, const ept & e2) {
    if (e1.bvCount != e2.bvCount)
        return (e2.bvCount < e1.bvCount);
    return (e1.rowCount < e2.rowCount);
}

void get_ept() {
    for (unsigned int i = 0; i < uSubjects.size(); ++i) {
        //        cout << i << "-->" << uSubjects.at(i).first << " " << uSubjects.at(i).second << " " << endl;
        string subject = uSubjects.at(i).first;
        string exTabName;
        string inTabName;
        unsigned int bvCount;
        unsigned long int rowCount;
        vector<string> proj;
        string seln;
        string lveStmt;
        vector<string> joinVar;

        for (map<string, string>::iterator it = s_tab.begin(); it != s_tab.end(); ++it) {
            if (uSubjects.at(i).first == it->first) {
                exTabName = it->second;
            }
        }

        for (unsigned int m = 0; m < s_internalTab.size(); ++m) {
            if (uSubjects.at(i).first == s_internalTab.at(m).first) {
                inTabName = s_internalTab.at(m).second;
            }
        }

        for (map<string, unsigned int>::iterator it = s_to_bvc.begin(); it != s_to_bvc.end(); ++it) {
            if (uSubjects.at(i).first == it->first) {
                bvCount = it->second;
            }
        }

        for (unsigned int it = 0; it < s_internalTab_rowCount.size(); ++it) {
            if (uSubjects.at(i).first == s_internalTab_rowCount.at(it).first) {
                rowCount = s_internalTab_rowCount.at(it).second;
            }
        }

        for (map<string, vector<string> >::iterator it = projection.begin(); it != projection.end(); ++it) {
            if (uSubjects.at(i).first == it->first) {
                proj = it->second;
            }
        }

        for (map<string, string>::iterator it = selection.begin(); it != selection.end(); ++it) {
            if (uSubjects.at(i).first == it->first) {
                seln = it->second;
            }
        }

        for (map<string, string>::iterator it = lve.begin(); it != lve.end(); ++it) {
            if (uSubjects.at(i).first == it->first) {
                lveStmt = it->second;
            }
        }

        for (map<string, vector<string> >::iterator it = join_variable.begin(); it != join_variable.end(); ++it) {
            if (uSubjects.at(i).first == it->first) {
                joinVar = it->second;
            }
        }

        eptTab.push_back({subject, exTabName, inTabName, bvCount, rowCount, proj, seln, lveStmt, joinVar});
    }
    sort(eptTab.begin(), eptTab.end(), entryComparator);
}

void get_rearrangedExtendedPT() {
    if (eptTab.size() <= 2) {
        re_eptTab = eptTab;
    } else {
        re_eptTab.push_back(eptTab[0]);
        vector<string> subj;
        subj.push_back(eptTab[0].subject);
        vector<string> join_vars;
        join_vars = eptTab[0].joinVar;
        int eptTabSize = eptTab.size();

        while (subj.size() != eptTabSize) {
            for (int i = 1; i < eptTab.size(); i++) {
                if (!elementExistInVector(subj, eptTab[i].subject)) {
                    if (existCommonElementsIn2Vectors(join_vars, eptTab[i].joinVar)) {
                        re_eptTab.push_back(eptTab[i]);
                        subj.push_back(eptTab[i].subject);
                        vector<string> mjoin_vars = merge_remove_dups(join_vars, eptTab[i].joinVar);
                        join_vars = mjoin_vars;
                        break;
                    }
                }

            }

        }

    }
}

void get_join_condition() {
    map<string, string> join_cond;
    if (re_eptTab.size() > 1) {
        for (int i = 1; i < re_eptTab.size(); ++i) {

            for (int j = 0; j < i; ++j) {
                if (existCommonElementsIn2Vectors(re_eptTab[j].joinVar, re_eptTab[i].joinVar)) {
                    vector<string> comn_vars = vector_intersection(re_eptTab[j].joinVar, re_eptTab[i].joinVar);
                    string str;
                    int indx1;
                    int indx2;
                    string s = re_eptTab[i].subject;
                    for (int k = 0; k < comn_vars.size(); ++k) {
                        indx1 = getIndex_vector(re_eptTab[i].joinVar, comn_vars.at(k));
                        indx2 = getIndex_vector(re_eptTab[j].joinVar, comn_vars.at(k));
                        str = re_eptTab[i].exTabName + "." + re_eptTab[i].proj.at(indx1) + " = " + re_eptTab[j].exTabName + "." + re_eptTab[j].proj.at(indx2);

                        map<string, string>::iterator it = join_cond.find(s);
                        if (it != join_cond.end()) {
                            string key = it->first;
                            string value = it->second;
                            value = value + " AND " + str;
                            join_cond[key] = value;
                        } else {
                            join_cond[s] = str;
                        }

                    }

                }

            }

        }

    }

    if (re_eptTab.size() > 1) {
        for (int i = 0; i < re_eptTab.size(); ++i) {
            string s = re_eptTab[i].subject;
            map<string, string>::iterator it = join_cond.find(s);
            if (it != join_cond.end()) {
                string value = it->second;
                join_condition.push_back(make_pair(s, value));
            } else {
                join_condition.push_back(make_pair(s, ""));
            }
        }
    }

}

void get_final_select_statement() {
    for (unsigned int i = 0; i < project.size(); ++i) {
        string proj;
        string p;
        p = project.at(i);
        for (unsigned int j = 0; j < re_eptTab.size(); ++j) {
            vector <string> join_vars = re_eptTab.at(j).joinVar;
            if (elementExistInVector(join_vars, p)) {
                int indx = getIndex_vector(join_vars, p);
                if (indx != -1) {
                    string str = re_eptTab.at(j).exTabName + "." + re_eptTab.at(j).proj.at(indx) + " AS " + replace_with_blank(p);
                    project_list.push_back(str);
                    break;
                } else {
                    ;
                }
            }

        }

    }

    final_select = "SELECT " + ltrim(distinct_stmt + removeLastChar(v2sCommaDelimited(project_list))) + " FROM\n";

}

void get_final_filter_statement() {
    if (filters.size() > 0)
        final_filter = "WHERE " + v2sANDDelimited(filters);
    else
        final_filter = "";
}

void get_individul_stm() {
    for (int i = 0; i < re_eptTab.size(); ++i) {
        string s;
        string lve_stmt = re_eptTab.at(i).lveStmt;
        if (lve_stmt.empty()) {
            s = "(SELECT " + removeLastChar(v2sCommaDelimited(re_eptTab.at(i).proj)) + " FROM " + re_eptTab.at(i).inTabName + "\n" +
                    "WHERE " + re_eptTab.at(i).seln + ")\nAS " + re_eptTab.at(i).exTabName;
        } else {
            s = "(SELECT " + removeLastChar(v2sCommaDelimited(re_eptTab.at(i).proj)) + " FROM " + re_eptTab.at(i).inTabName + "\n" +
                    re_eptTab.at(i).lveStmt + "\nWHERE " + re_eptTab.at(i).seln + ")\nAS " + re_eptTab.at(i).exTabName;
        }
        individualStm.push_back(s);
    }
}

string get_temp_views() {
    vector<string> rt;
    for (int i = 0; i < re_eptTab.size(); ++i) {
        rt.push_back(re_eptTab.at(i).inTabName);
    }
    return rtrim(v2sTabDelimited(remove_dups_vec(rt)));
}

string get_final_SQL_statements() {
    string q = final_select;
    for (unsigned int i = 0; i < individualStm.size(); ++i) {
        if (i == 0) {
            q = q + individualStm.at(i) + "\n";
        } else {
            q = q + "JOIN\n" + individualStm.at(i) + " ON (" + join_condition.at(i).second + ")\n";
        }
    }
    q = q + final_filter + "\n" + order_by_stmt + "\n" + limit_stmt;
    return rtrim(q);
}

//string get_orderby_col(string os) {
//    string str;
//    for (unsigned int j = 0; j < re_eptTab.size(); ++j) {
//        vector <string> join_vars = re_eptTab.at(j).joinVar;
//        if (elementExistInVector(join_vars, os)) {
//            int indx = getIndex_vector(join_vars, os);
//            if (indx != -1) {
//                str = re_eptTab.at(j).proj.at(indx);
//                break;
//            } else {
//                ;
//            }
//        }
//    }
//
//    return str;
//}