#include "sqlparser.h"
#include <algorithm>
#include <cctype>
#include <iostream>

// Cross-platform case-insensitive comparison
#ifdef _WIN32
    #define STRCASECMP _stricmp
#else
    #include <strings.h>
    #define STRCASECMP strcasecmp
#endif

using namespace SQL;
using namespace std;

namespace SQL {

// Helper function: trim leading and trailing spaces
static string trim(const string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (string::npos == first) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

// Helper function: convert string to uppercase (for keyword search)
static string toUpper(string str) {
    transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
}

// Helper function: case-insensitive search
static size_t findKeyword(const string& sql, const string& keyword, size_t startPos = 0) {
    string upperSql = toUpper(sql);
    string upperKey = toUpper(keyword);
    return upperSql.find(upperKey, startPos);
}

string extractTableName(const std::string& sql) {
    regex patternCreate(R"(\bCREATE\s+TABLE\s+)", std::regex_constants::icase);
    regex patternSelect(R"(\bFROM\s+(\w+))", std::regex_constants::icase);
    regex patternInsert(R"(\bINSERT\s+INTO\s+(\w+))", std::regex_constants::icase);
    regex patternUpdate(R"(\bUPDATE\s+(\w+))", std::regex_constants::icase);
    regex patternDrop(R"(\bDROP\s+TABLE\s+(\w+))", std::regex_constants::icase);
    regex patternDelete(R"(\bDELETE\s+FROM\s+(\w+))", std::regex_constants::icase); // Added DELETE support
    smatch matches;

    if (regex_search(sql, matches, patternCreate)) {
        size_t pos = matches.position(0) + matches.length(0);
        size_t end = sql.find('(', pos);
        if (end != string::npos) {
            return trim(sql.substr(pos, end - pos));
        }
    }
    else if (regex_search(sql, matches, patternDrop) && matches.size() > 1) return matches[1];
    else if (regex_search(sql, matches, patternSelect) && matches.size() > 1) return matches[1];
    else if (regex_search(sql, matches, patternInsert) && matches.size() > 1) return matches[1];
    else if (regex_search(sql, matches, patternUpdate) && matches.size() > 1) return matches[1];
    else if (regex_search(sql, matches, patternDelete) && matches.size() > 1) return matches[1];

    return "";
}

string extractJoinTableName(const std::string& sql){
    regex patternJoin(R"(\bJOIN\s+(\w+))", std::regex_constants::icase);
    smatch matches;
    if (regex_search(sql, matches, patternJoin) && matches.size() > 1) {
        return matches[1];
    }
    return "";
}

vector<string> splitCondition(const std::string& condition) {
    vector<string> parts(3);
    string cond = trim(condition);
    
    // Define all possible operators, order is important (>= matches before >)
    const char* ops[] = {"<=", ">=", "!=", "==", "=", "<", ">"};
    size_t opPos = string::npos;
    string opStr;

    for (const char* op : ops) {
        opPos = cond.find(op);
        if (opPos != string::npos) {
            opStr = op;
            break;
        }
    }

    if (opPos == string::npos) {
        // Operator not found, possibly a syntax error
        parts[0] = cond;
        parts[1] = ""; 
        parts[2] = "";
        return parts;
    }

    parts[0] = trim(cond.substr(0, opPos)); // Attribute name
    parts[1] = opStr;                        // Operator
    parts[2] = trim(cond.substr(opPos + opStr.length())); // Value

    // Remove quotes from value
    if (parts[2].size() >= 2) {
        char first = parts[2].front();
        char last = parts[2].back();
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            parts[2] = parts[2].substr(1, parts[2].size() - 2);
        }
    }

    return parts;
}

vector<WhereCondition> parseWhereClause(const std::string& whereClause) {
    vector<WhereCondition> conditions;
    if (trim(whereClause).empty()) return conditions;

    // Simple split logic by AND/OR (does not support nested parentheses, simplified version)
    // Here we replace " AND " with a special character to split, or simply search
    // To keep the project simple, we assume linear logical relationships
    
    string clause = whereClause;
    // Remove trailing semicolon
    if (!clause.empty() && clause.back() == ';') clause.pop_back();

    // Simplified handling: assume only AND or only OR, or mixed but split sequentially
    // A more complete method would be to traverse the string and record positions
    
    size_t pos = 0;
    size_t prev = 0;
    string upperClause = toUpper(clause);
    
    while (pos < clause.length()) {
        size_t nextAnd = upperClause.find(" AND ", pos);
        size_t nextOr = upperClause.find(" OR ", pos);
        size_t nextSplit = min(nextAnd, nextOr);

        string singleCondStr;
        if (nextSplit == string::npos) {
            singleCondStr = clause.substr(pos);
            pos = clause.length();
        } else {
            singleCondStr = clause.substr(pos, nextSplit - pos);
            pos = nextSplit + (nextSplit == nextAnd ? 5 : 4); // Skip " AND " or " OR "
        }

        vector<string> parts = splitCondition(singleCondStr);
        if (!parts[1].empty()) {
            conditions.push_back(WhereCondition(parts[0], parts[1], parts[2]));
        }
    }
    return conditions;
}

vector<WhereCondition> parseSelectStatement(const std::string& sql, vector<string>& attributeNames, vector<LOGIC>& Logics) {
    // 1. Extract part between SELECT and FROM (attributes)
    size_t selectPos = findKeyword(sql, "SELECT");
    size_t fromPos = findKeyword(sql, "FROM");
    
    if (selectPos == string::npos || fromPos == string::npos || fromPos < selectPos) {
        cout << "SQL Syntax Error: Missing SELECT or FROM" << endl;
        return vector<WhereCondition>();
    }

    string attrPart = sql.substr(selectPos + 6, fromPos - (selectPos + 6));
    istringstream attrStream(attrPart);
    string attr;
    while (getline(attrStream, attr, ',')) {
        attr = trim(attr);
        if (!attr.empty()) attributeNames.push_back(attr);
    }

    // 2. Extract WHERE part
    size_t wherePos = findKeyword(sql, "WHERE");
    if (wherePos == string::npos) {
        // No WHERE clause, return empty conditions
        return vector<WhereCondition>();
    }

    string wherePart = sql.substr(wherePos + 5); // Skip "WHERE"
    
    // 3. Extract logic operators (AND/OR)
    // Note: This needs to be synchronized with parseWhereClause. parseWhereClause is responsible for splitting conditions,
    // here responsible for recording AND or OR.
    string upperWhere = toUpper(wherePart);
    size_t pos = 0;
    while (true) {
        size_t nextAnd = upperWhere.find(" AND ", pos);
        size_t nextOr = upperWhere.find(" OR ", pos);
        
        if (nextAnd == string::npos && nextOr == string::npos) break;
        
        if (nextAnd < nextOr) {
            Logics.push_back(AND_LOGIC);
            pos = nextAnd + 5;
        } else {
            Logics.push_back(OR_LOGIC);
            pos = nextOr + 4;
        }
    }

    return parseWhereClause(wherePart);
}

vector<WhereCondition> parseDeleteStatement(const std::string& sql, vector<LOGIC>& logic) {
    size_t wherePos = findKeyword(sql, "WHERE");
    if (wherePos == string::npos) {
        // No WHERE, means delete all (but might not be allowed in your logic)
        // or return empty condition list
        return vector<WhereCondition>();
    }

    string wherePart = sql.substr(wherePos + 5);
    
    // Extract logic
    string upperWhere = toUpper(wherePart);
    size_t pos = 0;
    while (true) {
        size_t nextAnd = upperWhere.find(" AND ", pos);
        size_t nextOr = upperWhere.find(" OR ", pos);
        if (nextAnd == string::npos && nextOr == string::npos) break;
        if (nextAnd < nextOr) {
            logic.push_back(AND_LOGIC);
            pos = nextAnd + 5;
        } else {
            logic.push_back(OR_LOGIC);
            pos = nextOr + 4;
        }
    }

    return parseWhereClause(wherePart);
}

vector<WhereCondition> parseSetStatement(const std::string& s) {
    vector<WhereCondition> w;
    string part;
    istringstream iss(s);
    
    while (getline(iss, part, ',')) {
        vector<string> parts = splitCondition(part);
        if (!parts[1].empty()) {
            w.push_back(WhereCondition(parts[0], parts[1], parts[2]));
        }
    }
    return w;
}

vector<WhereCondition> parseUpdateStatement(const std::string& sql, vector<WhereCondition>& set_attributes) {
    size_t updatePos = findKeyword(sql, "UPDATE");
    size_t setPos = findKeyword(sql, "SET");
    size_t wherePos = findKeyword(sql, "WHERE");

    if (updatePos == string::npos || setPos == string::npos) {
        cout << "UPDATE Syntax Error" << endl;
        return vector<WhereCondition>();
    }

    // Extract SET part
    size_t setEnd = (wherePos == string::npos) ? sql.length() : wherePos;
    string setPart = sql.substr(setPos + 3, setEnd - (setPos + 3));
    set_attributes = parseSetStatement(setPart);

    // Extract WHERE part
    if (wherePos != string::npos) {
        return parseWhereClause(sql.substr(wherePos + 5));
    }

    return vector<WhereCondition>();
}

vector<attribute> parseCreateTableStatement(const std::string& sql) {
    vector<attribute> attr_arry;
    string columnsPart;
    // More robust regex: find content of first parenthesis
    size_t start = sql.find('(');
    size_t end = sql.rfind(')');
    
    if (start == string::npos || end == string::npos || start >= end) {
        return attr_arry;
    }
    
    columnsPart = sql.substr(start + 1, end - start - 1);

    istringstream columnStream(columnsPart);
    string columnDef;
    int keynum = 0;

    while (getline(columnStream, columnDef, ',')) {
        columnDef = trim(columnDef); // Trim spaces from both ends
        if (columnDef.empty()) continue;

        istringstream columnDetails(columnDef);
        string columnName, columnType, columeConstraint;
        int maxLength = 0;

        columnDetails >> columnName; // Automatically skip spaces to read word
        if (columnDetails >> columnType) {
            // Handle VARCHAR(100) case
            size_t leftPos = columnType.find('(');
            if (leftPos != string::npos) {
                size_t rightPos = columnType.find(')');
                string lenStr = columnType.substr(leftPos + 1, rightPos - leftPos - 1);
                try {
                    maxLength = stoi(lenStr);
                } catch(...) { maxLength = 100; }
                columnType = columnType.substr(0, leftPos);
            }
        }

        KEY_KIND key_kind = STRING_KEY;
        string upperType = toUpper(columnType);
        if (upperType == "INT") {
            key_kind = INT_KEY;
            maxLength = 4;
        } else if (upperType == "LONG" || upperType == "BIGINT") { // Support LONG or BIGINT
            key_kind = LL_KEY;
            maxLength = 8;
        }

        // Check subsequent constraints (PRIMARY KEY, ref etc.)
        string remainder;
        getline(columnDetails, remainder); // Read remaining part
        remainder = trim(remainder);
        
        columeConstraint = "";
        if (!remainder.empty()) {
            if (toUpper(remainder) == "PRIMARY KEY") {
                columeConstraint = "PRIMARY KEY";
                if (keynum > 0) {
                    cout << "Error: Only one primary key allowed" << endl;
                    return vector<attribute>();
                }
                keynum++;
            } else if (remainder.find("ref") == 0) {
                columeConstraint = remainder;
            }
        }

        if (columeConstraint.empty()) {
            attr_arry.push_back(attribute(columnName, key_kind, maxLength));
        } else {
            attr_arry.push_back(attribute(columnName, key_kind, maxLength, columeConstraint));
        }
    }
    
    return attr_arry;
}

vector<vector<string>> parseInsertStatement(const std::string& sql) {
    vector<vector<string>> rows;
    vector<string> columns;

    size_t valuesPos = findKeyword(sql, "VALUES");
    size_t colStart = sql.find('(');
    
    if (colStart == string::npos || valuesPos == string::npos || colStart > valuesPos) {
        cout << "Syntax Error: INSERT statement format incorrect" << endl;
        return rows;
    }
    
    size_t colEnd = sql.rfind(')', valuesPos);
    string colPart = sql.substr(colStart + 1, colEnd - colStart - 1);
    istringstream colStream(colPart);
    string colName;
    
    while (getline(colStream, colName, ',')) {
        columns.push_back(trim(colName));
    }
    rows.push_back(columns);

    string valuesPart = sql.substr(valuesPos + 6); 
    size_t pos1, pos2;
    size_t searchStart = 0;
    while ((pos1 = valuesPart.find("(", searchStart)) != std::string::npos) {
        pos2 = valuesPart.find(")", pos1);
        if (pos2 == string::npos) break;

        string valRowStr = valuesPart.substr(pos1 + 1, pos2 - pos1 - 1);
        searchStart = pos2 + 1;

        vector<string> rowData;
        istringstream valStream(valRowStr);
        string val;
        
        while (getline(valStream, val, ',')) {
            val = trim(val);
            // Remove quotes
            if (val.size() >= 2) {
                char first = val.front();
                char last = val.back();
                if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                    val = val.substr(1, val.size() - 2);
                }
            }
            rowData.push_back(val);
        }
        rows.push_back(rowData);
    }

    return rows;
}

}