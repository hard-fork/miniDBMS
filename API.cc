#include <vector>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <iostream>
#include <algorithm>
#include "API.h"

// File name rule:
// Table: $tableName.table
// Index: $tableName.$attrName.index
// DB File: $tableName.db

// bm: bufferManager, im: indexManager, cm: catalogManager


API::API() :im(bm),rm(bm,im){
}
//Create index on one table, first make sure table and attribute exist, and the attribute should be unique.
//Then use the indexMnanager to create the index, index may have alias.
Response API::createIndex(const std::string &indexName, const std::string &tableName, const std::string &attrName) {
    if (!cm.hasTable(tableName + ".table")) {
        return Response("Table does not exist");
    }
    int attrIndex = -1;
    Table nt = cm.loadTable(tableName + ".table");
    for (size_t i = 0; i < nt.attributes.size(); ++ i) {
        if (nt.attributes[i].name == attrName) {
            attrIndex = i;
            break;
        }
    }
    if (attrIndex == -1) {
        return Response("Attribute does not exist");
    }
    if (!nt.attributes[attrIndex].unique) {
        return Response("This attribute must be unique");
    }
    if (!cm.createIndex(tableName, indexName, attrIndex)) {
        return Response("This index already exists");
    }
    nt.attributes[attrIndex].indices.insert(indexName);
    nt.write();
    if (nt.attributes[attrIndex].indices.size() == 1) {
        // Index Manager add index
        im.create(tableName+"."+nt.attributes[attrIndex].name, tableName+".db", nt, nt.attributes[attrIndex]);
    }
    return Response();
}
//drop one index
//first make sure index exist, and update information of the table.
//Then use the catalogManager to drop the index in index catalog
Response API::dropIndex(const std::string &indexName) {
    if (!cm.hasIndex(indexName)) {
        return Response("Index does not exist");
    }
    std::pair<std::string, int> psi = cm.getIndex(indexName);
    std::string tableName = psi.first;
    int attrIndex = psi.second;
    Table nt = cm.loadTable(tableName + ".table");
    assert(nt.attributes[attrIndex].indices.count(indexName));
    nt.attributes[attrIndex].indices.erase(indexName);
    nt.write();
    cm.dropIndex(indexName);
    if (nt.attributes[attrIndex].indices.size() == 0) {
        bm.deleteFile(tableName + "." + nt.attributes[attrIndex].name + ".index");
    }
    return Response();
}
//create one table
//check if already exists,if not use record manager to create table
Response API::createTable(const std::string &tableName, std::vector<AttrType> &data, int pk) {
    if (cm.hasTable(tableName + ".table")) {
        return Response("This table already exists");
    }
    
    Table nt = cm.createTable(tableName + ".table", data);
    nt.attributes[pk].unique = true;
    nt.write();

    std::string dbName = tableName + ".db";
    // Record Manager create database
    rm.RecordManagerTableCreate(dbName);

    return createIndex(tableName + ".PrimaryKeyDefault", tableName, nt.attributes[pk].name);
}
//TODO 1: drop one table
//input table name, return response
Response API::dropTable(const std::string &tableName) {
    //=========================================
    //YOUR CODE GOES HERE:
    //first, use use catalog manager to check if the table information exists,
    //       if yes then use catalog manager clean the table information file
    //second, use record manager to clean the data file of the table.
    //==========================================
    if(!cm.hasTable(tableName + ".table"))
        return Response("This table does nojt exist");

    cm.dropTable(tableName);

    rm.RecordManagerTableDelete(tableName);
}

//select statement
//input table name and conditions
//check table exist or not, check the conditions and let record manager to handle it
Response API::Select(const std::string &tableName, const Filter &filter) {
    if (!cm.hasTable(tableName + ".table")) {
        return Response("Table does not exist");
    }
    
    Table nt = cm.loadTable(tableName + ".table");
    std::string dbName = tableName + ".db";
    for (size_t i = 0; i < filter.rules.size(); ++ i) {
        element val = filter.rules[i].val;
        int attrIndex = filter.rules[i].index;
        if (nt.attributes[attrIndex].type != val.type) {
            return Response("Type mismatch");
        }
    }
    
    for (size_t i = 0; i < filter.rules.size(); ++ i) {
        if (filter.rules[i].type == 2) { // =
            element val = filter.rules[i].val;
            int attrIndex = filter.rules[i].index;
            if (!nt.attributes[attrIndex].indices.empty()) { // has index on it
                std::string indexFileName = tableName + "." + nt.attributes[attrIndex].name;
                long offset = getOffset(indexFileName, val);
                // offset == -1 for no that val
                // Record Manager return select result by using index
                // input （dbName）(offset) (filter)，current table (nt)，return result(type:vector<vector<element>>)
                return Response(rm.RecordManagerRecordSelect(dbName, offset, filter, nt));
            }
        }
    }

    std::set<long> offset;
    // Record Manager get offset according to dbName
    // input (daName)，return offset (set)
    offset = rm.RecordManagerGetAllOffsets(dbName, nt);
    for (size_t i = 0; i < filter.rules.size(); ++ i) {
        Rule rule = filter.rules[i];
        int attrIndex = rule.index;
        if (nt.attributes[attrIndex].indices.empty()) {
            continue;
        }
        element val = rule.val;
        std::string indexFileName = tableName + "." + nt.attributes[attrIndex].name;
        std::set<long> newOffset;
        long tmpOffset;
        // B plus tree get offset according to filter rules
        switch (rule.type) {
            case 0: // <
                newOffset = getLessOffset(indexFileName, val);
                break;
            case 1: // <=
                newOffset = getLessOffset(indexFileName, val);
                tmpOffset = getOffset(indexFileName, val);
                if (tmpOffset != -1) {
                    newOffset.insert(tmpOffset);
                }
                break;
            case 2: // =
                assert(false);
                break;
            case 3: // >=
                newOffset = getMoreOffset(indexFileName, val);
                tmpOffset = getOffset(indexFileName, val);
                if (tmpOffset != -1) {
                    newOffset.insert(tmpOffset);
                }
                break;
            case 4: // >
                newOffset = getMoreOffset(indexFileName, val);
                break;
            case 5: // <>
                newOffset = offset;
                break;
            default: 
                assert(false);
                break;
        }
        std::vector<long> tmp(offset.size());
        std::vector<long>::iterator end = std::set_intersection(offset.begin(), offset.end(), newOffset.begin(), newOffset.end(), tmp.begin());
        offset.clear();
        for (std::vector<long>::iterator it = tmp.begin(); it != end; ++ it) {
            offset.insert(*it);
        }
    }
    
    std::vector<std::vector<element> > res;
    for (auto x : offset) {
        std::vector<std::vector<element> > tmp;
        // Record Manager return select result by using index
        tmp = rm.RecordManagerRecordSelect(dbName, x, filter, nt);
        for (size_t i = 0; i < tmp.size(); ++ i) {
            res.push_back(tmp[i]);
        }
    }
    return Response(res);
}

Response API::Delete(const std::string &tableName, const Filter &filter) {
    if (!cm.hasTable(tableName + ".table")) {
        return Response("Table does not exist");
    }
    
    Table nt = cm.loadTable(tableName + ".table");
    std::string dbName = tableName + ".db";
    for (size_t i = 0; i < filter.rules.size(); ++ i) {
        element val = filter.rules[i].val;
        int attrIndex = filter.rules[i].index;
        if (nt.attributes[attrIndex].type != val.type) {
            return Response("Type mismatch");
        }
    }
    
    for (size_t i = 0; i < filter.rules.size(); ++ i) {
        if (filter.rules[i].type == 2) { // =
            element val = filter.rules[i].val;
            int attrIndex = filter.rules[i].index;
            if (!nt.attributes[attrIndex].indices.empty()) { // has index on it
                std::string indexFileName = tableName + "." + nt.attributes[attrIndex].name;
                long offset = getOffset(indexFileName, val);
                // Record Manager delete records by using index
                rm.RecordManagerRecordDelete(dbName, offset, filter, nt);
                return Response();
            }
        }
    }
    std::set<long> offset;
    // Record Manager get offset according to dbName
    offset = rm.RecordManagerGetAllOffsets(dbName, nt);
    for (size_t i = 0; i < filter.rules.size(); ++ i) {
        Rule rule = filter.rules[i];
        int attrIndex = rule.index;
        if (nt.attributes[attrIndex].indices.empty()) {
            continue;
        }
        element val = rule.val;
        std::string indexFileName = tableName + "." + nt.attributes[attrIndex].name;
        std::set<long> newOffset;
        long tmpOffset;
        // B plus tree get offset according to filter rules
        switch (rule.type) {
            case 0: // <
                newOffset = getLessOffset(indexFileName, val);
                break;
            case 1: // <=
                newOffset = getLessOffset(indexFileName, val);
                tmpOffset = getOffset(indexFileName, val);
                if (tmpOffset != -1) {
                    newOffset.insert(tmpOffset);
                }
                break;
            case 2: // =
                assert(false);
                break;
            case 3: // >=
                newOffset = getMoreOffset(indexFileName, val);
                tmpOffset = getOffset(indexFileName, val);
                if (tmpOffset != -1) {
                    newOffset.insert(tmpOffset);
                }
                break;
            case 4: // >
                newOffset = getMoreOffset(indexFileName, val);
                break;
            case 5: // <>
                newOffset = offset;
                break;
            default: 
                assert(false);
                break;
        }
        std::vector<long> tmp(offset.size());
        std::vector<long>::iterator end = std::set_intersection(offset.begin(), offset.end(), newOffset.begin(), newOffset.end(), tmp.begin());
        offset.clear();
        for (std::vector<long>::iterator it = tmp.begin(); it != end; ++ it) {
            offset.insert(*it);
        }
    }
    
    for (auto x : offset) {
        // Record Manager delete records by using index
        rm.RecordManagerRecordDelete(dbName, x, filter, nt);
    }
    return Response();
}

Response API::Insert(const std::string &tableName, const std::vector<element> entry) {
    if (!cm.hasTable(tableName + ".table")) {
        return Response("Table does not exist");
    }
    Table nt = cm.loadTable(tableName + ".table");
    std::string dbName = tableName + ".db";
    if (nt.attributes.size() != entry.size()) {
        return Response("Type mismatch");
    }
    
    for (size_t i = 0; i < nt.attributes.size(); ++ i) {
        if (nt.attributes[i].unique) {
            Filter filter;
            filter.addRule(Rule(i, 2, entry[i]));
            Response tmp = Select(tableName, filter);
            if (!tmp.result.empty()) {
                return Response("unique integrity violation");
            }
        }
    }
    // Record Manager insert record
    rm.RecordManagerRecordInsert(dbName, entry, nt);
    // NOTE: table information may be changed after that
    return Response();
}

std::set<long> API::getMoreOffset(const std::string indexName, const element val) { // >
    return im.greater(indexName, val);
}

std::set<long> API::getLessOffset(const std::string indexName, const element val) { // <
    return im.less(indexName, val);
}

long API::getOffset(const std::string indexName, const element val) { // =
    return im.find(indexName, val);
}
