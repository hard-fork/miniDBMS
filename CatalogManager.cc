#include <map>
#include <string>
#include <vector>
#include <cassert>
#include <fstream>
#include <iostream>
#include <algorithm>
#include "global.h"
#include "CatalogManager.h"
#include "BufferManager.h"
#include "RecordManager.h"

bool CatalogManager::hasTable(const std::string &tableName) {
    std::ifstream fin((tableName).c_str());
    if (!fin) return false;
    std::string header;
    fin >> header;
    fin.close();
    return tableName == header;
}

Table CatalogManager::createTable(const std::string &tableName, const std::vector<AttrType> &attrs) {
    assert(!hasTable(tableName));
    Table nt(tableName, attrs);
    nt.write();
    return nt;
}

//TODO 5: load table information
//input a string table name, return a table structure

Table CatalogManager::loadTable(const std::string &tableName) {
    assert(hasTable(tableName));
    //first make sure this table information exits!
    Table nt;
    int attrSum;
    std::ifstream fin((tableName).c_str());
    //==========================================
    // YOUR CODE GOES HERE!
    // HIT: read createTable() function above and table.write() function in file global.cc
    // follow the file structure
    //==========================================
    return nt;
}

void CatalogManager::dropTable(const std::string &tableName) {
    assert(hasTable(tableName));
    std::string name = tableName.substr(0, tableName.find('.'));
    Table nt = loadTable(tableName);
    std::vector<AttrType> &attr = nt.attributes;
    for (size_t i = 0; i < attr.size(); ++ i) {
        bool flag = false;
        for (auto &x : attr[i].indices) {
            flag = true;
            dropIndex(x);
        }
        if (flag) {
            BufferManager bm;
            std::string indexFileName = name + "." + attr[i].name + ".index";
            bm.deleteFile(indexFileName);
        }
    }
    remove((tableName).c_str());
}

bool CatalogManager::hasIndex(const std::string &indexName) {
    return IM.find(indexName);
}

bool CatalogManager::createIndex(const std::string &tableName, const std::string &indexName, int attrIndex) {
    if (hasIndex(indexName)) return false;
    IM.insert(indexName, tableName, attrIndex);
    return true;
}

bool CatalogManager::dropIndex(const std::string &indexName) {
    if (!hasIndex(indexName)) return false;
    IM.erase(indexName);
    return true;
}

std::pair<std::string, int> CatalogManager::getIndex(const std::string &indexName) {
    assert(hasIndex(indexName));
    return IM.ask(indexName);
}
