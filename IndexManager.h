#ifndef _INDEXMANAGER_H
#define _INDEXMANAGER_H

#include "global.h"
#include "BufferManager.h"
#include <string>
#include <set>
#include <map>

struct IndexIter;
struct Node;

class IndexManager {
public:
    IndexManager(BufferManager &);
    ~IndexManager();

    Response create(const std::string &indexName, const std::string &dbName, Table &table, const AttrType &attr);

    Response drop(const std::string &indexName);

    Response insert(const std::string &indexName, const element &e, long offset);

    Response erase(const std::string &indexName, const element &e);

    //return offset of the element , -1 for not found
    long find(const std::string &indexName, const element &e);

    std::set <long> greater(const std::string &indexName, const element &e);

    std::set <long> less(const std::string &indexName, const element &e);

    //return content in [lhs, rhs]
    std::set <long> inRange(const std::string &indexName, const element &lhs, const element &rhs);

    void load();
    void save();

private:
    BufferManager &bfm;
    std::map <element, long> mp;
    std::string currentFile;
};

#endif