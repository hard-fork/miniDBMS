//#include <cstring>
#include <cassert>
#include "RecordManager.h"
#include "BufferManager.h"
#include "IndexManager.h"


union INT{
    int intFormat;
    char charFormat[4];
};

union FLOAT{
    float floatFormat;
    char charFormat[4];
};


int RecordManager::getInt(Block &block, int startPos)
{
    INT info;
    info.charFormat[0] = block.data[startPos];
    info.charFormat[1] = block.data[startPos + 1];
    info.charFormat[2] = block.data[startPos + 2];
    info.charFormat[3] = block.data[startPos + 3];
    return info.intFormat;
}

void RecordManager::setInt(Block &block, int startPos, int num)
{
    INT info;
    info.intFormat = num;
    block.data[startPos] = info.charFormat[0];
    block.data[startPos + 1] = info.charFormat[1];
    block.data[startPos + 2] = info.charFormat[2];
    block.data[startPos + 3] = info.charFormat[3];
}

float RecordManager::getFloat(Block &block, int startPos)
{
    FLOAT info;
    info.charFormat[0] = block.data[startPos];
    info.charFormat[1] = block.data[startPos + 1];
    info.charFormat[2] = block.data[startPos + 2];
    info.charFormat[3] = block.data[startPos + 3];
    return info.floatFormat;
}

void RecordManager::setFloat(Block &block, int startPos, float num)
{
    FLOAT info;
    info.floatFormat = num;
    block.data[startPos] = info.charFormat[0];
    block.data[startPos + 1] = info.charFormat[1];
    block.data[startPos + 2] = info.charFormat[2];
    block.data[startPos + 3] = info.charFormat[3];
}

std::string RecordManager::getString(Block &block, int startPos)
{
    //NOTE: space for the string = user input + 1 !!
    char* str = (char*)block.data + startPos;
    std::string theString(str, strlen(str));
    return theString;
}

void RecordManager::setString(Block &block, int startPos, std::string str)
{
    strcpy((char *)block.data + startPos, str.c_str());
}

RecordManager::RecordManager(BufferManager & bm, IndexManager & im) :bm(bm), im(im)
{

}

void RecordManager::RecordManagerTableCreate(std::string dbName)
{
    Block firstBlock;
    firstBlock.fileName = dbName;
    firstBlock.offset = 0;
    setInt(firstBlock, BLOCKSIZE - 4, 0);
    bm.BufferManagerWrite(firstBlock);
}

void RecordManager::RecordManagerTableDetete(std::string dbName)
{
    bm.deleteFile(dbName);
}
/*TODO 3: 
insert a record into the database file: dbName.db
input dbName, data entries, and table information

HIT: 
block record structure
The size of each record = 1 byte (available:'Y',unavailable:'N') + entrySize

Use the last 4 byte in each record block to identify whether the block is the last:
If the block is the last, it is a long int identify the next available position.
Otherwise, it is BLOCKSIZE.
*/
void RecordManager::RecordManagerRecordInsert(std::string dbName, const std::vector<element> & entry, Table & nt)
{
    long offset = (nt.blockCount - 1) * BLOCKSIZE;
    bufferIter pBlock;
    int tupleSize = nt.entrySize + 1;
    // first get the block where the data entry should store, by using buffer manager 
    // then check if there is enough space for the data
    pBlock = bm.BufferManagerRead(dbName, offset);
    Block &block = *pBlock;
    int endPoint = getInt(block, BLOCKSIZE - 4);
    if (endPoint + tupleSize + 4 >= BLOCKSIZE) {
        // The block is full;
        //==========================================
        // YOUR CODE GOES HERE:
        // update the endpoint that the block is full, and create a new block to input record
        // remember to use buffer manager to write the new block
        // remember to update the table information. i.e. the nt.blockCount and store it.
        //==========================================
    }
    else {
        // The block is not full;
        //==========================================
        // YOUR CODE GOES HERE:
        // just input the record and update the endpoint 
        //==========================================
    }

    // update the index information by insert data into index
}

void RecordManager::InputRecord(Block & block, int tuplePos, const std::vector<element> & entry, Table & nt)
{
    int startPos = tuplePos;
    block.data[startPos] = 'Y';
    startPos++;
    int index = 0;
    for (auto ele : entry) {
        assert(nt.attributes[index].type == ele.type);
        switch (ele.type) {
            case 0:
                setInt(block, startPos, ele.i);
                startPos += sizeof(int);
                break;
            case 1:
                setFloat(block, startPos, ele.f);
                startPos += sizeof(float);
                break;
            case 2:
                setString(block, startPos, ele.s);
                startPos += nt.attributes[index].length;//Include +1
                break;
            default:
                break;
        }
        index++;
    }
}

/* TODO 4: 
delete record
input: dbName, offset, delete conditions, table information
use fake deletion, just mark the deleted entry to be unavailable.
*/
void RecordManager::RecordManagerRecordDelete(std::string dbName, long offset, const Filter & filter, Table & nt)
{
    bufferIter pBlock;
    std::vector<element> queryTuple;
    int tupleSize = nt.entrySize + 1;
    // first get the block according to data offset, by using buffer manager
    pBlock = bm.BufferManagerRead(dbName, offset);
    Block & block = *pBlock;
    int endPoint = getInt(block, BLOCKSIZE - 4);
    int tuple;
    int startPos;
    //==========================================
    //YOUR CODE GOES HERE:
    //go through the block and read the data tuple, check if it pass the filter.test(), i.e. meet the condition
    //if meet the conditions, mark the data tuple unavailable 
    //update index if necessary, to erase the tuple in index
    // HIT: borrow idea from function RecordManager::RecordManagerRecordSelect()
    //==========================================
}

std::vector<std::vector<element> > RecordManager::RecordManagerRecordSelect(std::string dbName, long offset, const Filter & filter, Table & nt)
{
    bufferIter pBlock;
    std::vector<std::vector<element> > queryResut;
    std::vector<element> queryTuple;
    int tupleSize = nt.entrySize + 1;

    pBlock = bm.BufferManagerRead(dbName, offset);
    Block & block = *pBlock;
    int endPoint = getInt(block, BLOCKSIZE - 4);
    int tuple;
    int startPos;
    for (tuple = 0; tuple <= endPoint - tupleSize; tuple += tupleSize) {
        if (block.data[tuple] == 'N')
            continue;
        startPos = tuple + 1;
        queryTuple.clear();
        for (auto attr : nt.attributes) {
            switch (attr.type) {
                case 0:
                    queryTuple.push_back(element(getInt(block, startPos)));
                    startPos += sizeof(int);
                    break;
                case 1:
                    queryTuple.push_back(element(getFloat(block, startPos)));
                    startPos += sizeof(float);
                    break;
                case 2:
                    queryTuple.push_back(element(getString(block, startPos)));
                    startPos += attr.length;//Include +1
                    break;
                default:
                    break;
            }
        }
        if (true == filter.test(queryTuple))
            queryResut.push_back(queryTuple);
    }
    return queryResut;
}

std::set<long> RecordManager::RecordManagerGetAllOffsets(std::string dbName, Table & nt)
{
    std::set<long> allOffsets;

    bufferIter pBlock;
    int endPoint;
    long offset = 0;
    do {
        pBlock = bm.BufferManagerRead(dbName, offset);
        Block & block = *pBlock;
        endPoint = getInt(block, BLOCKSIZE - 4);
        allOffsets.insert(offset);
        offset += BLOCKSIZE;
    } while (endPoint == BLOCKSIZE);
    return allOffsets;
}