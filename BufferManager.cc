#include "BufferManager.h"
#include <iostream>

using namespace std;

map <tag, bufferIter> BufferManager::inmemory_table;
list <Block> BufferManager::buffer;

/*TODO 2
input：1.filename 2.read start address(must be int*BLOCKSIZE)
HIT:
    check the data wanted exist in the memory or not, 
    if is, return the data
    if is not, load the data from the db file;
    when the buffer memory goes out,i.e., need to throw some other blocks to make space for the new block
    use the LRU(least recently used) schema.
return：bufferIter. 
*/
bufferIter BufferManager::BufferManagerRead(const string &fileName, long offset)
{
	tableIter it = inmemory_table.find(make_pair(fileName, offset));
	if (it != inmemory_table.end()){ //already in the buffer
        //YOUR CODE GOES HERE:
        //=============================
        //move the recent used block to be the last block in the list:buffer to support LRU
        //update the inmemory_table
        //then return the block
        //=============================
            bufferIter bit = it -> second;
            tableIter tmp = it;

            for(; (tmp + 1) != inmemory_table.end(); tmp ++){
                tableIter next = tmp + 1;
                tmp -> first = next -> first;
                tmp -> second = next -> second;
            }

            tmp -> first = it -> first;
            tmp -> second = it -> second;

            return bit;
	}
	else{ //not in the buffer
        //YOUR CODE GOES HERE:
        //=============================
        //read the block from the file, and push it into the list:buffer at the end for the LRU.
        //update the inmemory_table
        //then return the block
        //=============================
            FILE* fp = fopen(fileName.c_str(), "rb");
            if(fp == NULL){
                cerr <<"ERROR: Read File " << fileName << "failed." << endl;
                return;
            }

            Block block;
            block.filename = filename;
            block.offset = offset;

            fseek(fp, offset, SEEK_SET);
            fread(block.data, 1, BLOCKSIZE, fp);
            fclose(fp);

            if (buffer.size()>BUFFERSIZE){ //we should use LRU replace
            // YOUR CODE GOES HERE:
            //=============================
            // find the fist unpin block in the list:buffer, this should be least recent used according to operation above.
            // make sure its status allow it to be replace, if not find the block behind, until it meets the condition
            //=============================    
              for(tableIter tit = inmemory_table.begin(); tit != inmemory_table.end(); tit++){
                if(tit->second->status){
                    continue;
                }

                tableIter tmp = tit;
                for(; (tmp + 1) != inmemory_table.end(); tmp ++){
                    tableIter next = tmp + 1;
                    tmp -> first = next -> first;
                    tmp -> second = next -> second;
                }

                List<Block> bufferList;
                bufferList.insert(make_pair(make_pair(filename, offset), block));
                tmp -> first = make_pair(filename, offset);
                tmp -> second = bufferList.begin();

                break;
              }
            } else{
                List<Block> bufferList;
                bufferList.insert(make_pair(make_pair(filename, offset), block));
                inmemory_table.insert(make_pair(make_pair(filename, offset), bufferList.begin()));
            }
            return block;
        // return block of data;
	}
}

void BufferManager::BufferManagerPin(Block &b)
{
    b.status = 1;
}

void BufferManager::BufferManagerWrite(const Block &b)
{
    FILE *fp = fopen(b.fileName.c_str(), "rb");
    FILE *outFile;
    if (fp==NULL){
        outFile = fopen(b.fileName.c_str(), "wb");
    }
    else{
        fclose(fp);
        outFile = fopen(b.fileName.c_str(), "rb+");
    }
    if(outFile==NULL){
        cerr<<"ERROR: Open file "<<b.fileName<<" failed.\n";
        return;
    }
    fseek(outFile, b.offset, SEEK_SET);
    fwrite(&b.data, 1, BLOCKSIZE, outFile);
    fclose(outFile);

    tag T = make_pair(b.fileName, b.offset);
    tableIter tmp = inmemory_table.find(T);
    if (tmp!=inmemory_table.end()){ //in the buffer, which means not new written
        bufferIter victim = tmp->second;
        inmemory_table.erase(inmemory_table.find(T)); //remove from table
        buffer.erase(victim); //remove from buffer
    }

}

int BufferManager::BufferManagerGetStatus(const Block &b)
{
    return b.status;
}

void BufferManager::BufferManagerFlush()
{
    for (bufferIter T = buffer.begin(); T!=buffer.end();){
        BufferManagerWrite(*T++);
    }
    buffer.clear();
    inmemory_table.clear();
}

void BufferManager::deleteFile(const string &fileName)
{
    tableIter tIt = inmemory_table.lower_bound(make_pair(fileName,0));
    for (; tIt!=inmemory_table.end() && tIt->first.first==fileName; ){
        BufferManagerWrite(*(tIt++->second));
    }
    cout << remove(fileName.c_str()) << " " <<fileName<<endl;
}
