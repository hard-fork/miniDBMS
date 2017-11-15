#ifndef _BUFFERMANAGER_H
#define _BUFFERMANAGER_H

#define BLOCKSIZE 4096
#define BUFFERSIZE 1024

#include <list>
#include <string>
#include <cstdio>
#include <map>
#include <cstring>
#include <iostream>
#include <cstdlib>

class BufferManager;

struct Block{
public:
    Block():offset(0){
        memset(data, 0, sizeof(data));
    }
	//data part
	unsigned char data[BLOCKSIZE];
	//which file's block
	std::string fileName;
	//start address
	long offset;
private:
    friend BufferManager;
	int status;
	//block status 0-normal 1-pin
};

typedef std::list<Block>::iterator bufferIter;
typedef std::pair <std::string, long> tag;
typedef std::map <tag, bufferIter>::iterator tableIter;

class BufferManager{
public:
	bufferIter BufferManagerRead(const std::string &fileName, long offset);

	void BufferManagerPin(Block &b);
	/*
		input：the block to be locked
		change the blocked status, forbid it to replace, can be force to write back.
	*/
	void BufferManagerWrite(const Block &b);
	/*
		input：block
		write the block data into hard disk. i.e. filename.db
	*/
	int  BufferManagerGetStatus(const Block &b);
	/*
		input: block
		return ：0--normal(can be replace)
		      1--locked(can not be replace if not be write back first
	*/
	void BufferManagerFlush();
	/*
		force to write back all data in the buffer.
	*/
	void deleteFile(const std::string &fileName);
	/*
        input: filename
        write back all data relate to that file, then delete the file.db.
	*/
private:
	static std::list <Block> buffer;
	static std::map <tag, bufferIter> inmemory_table;
};

#endif
