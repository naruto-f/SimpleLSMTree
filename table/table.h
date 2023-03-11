//
// Created by 123456 on 2023/3/10.
//

#ifndef SIMPLELSMTREE_TABLE_H
#define SIMPLELSMTREE_TABLE_H

#include <table_format.h>

#include <vector>

namespace lsmtree {

class Cache;

class SSTable {
public:
    SSTable(const char* filename, Cache* cache);

    int Get(const std::string_view& key, std::string_view& value) const;

    bool Valid() const;

private:
    void ReadMetaData();

    void ReadDataIndexBlock();

    ///find the block index key may in use Binary search
    int GetBlockIndexOfKeyMayIn(const std::string_view& key) const;


private:
    struct Footer footer_;
    std::vector<IndexHandler> data_index_block_;
    Cache* cache_;
};

}  //namespace lsmtree

#endif //SIMPLELSMTREE_TABLE_H
