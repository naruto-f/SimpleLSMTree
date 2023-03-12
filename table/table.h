//
// Created by 123456 on 2023/3/10.
//

#ifndef SIMPLELSMTREE_TABLE_H
#define SIMPLELSMTREE_TABLE_H

#include <table_format.h>

#include <vector>
#include <memory>
#include <cache.h>

namespace lsmtree {

class Block;

class SSTable {
    using CacheId_t = uint64_t;
    using L2Cache = Cache<CacheId_t, std::shared_ptr<Block>>;
public:
    SSTable(const char* filename, L2Cache* cache);

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
    L2Cache* cache_;
};

}  //namespace lsmtree

#endif //SIMPLELSMTREE_TABLE_H
