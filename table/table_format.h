//
// Created by 123456 on 2023/3/10.
//

#ifndef SIMPLELSMTREE_TABLE_FORMAT_H
#define SIMPLELSMTREE_TABLE_FORMAT_H

#include <cstdint>
#include <string>



namespace lsmtree {




/* 16 bytes */
struct BlockHandler {
    uint64_t offset_;   //file ptr offset
    uint64_t size_;
};


struct IndexHandler {
    BlockHandler block_handle_;
    uint64_t id_;
    std::string key_;
};

//struct IndexHandler {
//    BlockHandler data_block_handle_;
//    const char* key_;
//};

struct MetaIndexBlock {
    IndexHandler* meta_block_handle_;
};

struct DataIndexBlock {
    IndexHandler* data_block_handle_;
};


/* 24 bytes */
struct Footer {
    //BlockHandler meta_index_handle_;
    BlockHandler data_index_handle_;
    uint64_t magic_num_;
};




}  //namespace lsmtree
#endif //SIMPLELSMTREE_TABLE_FORMAT_H
