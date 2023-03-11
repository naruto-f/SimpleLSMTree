//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_CACHE_H
#define SIMPLELSMTREE_CACHE_H

#include <memory>
#include <string_view>


namespace lsmtree {

class Block;

///the memory cache use LRU
class Cache {
public:
    void Insert(std::shared_ptr<Block> block_ptr);

    void Search(const std::string_view& key, std::string_view& value) const;
private:

};

};  //namespace lsmtree

#endif //SIMPLELSMTREE_CACHE_H
