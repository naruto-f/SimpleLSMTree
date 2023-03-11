//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_BLOCK_H
#define SIMPLELSMTREE_BLOCK_H

#include <memory>
#include <unordered_map>
#include <string_view>
#include <vector>

namespace lsmtree {

class Block {
public:
    Block(char* raw_data, uint64_t block_size) : raw_data_(raw_data), block_size_(block_size) {
        PraseBlock();
    }

    ~Block() { delete[] raw_data_; }

    ///return 0 for find success, return -1 for not found, return -2 for is already deleted.
    int Get(const std::string_view& key, std::string_view& value) const;
private:
    ///return 0 for prase success, return -1 for raw_data_ is nullptr.
    int PraseBlock();

private:
    uint64_t block_size_;
    char* raw_data_;
    std::vector<char*> key_value_info_;
    std::unordered_map<std::string_view, std::size_t> key_to_index_;
};

}  //namespace lsmtree


#endif //SIMPLELSMTREE_BLOCK_H
