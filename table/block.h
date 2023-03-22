//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_BLOCK_H
#define SIMPLELSMTREE_BLOCK_H

#include <memory>
#include <unordered_map>
#include <string_view>
#include <vector>
#include <cassert>

namespace lsmtree {

class Block {
public:
    class Iterator {
    public:
        Iterator(Block* block) : block_(block), cur_index(0) { }

        void Next() {
            ++cur_index;
        }

        char GetFlag() {
            assert(Valid());
            return block_->GetFlagByIndex(cur_index);
        }

        std::string_view GetKey() {
            assert(Valid());
            return block_->GetKeyByIndex(cur_index);
        }

        std::string_view GetValue() {
            assert(Valid());
            return block_->GetValueByIndex(cur_index);
        }

        bool Valid() {
            return block_ && cur_index < block_->key_value_info_.size();
        }

        uint64_t KeySize() {
            assert(Valid());
            return block_->key_value_info_.size();
        }
    private:
        Block* block_;
        uint32_t cur_index;
    };

public:
    Block(uint64_t block_id, char* raw_data, uint64_t block_size) : id_(block_id), raw_data_(raw_data), block_size_(block_size) {
        PraseBlock();
    }

    ~Block() { delete[] raw_data_; }

    ///return 0 for find success, return -1 for not found, return -2 for is already deleted.
    int Get(const std::string_view& key, std::string_view& value) const;

private:
    ///return 0 for prase success, return -1 for raw_data_ is nullptr.
    int PraseBlock();

    char GetFlagByIndex(int index);

    std::string_view GetKeyByIndex(int index);

    std::string_view GetValueByIndex(int index);

private:
    uint64_t id_;
    uint64_t block_size_;
    char* raw_data_;
    std::vector<char*> key_value_info_;
    std::unordered_map<std::string_view, std::size_t> key_to_index_;
};

}  //namespace lsmtree


#endif //SIMPLELSMTREE_BLOCK_H
