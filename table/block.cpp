//
// Created by 123456 on 2023/3/11.
//

#include "block.h"

#include <cassert>

int lsmtree::Block::PraseBlock() {
    if (!raw_data_) {
       return -1;
    }

    char* cur = raw_data_;
    uint64_t cur_offset = 0;
    uint64_t cur_index = 0;
    while (cur_offset < block_size_) {
        key_value_info_.push_back(cur);

        ++cur;
        uint64_t key_size = *reinterpret_cast<uint64_t*>(cur);
        cur += sizeof(uint64_t);

        key_to_index_[std::string_view{cur, key_size}] = cur_index++;
        cur += key_size;

        uint64_t value_size = *reinterpret_cast<uint64_t*>(cur);
        cur += sizeof(uint64_t);
        cur += value_size;

        cur_offset += (1 + 2 * sizeof(uint64_t) + key_size + value_size);
    }

    return 0;
}

int lsmtree::Block::Get(const std::string_view& key, std::string_view& value) const {
    if (!key_to_index_.count(key)) {
        return -1;
    }

    std::size_t pos = key_to_index_.find(key)->second;
    char* key_value_info = key_value_info_[pos];

    if (*key_value_info == 1) {
       return -2;
    }
    key_value_info += (1 + sizeof(uint64_t) + key.size());

    uint64_t value_size = *reinterpret_cast<uint64_t*>(key_value_info);
    key_value_info += sizeof(uint64_t);

    value = std::string_view(key_value_info, value_size);
    return 0;
}
