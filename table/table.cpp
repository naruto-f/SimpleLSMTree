//
// Created by 123456 on 2023/3/10.
//

#include "table.h"
#include <reader.h>
#include <block.h>
#include <cache.h>

#include <cassert>
#include <memory>


namespace {
    Reader reader_;


}

lsmtree::SSTable::SSTable(const char *filename, L2Cache* cache) : cache_(cache) {
    std::ifstream& reader = reader_.GetReader();

    reader.open(filename, std::ios::in | std::ios::binary);
    if (reader) {
        ReadMetaData();
        ReadDataIndexBlock();
    }
}

bool lsmtree::SSTable::Valid() const {
    std::ifstream& reader = reader_.GetReader();
    return reader && !reader.fail();
}

void lsmtree::SSTable::ReadMetaData() {
    std::ifstream& reader = reader_.GetReader();

    reader.seekg(-8, std::ios::end);
    reader.read(reinterpret_cast<char*>(&footer_.magic_num_), 8);
    if (!Valid() || footer_.magic_num_ != 0x87654321) {
        return;
    } else {
        reader.seekg(-24, std::ios::end);
        reader.read(reinterpret_cast<char*>(&footer_.data_index_handle_.offset_), 8);
        reader.read(reinterpret_cast<char*>(&footer_.data_index_handle_.size_), 8);
    }
}

void lsmtree::SSTable::ReadDataIndexBlock() {
    std::ifstream& reader = reader_.GetReader();
    if (!Valid()) {
       return;
    }

    reader.seekg(footer_.data_index_handle_.offset_, std::ios::beg);

    IndexHandler data_index_handler;
    while (reader.tellg() < footer_.data_index_handle_.offset_ + footer_.data_index_handle_.size_) {
        //read block_id(same as cache_id)
        reader.read(reinterpret_cast<char*>(data_index_handler.id_), 8);

        int key_size = 0;
        reader.read(reinterpret_cast<char*>(&key_size), 8);
        assert(key_size != 0);

        data_index_handler.key_.resize(key_size);
        //data_index_handler.key_ = new char[key_size];
        reader.read(data_index_handler.key_.data(), key_size);

        reader.read(reinterpret_cast<char*>(data_index_handler.block_handle_.offset_), 8);
        reader.read(reinterpret_cast<char*>(data_index_handler.block_handle_.size_), 8);

        data_index_block_.push_back(data_index_handler);
    }
}

int lsmtree::SSTable::Get(const std::string_view &key, std::shared_ptr<std::string>& value) const {
    int block_index = GetBlockIndexOfKeyMayIn(key);
    if (block_index == -1) {
        return -1;
    }

    int res = -1;
    std::string_view value_view(nullptr);
    std::shared_ptr<Block> block_ptr(nullptr);
    if (cache_->Search(data_index_block_[block_index].id_, block_ptr)) {
        res = block_ptr->Get(key, value_view);
        if (res == 0) {
            value = std::make_shared<std::string>(value_view.data(), value_view.size());
        }

        return res;
    }

    uint64_t block_size = data_index_block_[block_index].block_handle_.size_;
    uint64_t block_offset = data_index_block_[block_index].block_handle_.offset_;
    char* block_begin = new char[block_size];

    std::ifstream& reader = reader_.GetReader();

    reader.seekg(block_offset, std::ios::beg);
    reader.read(block_begin, block_size);
    assert(Valid());

    std::shared_ptr<Block> new_block_ptr = std::make_shared<Block>(data_index_block_[block_index].id_, block_begin, block_size);
    cache_->Insert(data_index_block_[block_index].id_, block_ptr);

    res = block_ptr->Get(key, value_view);
    if (res == 0) {
        value = std::make_shared<std::string>(value_view.data(), value_view.size());
    }

    return res;
}

int lsmtree::SSTable::GetBlockIndexOfKeyMayIn(const std::string_view &key) const {
    std::size_t left = 0, right = data_index_block_.size();

    while (left < right) {
        auto mid = left + (right - left) / 2;
        if (data_index_block_[mid].key_ < key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    return left == data_index_block_.size() ? -1 : left;
}


