//
// Created by 123456 on 2023/3/10.
//

#ifndef SIMPLELSMTREE_TABLE_H
#define SIMPLELSMTREE_TABLE_H

#include <table_format.h>
#include <reader.h>
#include <vector>
#include <memory>
#include <block.h>
#include <cache.h>

namespace lsmtree {

class Block;

class SSTable {
    using CacheId_t = uint64_t;
    using L2Cache = Cache<CacheId_t, std::shared_ptr<Block>>;

public:
    class Iterator {
    public:
        Iterator(SSTable* table) : table_(table), cur_index_(0), cur_block_(nullptr) {
            ChangeBlock(cur_index_);
        }

        bool Valid() const {
            return table_ && cur_index_ < table_->data_index_block_.size() && cur_block_;
        }

        void Next() {
            ++cur_index_;
            if (Valid()) {
                ChangeBlock(cur_index_);
            } else {
                cur_block_ = nullptr;
            }
        }

        Block::Iterator GetBlockIterator() {
            assert(Valid());
            return Block::Iterator(cur_block_);
        }

        std::vector<Block*>& GetAllocBlockAdress() {
            return block_ptr_vec;
        }

    private:
        void ChangeBlock(uint64_t index) {
            uint64_t block_size = table_->data_index_block_[cur_index_].block_handle_.size_;
            uint64_t block_offset = table_->data_index_block_[cur_index_].block_handle_.offset_;
            char* block_begin = new char[block_size];
            auto& reader = table_->reader_.GetReader();
            reader.seekg(block_offset, std::ios::beg);
            reader.read(block_begin, block_size);
            assert(reader && reader.good());

            cur_block_ = new Block(table_->data_index_block_[cur_index_].id_, block_begin, block_size);
            block_ptr_vec.push_back(cur_block_);
        }

    private:
        SSTable* table_;
        uint64_t cur_index_;
        Block* cur_block_;
        std::vector<Block*> block_ptr_vec;
    };

public:
    SSTable(const char* filename, L2Cache* cache);

    int Get(const std::string_view& key, std::shared_ptr<std::string>& value);

    bool Valid();

private:
    void ReadMetaData();

    void ReadDataIndexBlock();

    ///find the block index key may in use Binary search
    int GetBlockIndexOfKeyMayIn(const std::string_view& key) const;

private:
    struct Footer footer_;
    std::vector<IndexHandler> data_index_block_;
    L2Cache* cache_;
    Reader reader_;
};

}  //namespace lsmtree

#endif //SIMPLELSMTREE_TABLE_H
