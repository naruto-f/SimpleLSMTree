//
// Created by 123456 on 2023/3/9.
//

#ifndef SIMPLELSMTREE_MEMTABLE_H
#define SIMPLELSMTREE_MEMTABLE_H

#include <skiplist.h>
#include <string_view>


namespace lsmtree {

class MemTable {
public:
    MemTable();

    enum class Status : uint8_t {
        kSuccess = 0,
        kNotFound,
        kFound,
        kError,
        kFull
    };

//    enum : uint16_t {
//        kMaxNumsPerTable = 20001
//    };

    ///
    Status Add(const std::string& key, const std::string& value);

    ///
    Status Delete(const std::string& key);

    ///
    Status Search(const std::string& key, std::string_view& value) const;

    Status Dump(const std::string& filename, std::atomic<uint64_t>& block_id) const;

    ///
    bool IsFull() { return estimate_size_ >= 2 * 1024 * 1024; }   //2MB

    ///
    void Ref();

    void UnRef();

private:
    class KeyCompartor {
    public:
        int operator()(const std::string& lhs, const std::string& rhs) const {
            if(lhs == rhs) {
                return 0;
            } else if (lhs > rhs) {
                return 1;
            } else {
                return -1;
            }
        }
    };

    //uint32_t GetCurNums() const;
    void UpdateCurSize(const std::string& key, uint64_t add_size);

    using Table = SkipList<std::string, std::string, KeyCompartor>;

    ///destructed only no thread working on this memtable, don't allow destruct from extern.
    ~MemTable();

    Table table_;
    std::atomic<int> refs_;    //thread nums working on this memtable now
    //std::atomic<uint32_t> node_counts_;
    uint64_t estimate_size_;
    uint64_t cur_block_size_;
};

}  //namespace lsmtree

#endif //SIMPLELSMTREE_MEMTABLE_H
