//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_LSMTREE_H
#define SIMPLELSMTREE_LSMTREE_H

#include <db.h>
#include <cache.h>

#include <thread>
#include <condition_variable>
#include <mutex>
#include <deque>
#include <bloom.h>

namespace lsmtree {


class Block;
class MemTable;

class LsmTree : public Db {
public:
    LsmTree();

    void Add(const std::string& key, const std::string& value) override;

    void Delete(const std::string& key) override;

    bool Get(const std::string& key, std::shared_ptr<std::string>& value) override;

private:
    void BackgroundWorkerThreadEntry();

    void MemtableChange();

    void MergeMemtableToDisk(MemTable* table);


private:
    using CacheId_t = uint64_t;
    using L1Cache = Cache<std::string, std::shared_ptr<std::string>>;
    using L2Cache = Cache<CacheId_t, std::shared_ptr<Block>>;

    SimpleBloomFilter* bloom_;
    MemTable* table_;
    std::deque<MemTable*> background_tables_;
    L1Cache line_cache_;
    L2Cache block_cache_;
    CacheId_t next_block_id_;      //only increase num
    std::thread background_worker;
    std::condition_variable cond_;
    mutable std::mutex table_mutex_;
    mutable std::mutex queue_mutex_;
};

};  //namespace lsmtree

#endif //SIMPLELSMTREE_LSMTREE_H
