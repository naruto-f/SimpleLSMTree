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
#include <list>
#include <atomic>
#include <unordered_set>


namespace lsmtree {


class Block;
class MemTable;
struct MergeNode;

class LsmTree : public Db {
public:
    LsmTree(uint32_t line_cache_capacity, uint32_t block_cache_capacity);

    void Add(const std::string& key, const std::string& value) override;

    void Delete(const std::string& key) override;

    bool Get(const std::string& key, std::shared_ptr<std::string>& value) override;

private:
    void BackgroundWorkerThreadEntry();

    void MemtableChange();

    bool MergeMemtableToDisk(MemTable* table);

    bool CheckAndDownSstableIfPossible();

    void CheckSstableInfo() const;

    ///range is [start_index, end_index]
    bool MergeRangeOfSstable(int level, int start_index, int end_index);

    std::list<MergeNode> MergeTwoSstable(int level, int older_index, int newer_index, std::vector<Block*>& alloc_blocks);

    std::list<MergeNode> MergeTwoMergerdList(std::list<MergeNode>& older_list, std::list<MergeNode>& newer_list);

    bool DumpToNextLevel(int level, std::list<MergeNode>& list);

    bool DumpToOneFile(const std::string &filename, std::list<MergeNode>::const_iterator& iter);

    void TryBestDoDelayDelete();
private:
    using CacheId_t = uint64_t;
    using L1Cache = Cache<std::string, std::shared_ptr<std::string>>;
    using L2Cache = Cache<CacheId_t, std::shared_ptr<Block>>;

    SimpleBloomFilter* bloom_;
    MemTable* table_;
    std::deque<MemTable*> background_tables_;
    L1Cache line_cache_;
    L2Cache block_cache_;
    std::atomic<CacheId_t> next_block_id_;      //only increase num until touch Max uint64_t
    std::thread background_worker;
    std::condition_variable cond_;
    std::unordered_set<std::string> file_need_delete_;
    mutable std::mutex table_mutex_;
    mutable std::mutex queue_mutex_;
};

};  //namespace lsmtree

#endif //SIMPLELSMTREE_LSMTREE_H
