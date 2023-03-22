//
// Created by 123456 on 2023/3/11.
//

#include "lsmtree.h"
#include <memtable.h>
#include <file.h>
#include <table.h>
#include <atomic>
#include <filesystem>
#include <writer.h>
#include <iostream>
#include <filelock.h>

namespace {
    const char* dir_name = "/home/naruto/StudyDir/SimpleLSMTree/storage/";
    const char* config_dir_name = "/home/naruto/StudyDir/SimpleLSMTree/config/";
    const std::string filename_prefix = "sstable";

    const uint32_t SstableNumsPerLevel[] = {8, 80, 800, 8000, 80000};

    enum {
        kMaxLevel = 4,
        kMaxLenApproximatePerSstable = 2 * 1024 * 1024
    };

    enum UpdateType : uint8_t {
        kAdd = 0,
        kDelete = 1
    };

    std::atomic<uint32_t> log_suffix_max;
    std::atomic<uint32_t> log_suffix_min;

    std::mutex level_files_mutex;
    uint32_t level_file_nums[kMaxLevel + 1] = { 0 };
    uint64_t level_suffix_max[kMaxLevel + 1] = { 0 };
    uint64_t level_suffix_min[kMaxLevel + 1] = { 0 };
    std::unordered_map<std::string, uint64_t> level_file_refs;

    bool TestFileCanDelete(const std::string& filename) {
        std::filesystem::path path(filename);

        if (std::filesystem::exists(path)) {
//            Writer writer;
//            auto& write = writer.GetWriter();
//
//            FileLock flock(dynamic_cast<std::fstream&>(write), F_WRLCK);
//            if (!flock.LockWithoutWait()) {
//                return false;
//            }
            std::filesystem::remove(path);
        }
        //std::filesystem::remove(path);

        return true;
    }
}

struct lsmtree::MergeNode {
    std::string_view key;
    std::string_view value;
    char flag;
};

bool lsmtree::LsmTree::Get(const std::string& key, std::shared_ptr<std::string>& value) {
    if (!Valid()) {
        return false;
    }

    ///①Throught bloom filter first
    if (!bloom_->MayExist(key)) {
        return false;
    }


    ///②lookup at L1Cache second
    if (line_cache_.Search(key, value)) {
        return true;
    }


    ///③lookup at cur main SkipList
    std::string_view value_view;
    MemTable* table = nullptr;
    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        table = table_;
        assert(table);
        table->Ref();
    }

    MemTable::Status status = table->Search(key, value_view);
    if (status == MemTable::Status::kSuccess) {
        value = std::make_shared<std::string>(value_view.data(), value_view.size());
        line_cache_.Insert(key, value);
        table->UnRef();
        return true;
    } else if (status == MemTable::Status::kFound) {
        table->UnRef();
        return false;
    }
    table->UnRef();


    ///④lookup at tables that await for presist to disk
    std::vector<MemTable*> background_tables;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto riter = background_tables_.rbegin(); riter != background_tables_.rend(); ++riter) {
            background_tables.push_back(*riter);
            (*riter)->Ref();
        }
    }

    for (auto table : background_tables) {
        status = table->Search(key, value_view);
        if (status == MemTable::Status::kSuccess) {
            value = std::make_shared<std::string>(value_view.data(), value_view.size());
            line_cache_.Insert(key, value);
            break;
        } else if (status == MemTable::Status::kFound) {
            break;
        }
    }

    for (auto table : background_tables) {
        table->UnRef();
    }

    if (status == MemTable::Status::kSuccess) {
        return true;
    } else if (status == MemTable::Status::kFound) {
        return false;
    }


    ///⑤Binary search block index that key may exist, foreach sstable from new to old, and lookup at L2Cache if possible.
    std::pair<int64_t, int64_t> range_of_file_pre_level[kMaxLevel + 1] = {{0, 0}};
    std::unordered_set<std::string> need_locked_files;
    {
        std::lock_guard<std::mutex> lock(level_files_mutex);
        for (int i = 0; i <= kMaxLevel; ++i) {
            if(level_file_nums[i] == 0) {
                continue;
            }

            range_of_file_pre_level[i] = {level_suffix_min[i], level_suffix_max[i]};
            for (int j = level_suffix_min[i]; j < level_suffix_min[i] + std::min(SstableNumsPerLevel[0], level_file_nums[i]); ++j) {
                std::string file_tag = std::to_string(i) + ":" + std::to_string(j);
                need_locked_files.insert(file_tag);
                ++level_file_refs[file_tag];
            }
        }
    }

    int res = -1;
    for (int i = 0; i <= kMaxLevel; ++i) {
        if (res != -1) {
            break;
        }

        if (range_of_file_pre_level[i].second == range_of_file_pre_level[i].first) {
            continue;
        }

        for (int j = range_of_file_pre_level[i].second - 1; j >= range_of_file_pre_level[i].first; --j) {
            std::string filename = dir_name + std::string("level" + std::to_string(i) + "/") + filename_prefix + std::to_string(j);

            SSTable cur_table(filename.c_str(), &block_cache_);
            assert(cur_table.Valid());

            res = cur_table.Get(std::string_view(key.data(), key.size()), value);
            if (res == 0 || res == -2) {
                break;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(level_files_mutex);
        for (auto file_tag : need_locked_files) {
            if (--level_file_refs[file_tag] == 0) {
                level_file_refs.erase(file_tag);
            }
        }

//        for (int i = 0; i <= kMaxLevel; ++i) {
//            range_of_file_pre_level[i] = {level_suffix_min[i], level_suffix_max[i]};
//            for (int j = level_suffix_min[i]; j < level_suffix_min[i] + SstableNumsPerLevel[0]; ++j) {
//                std::string file_tag = std::to_string(i) + ":" + std::to_string(j);
//                if (--level_file_refs[file_tag] == 0) {
//                    level_file_refs.erase(file_tag);
//                }
//            }
//        }
    }

    // TODO: L1Cache add target that tag node is added or deleted
    if (res == 0) {
        line_cache_.Insert(key, value);
        return true;
    }

    return false;
}

bool lsmtree::LsmTree::Add(const std::string &key, const std::string &value) {
    if (!Valid()) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        if (table_->IsFull()) {
            MemtableChange();
        }

        Log(UpdateType::kAdd, key, value);
        table_->Add(key, value);
        bloom_->Insert(key);
    }

    line_cache_.Insert(key, std::make_shared<std::string>(value));
    return true;
}

bool lsmtree::LsmTree::Delete(const std::string &key) {
    if (!Valid()) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        if (table_->IsFull()) {
            MemtableChange();
        }

        Log(UpdateType::kAdd, key, "");
        table_->Delete(key);
        bloom_->Insert(key);
    }

    line_cache_.Insert(key, nullptr);
    return true;
}

void lsmtree::LsmTree::BackgroundWorkerThreadEntry() {
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        cond_.wait(lock, [this]() {
            return !background_tables_.empty() || !Valid();
        });

        while (!background_tables_.empty()) {
            auto table = background_tables_.front();
            background_tables_.pop_front();
            lock.unlock();

            //TODO: how to recover from disk write failures or low space
            assert(MergeMemtableToDisk(table));  //if return false, this storage system is not in Consistent state, now we stop system immediately.
            table->UnRef();

            lock.lock();
        }

        TryBestDoDelayDelete();

        if (!Valid()) {
            break;
        }
    }

    while (!file_need_delete_.empty()) {
        TryBestDoDelayDelete();
    }
}

void lsmtree::LsmTree::MemtableChange() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        background_tables_.push_back(table_);

        log_suffix_max.fetch_add(1, std::memory_order_acq_rel);

        MemTable* new_table = new MemTable();
        assert(new_table);

        table_ = new_table;
        cond_.notify_all();
    }
}

bool lsmtree::LsmTree::MergeMemtableToDisk(MemTable* table) {
    bool res = CheckAndDownSstableIfPossible();
    if (!res) {
        std::cerr << "Lack of space or Write failed!" << std::endl;
        return false;
    }

    std::string filename = dir_name + std::string("level0/") + filename_prefix + std::to_string(level_suffix_max[0]);
    table->Dump(filename, next_block_id_);

    {
        std::lock_guard<std::mutex> lock(level_files_mutex);
        ++level_suffix_max[0];
        ++level_file_nums[0];
    }

    /// log can be deleted when it own memtable's data persist to disk
    assert(log_suffix_min <= log_suffix_max);
    std::string log_filename = config_dir_name + std::string("log/") + std::to_string(log_suffix_min) + ".log";
    std::filesystem::path path(log_filename);
    if (std::filesystem::exists(path)) {
        std::filesystem::remove(path);
    }

    if (log_suffix_min < log_suffix_max) {
        ++log_suffix_min;
    }

    return true;
}

bool lsmtree::LsmTree::CheckAndDownSstableIfPossible() {
    if (level_file_nums[0] < SstableNumsPerLevel[0]) {
        return true;
    } else {
        for (int i = 1; i <= kMaxLevel; ++i) {
            if (level_file_nums[i] + SstableNumsPerLevel[0] > SstableNumsPerLevel[i]) {
                if (i == kMaxLevel) {
                    return false;
                }
            } else {
                for (int j = 0; j < i; ++j) {
                    MergeRangeOfSstable(i - 1 - j, level_suffix_min[i - 1 - j], level_suffix_min[i - 1 - j] + SstableNumsPerLevel[0] - 1);
                }
                break;
            }
        }
    }

    return true;
}

lsmtree::LsmTree::LsmTree(uint32_t line_cache_capacity, uint32_t block_cache_capacity)
                          : valid_flag_(false),
                            bloom_(new SimpleBloomFilter(10000000, 3)),
                            table_(new MemTable()),
                            line_cache_(line_cache_capacity),
                            block_cache_(block_cache_capacity) {
    //CheckSstableInfo();
    //table_->Ref();
    ReadMetaInfo();
    assert(Valid());

    ///running backgroud worker thread
    background_worker = std::move(std::thread(&LsmTree::BackgroundWorkerThreadEntry, this));

    CheckLogAndRedoIfPossible();
    assert(Valid());

    std::cout << "db is runing!" << std::endl;
}

void lsmtree::LsmTree::CheckSstableInfo() const {
    for (int i = 0; i <= kMaxLevel; ++i) {
        std::string cur_level_dirname = dir_name + std::string("level") + std::to_string(i) + "/";
        std::filesystem::path cur_level_path(cur_level_dirname.c_str());

        std::filesystem::directory_entry cur_level(cur_level_path);
        for (auto file : std::filesystem::directory_iterator(cur_level)) {
            ++level_file_nums[i];

            unsigned long suffix = std::stoul(file.path().filename().string().substr(7));
            level_suffix_max[i] = std::max(level_suffix_max[i], suffix);
            level_suffix_min[i] = std::min(level_suffix_min[i], suffix);
        }
    }
}

bool lsmtree::LsmTree::MergeRangeOfSstable(int level, int start_index, int end_index) {
    assert(end_index - start_index + 1 == SstableNumsPerLevel[0]);

    std::vector<Block*> all_alloc_block_ptrs;
    std::vector<std::list<lsmtree::MergeNode>> lists;

    int epoch = SstableNumsPerLevel[0] / 2;   //merge level0 max-file-nums files every time

    for (int i = 0; i < epoch; ++i) {
        lists.push_back(MergeTwoSstable(level, start_index + 2 * i, start_index + 2 * i + 1, all_alloc_block_ptrs));
    }

    std::list<lsmtree::MergeNode> older_list = MergeTwoMergerdList(lists[0], lists[1]);
    std::list<lsmtree::MergeNode> newer_list = MergeTwoMergerdList(lists[2], lists[3]);
    std::list<lsmtree::MergeNode> last_list = MergeTwoMergerdList(older_list, newer_list);
    assert(older_list.size() + newer_list.size() == last_list.size());

    bool res = DumpToNextLevel(level, last_list);
    for (auto block_ptr : all_alloc_block_ptrs) {
        delete block_ptr;
    }

    ///delay delete
    for (int i = start_index; i <= end_index; ++i) {
        file_need_delete_.insert(std::to_string(level) + ":" + std::to_string(i));
    }

    return res;
}

std::list<lsmtree::MergeNode> lsmtree::LsmTree::MergeTwoSstable(int level, int older_index, int newer_index, std::vector<Block*>& alloc_blocks) {
    std::string older_table_name = dir_name + std::string("level") + std::to_string(level) + "/sstable" + std::to_string(older_index);
    std::string newer_table_name = dir_name + std::string("level") + std::to_string(level) + "/sstable" + std::to_string(newer_index);

    lsmtree::SSTable older_table(older_table_name.c_str(), nullptr), newer_table(newer_table_name.c_str(), nullptr);
    lsmtree::SSTable::Iterator older_iter(&older_table), newer_iter(&newer_table);
    lsmtree::Block::Iterator older_block_iter = older_iter.GetBlockIterator(), newer_block_iter = newer_iter.GetBlockIterator();
    older_iter.Next();
    newer_iter.Next();

    std::vector<Block*>& old_blocks = older_iter.GetAllocBlockAdress();
    std::vector<Block*>& new_blocks = newer_iter.GetAllocBlockAdress();

    std::list<lsmtree::MergeNode> merge_list;

    while (newer_block_iter.Valid() && older_block_iter.Valid()) {
        std::string_view older_key = older_block_iter.GetKey();
        std::string_view newer_key = newer_block_iter.GetKey();

        struct MergeNode node;
        if (older_key == newer_key) {
            node.key = newer_key;
            node.value = newer_block_iter.GetValue();
            node.flag = newer_block_iter.GetFlag();
            merge_list.push_back(node);

            older_block_iter.Next();
            newer_block_iter.Next();
        } else if (older_key > newer_key) {
            node.key = newer_key;
            node.value = newer_block_iter.GetValue();
            node.flag = newer_block_iter.GetFlag();
            merge_list.push_back(node);

            newer_block_iter.Next();
        } else {
            node.key = older_key;
            node.value = older_block_iter.GetValue();
            node.flag = older_block_iter.GetFlag();
            merge_list.push_back(node);

            older_block_iter.Next();
        }

        if (!newer_block_iter.Valid()) {
            if (!newer_iter.Valid()) {
                break;
            } else {
                newer_block_iter = newer_iter.GetBlockIterator();
                newer_iter.Next();
            }
        }

        if (!older_block_iter.Valid()) {
            if (!older_iter.Valid()) {
                break;
            } else {
                older_block_iter = older_iter.GetBlockIterator();
                older_iter.Next();
            }
        }
    }

    if (older_block_iter.Valid()) {
        while (older_iter.Valid() || older_block_iter.Valid()) {
            if (!older_block_iter.Valid() && older_iter.Valid()) {
                older_block_iter = older_iter.GetBlockIterator();
                older_iter.Next();
            }

            struct MergeNode node;
            while (older_block_iter.Valid()) {
                node.key = older_block_iter.GetKey();
                node.value = older_block_iter.GetValue();
                node.flag = older_block_iter.GetFlag();
                merge_list.push_back(node);

                older_block_iter.Next();
            }
        }
    } else {
        while (newer_iter.Valid() || newer_block_iter.Valid()) {
            if (!newer_block_iter.Valid() && newer_iter.Valid()) {
                newer_block_iter = newer_iter.GetBlockIterator();
                newer_iter.Next();
            }

            struct MergeNode node;
            while (newer_block_iter.Valid()) {
                node.key = newer_block_iter.GetKey();
                node.value = newer_block_iter.GetValue();
                node.flag = newer_block_iter.GetFlag();
                merge_list.push_back(node);

                newer_block_iter.Next();
            }
        }
    }

    alloc_blocks.insert(alloc_blocks.end(), old_blocks.begin(), old_blocks.end());
    alloc_blocks.insert(alloc_blocks.end(), new_blocks.begin(), new_blocks.end());

    return std::move(merge_list);
}

std::list<lsmtree::MergeNode> lsmtree::LsmTree::MergeTwoMergerdList(std::list<MergeNode>& older_list, std::list<MergeNode>& newer_list) {
    std::list<lsmtree::MergeNode> merge_list;
    std::list<lsmtree::MergeNode>::const_iterator older_list_iter = older_list.cbegin(), newer_list_iter = newer_list.cbegin();

    while (older_list_iter != older_list.cend() || newer_list_iter != newer_list.cend()) {
        if (older_list_iter == older_list.cend()) {
            merge_list.push_back(*newer_list_iter++);
        } else if (newer_list_iter == newer_list.cend()) {
            merge_list.push_back(*older_list_iter++);
        } else {
            if (older_list_iter->key == newer_list_iter->key) {
                merge_list.push_back(*newer_list_iter);
                ++older_list_iter;
                ++newer_list_iter;
            } else if (older_list_iter->key > newer_list_iter->key) {
                merge_list.push_back(*newer_list_iter++);
            } else {
                merge_list.push_back(*older_list_iter++);
            }
        }
    }

    return std::move(merge_list);
}

bool lsmtree::LsmTree::DumpToNextLevel(int level, std::list<MergeNode> &list) {
    if (level + 1 > kMaxLevel) {
        std::cerr << "no left space anymore!" << std::endl;
        return false;
    }

    uint64_t start_index = level_suffix_max[level + 1];

    std::string target_level_dirname_prefix = dir_name + std::string("level") + std::to_string(level + 1) + "/sstable";

    std::list<MergeNode>::const_iterator iter = list.cbegin();
    while (iter != list.cend()) {
        std::string filename = target_level_dirname_prefix + std::to_string(start_index);
        ++start_index;

        bool res = DumpToOneFile(filename, iter, list.cend());
        if (!res) {
            return false;
        }
    }

    {
        // TODO: how to store those var use atomic ops could not result in race condition
        std::lock_guard<std::mutex> lock(level_files_mutex);
        level_suffix_min[level] += SstableNumsPerLevel[0];
        level_file_nums[level] -= SstableNumsPerLevel[0];
        level_file_nums[level + 1] += (start_index - level_suffix_max[level + 1]);
        level_suffix_max[level + 1] = start_index;
    }

    return true;
}

bool lsmtree::LsmTree::DumpToOneFile(const std::string &filename, std::list<MergeNode>::const_iterator& iter, std::list<MergeNode>::const_iterator cend) {
    Writer file_writer;
    std::fstream& writer = file_writer.GetWriter();
    writer.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!writer) {
        std::cerr << "file " << filename << " open filed!" << std::endl;
        return false;
    }

    const uint64_t MaxBlockSize = 65535;
    uint64_t cur_block_offset = 0;
    uint64_t cur_table_offset = 0;
    uint64_t magic_num = 0x87654321;

    std::vector<uint64_t> block_offset(1, 0);
    std::vector<uint64_t> block_size;
    std::vector<std::pair<const char*, uint64_t>> last_key_of_blocks;

    char flag;
    uint64_t key_size;
    uint64_t value_size;
    const char* key;
    const char* value;

    ///write data blocks to file.
    while (iter != cend) {
        char* write_start = const_cast<char*>(iter->key.data()) - 9;
        int write_size = 1 + 2 * sizeof(uint64_t) + iter->key.size() + iter->value.size();

        writer.write(write_start, write_size);
        if (writer.fail()) {
            return false;
        }
        writer.flush();

        cur_block_offset += write_size;
        if (cur_block_offset >= MaxBlockSize) {
            cur_table_offset += cur_block_offset;
            block_offset.push_back(cur_table_offset);
            last_key_of_blocks.push_back({iter->key.data(), iter->key.size()});
            block_size.push_back(cur_block_offset);
            cur_block_offset = 0;

            uint64_t all_size = cur_table_offset + last_key_of_blocks.size() * 4 * sizeof(uint64_t)
                                + std::accumulate(last_key_of_blocks.begin(), last_key_of_blocks.end(), static_cast<uint64_t>(0), [](uint64_t sum, const std::pair<const char*, uint64_t>& rhs) {
                                    return sum + rhs.second;
                                }) + 3 * sizeof(uint64_t);

            if (all_size >= kMaxLenApproximatePerSstable) {
                ++iter;
                break;
            }
        }

        key = iter->key.data();
        key_size = iter->key.size();
        ++iter;
    }

    if (cur_block_offset != 0) {
        cur_table_offset += cur_block_offset;
        block_size.push_back(cur_block_offset);
        last_key_of_blocks.push_back({key, key_size});
    }

    ///write data index block to file.
    auto block_num = block_size.size();
    for (int i = 0; i < block_num; ++i) {
        CacheId_t block_id = next_block_id_.fetch_add(1, std::memory_order_relaxed);
        writer.write(reinterpret_cast<char*>(&block_id), sizeof(CacheId_t));
        if (writer.fail()) {
            return false;
        }

        auto last_key_info = last_key_of_blocks[i];
        writer.write(reinterpret_cast<char*>(&last_key_info.second), sizeof(uint64_t));
        if (writer.fail()) {
            return false;
        }

        writer.write(last_key_info.first, last_key_info.second);
        if (writer.fail()) {
            return false;
        }

        writer.write(reinterpret_cast<char*>(&block_offset[i]), sizeof(uint64_t));
        if (writer.fail()) {
            return false;
        }

        writer.write(reinterpret_cast<char*>(&block_size[i]), sizeof(uint64_t));
        if (writer.fail()) {
            return false;
        }
    }
    writer.flush();

    ///write Footer block to file
    writer.write(reinterpret_cast<char*>(&cur_table_offset), sizeof(uint64_t));
    if (writer.fail()) {
        return false;
    }

    uint64_t data_index_block_size = static_cast<uint64_t>(writer.tellp()) - cur_table_offset - sizeof(uint64_t);
    writer.write(reinterpret_cast<char*>(&data_index_block_size), sizeof(uint64_t));
    if (writer.fail()) {
        return false;
    }

    //The magic num is 0x87654321
    writer.write(reinterpret_cast<char*>(&magic_num), sizeof(uint64_t));
    if (writer.fail()) {
        return false;
    }
    writer.flush();

    return true;
}

void lsmtree::LsmTree::TryBestDoDelayDelete() {
    if (file_need_delete_.empty()) {
        return;
    }

    ///can't delete element when foreach hashmap using iterator
    std::vector<std::string> sstable_can_delete;
    for (auto& file_tag : file_need_delete_) {
        {
            std::lock_guard<std::mutex> lock(level_files_mutex);
            if (level_file_refs.count(file_tag)) {
                continue;
            }
        }

        auto split_pos = file_tag.find(':');
        if (split_pos == std::string::npos) {
            file_need_delete_.erase(file_tag);
        } else {
            std::string level = file_tag.substr(0, split_pos);
            std::string suffix = file_tag.substr(split_pos + 1);

            std::string path = dir_name + std::string("level") + level + std::string("/") + filename_prefix + suffix;
            if (TestFileCanDelete(path)) {
                sstable_can_delete.push_back(file_tag);
                //file_need_delete_.erase(file_tag);
            }
        }
    }

    for (auto& file_tag : sstable_can_delete) {
        file_need_delete_.erase(file_tag);
    }
}

bool lsmtree::LsmTree::Log(uint8_t type, const std::string& key, const std::string& value) {
    Writer writer_handle;
    auto& writer = writer_handle.GetWriter();

    std::string log_filename = config_dir_name + std::string("log/") + std::to_string(log_suffix_max.load(std::memory_order_acquire)) + ".log";
    writer.open(log_filename, std::ios::binary | std::ios::out | std::ios::app);
    if (!writer) {
        return false;
    }

    //FileLock writer_lock(writer, F_WRLCK);
    //writer_lock.Lock();

    //writer.seekp(std::ios::end);

    uint64_t key_size = key.size();
    uint64_t value_size = value.size();

    writer.write(reinterpret_cast<char*>(&type), 1);
    if (writer.fail()) {
        return false;
    }

    writer.write(reinterpret_cast<char*>(&key_size), sizeof(uint64_t));
    if (writer.fail()) {
        return false;
    }

    writer.write(key.c_str(), key_size);
    if (writer.fail()) {
        return false;
    }

    writer.write(reinterpret_cast<char*>(&value_size), sizeof(uint64_t));
    if (writer.fail()) {
        return false;
    }

    writer.write(value.c_str(), value_size);
    if (writer.fail()) {
        return false;
    }
    writer.flush();

    return true;
}

void lsmtree::LsmTree::ReadMetaInfo() {
    std::string meta_filename = config_dir_name + std::string("metainfo");
    std::filesystem::path path(meta_filename);
    if (!std::filesystem::exists(path)) {
        valid_flag_ = true;
        return;
    }

    Reader reader_handle;
    auto& reader = reader_handle.GetReader();
    reader.open(meta_filename, std::ios::in | std::ios::binary);
    if (!reader) {
        return;
    }

    reader.read(reinterpret_cast<char*>(&next_block_id_), sizeof(CacheId_t));
    if (reader.fail()) {
        return;
    }

    for (int i = 0; i <= kMaxLevel; ++i) {
        reader.read(reinterpret_cast<char*>(&level_file_nums[i]), sizeof(uint32_t));
        if (reader.fail()) {
            return;
        }

        reader.read(reinterpret_cast<char*>(&level_suffix_min[i]), sizeof(uint64_t));
        if (reader.fail()) {
            return;
        }

        reader.read(reinterpret_cast<char*>(&level_suffix_max[i]), sizeof(uint64_t));
        if (reader.fail()) {
            return;
        }

        assert(level_suffix_max[i] == level_suffix_min[i] || level_file_nums[i] == (level_suffix_max[i] - level_suffix_min[i]));
        if (level_suffix_max[i] == level_suffix_min[i]) {
            level_suffix_max[i] = level_suffix_min[i] = 0;
            level_file_nums[i] = 0;
        }
    }

    reader.read(reinterpret_cast<char*>(&log_suffix_min), sizeof(uint32_t));
    if (reader.fail()) {
        return;
    }

    reader.read(reinterpret_cast<char*>(&log_suffix_max), sizeof(uint32_t));
    if (reader.fail()) {
        return;
    }

    assert(log_suffix_min <= log_suffix_max);

    valid_flag_ = true;
}

bool lsmtree::LsmTree::Valid() {
    return valid_flag_.load(std::memory_order_acquire);
}

bool lsmtree::LsmTree::WriteMetaInfoToDisk() {
    assert(!Valid());

    Writer writer_handle;
    auto& writer = writer_handle.GetWriter();
    std::string meta_filename = config_dir_name + std::string("metainfo");
    writer.open(meta_filename, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!writer) {
        return false;
    }

    writer.write(reinterpret_cast<char*>(&next_block_id_), sizeof(CacheId_t));
    if (writer.fail()) {
        return false;
    }

    for (int i = 0; i <= kMaxLevel; ++i) {
        assert(level_suffix_max[i] == level_suffix_min[i] || level_file_nums[i] == (level_suffix_max[i] - level_suffix_min[i]));
        writer.write(reinterpret_cast<char*>(&level_file_nums[i]), sizeof(uint32_t));
        if (writer.fail()) {
            return false;
        }

        writer.write(reinterpret_cast<char*>(&level_suffix_min[i]), sizeof(uint64_t));
        if (writer.fail()) {
            return false;
        }

        writer.write(reinterpret_cast<char*>(&level_suffix_max[i]), sizeof(uint64_t));
        if (writer.fail()) {
            return false;
        }
    }

    assert(log_suffix_min <= log_suffix_max);
    writer.write(reinterpret_cast<char*>(&log_suffix_min), sizeof(uint32_t));
    if (writer.fail()) {
        return false;
    }

    writer.write(reinterpret_cast<char*>(&log_suffix_max), sizeof(uint32_t));
    if (writer.fail()) {
        return false;
    }
    writer.flush();

    return true;
}

bool lsmtree::LsmTree::CheckLogAndRedoIfPossible() {
//    if (log_suffix_min == log_suffix_max) {
//        if (log_suffix_min != 0) {
//            log_suffix_min = log_suffix_max = 0;
//        }
//        return true;
//    }

    assert(log_suffix_min <= log_suffix_max);
    while (log_suffix_min <= log_suffix_max) {
        std::string cur_log_filename = config_dir_name + std::string("log/") + std::to_string(log_suffix_min) + ".log";
        std::filesystem::path path(cur_log_filename);
        if (std::filesystem::exists(path)) {
            if (RedoLog(cur_log_filename)) {
                if (log_suffix_min < log_suffix_max) {
                    std::filesystem::remove(path);
                    ++log_suffix_min;
                } else {
                    //TODO: rename log to 0.log
                    std::string new_log_filename = config_dir_name + std::string("log/0.log");
                    std::filesystem::path new_log_path(new_log_filename);
                    std::filesystem::rename(path, new_log_path);
                    log_suffix_min = log_suffix_max = 0;
                    break;
                }
            } else {
                valid_flag_ = false;
                return false;
            }
        } else {
            break;
        }
    }

    //log_suffix_min = log_suffix_max = 0;
    return true;
}

bool lsmtree::LsmTree::RedoLog(const std::string& log_filename) {
    Reader reader_handle;
    auto& reader = reader_handle.GetReader();
    reader.open(log_filename, std::ios::in | std::ios::binary);
    if (!reader) {
        return false;
    }

    reader.seekg(0, std::ios::end);
    auto eof = reader.tellg();

    reader.seekg(0, std::ios::beg);

    char flag = -1;
    uint64_t key_size = 0;
    uint64_t value_size = 0;
    std::string key;
    std::string value;

    while (reader.tellg() < eof) {
        reader.read(reinterpret_cast<char*>(&flag), sizeof(char));
        if (reader.fail()) {
            return false;
        }

        reader.read(reinterpret_cast<char*>(&key_size), sizeof(uint64_t));
        if (reader.fail()) {
            return false;
        }

        key.resize(key_size, 0);
        reader.read(key.data(), key_size);
        if (reader.fail()) {
            return false;
        }

        reader.read(reinterpret_cast<char*>(&value_size), sizeof(uint64_t));
        if (reader.fail()) {
            return false;
        }

        value.resize(value_size, 0);
        reader.read(value.data(), value_size);
        if (reader.fail()) {
            return false;
        }

        if (flag == 0) {
            table_->Add(key, value);
            //line_cache_.Insert(key, std::make_shared<std::string>(value));
        } else {
            table_->Delete(key);
            //line_cache_.Insert(key, nullptr);
        }
    }

    return true;
}

lsmtree::LsmTree::~LsmTree() {
    if (Valid()) {
        Close();
    }
    background_worker.join();

    WriteMetaInfoToDisk();

    delete bloom_;

    std::cout << "db is closed!"<< std::endl;
}

void lsmtree::LsmTree::Close() {
    valid_flag_.store(false, std::memory_order_release);
    cond_.notify_all();
}




