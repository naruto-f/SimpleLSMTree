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
    const char* dir_name = "../storage/";
    const char* config_dir_name = "../config/";
    const std::string filename_prefix = "sstable";

    const uint32_t SstableNumsPerLevel[] = {8, 80, 800, 8000, 80000};

    enum {
        kMaxLevel = 4,
        kMaxLenApproximatePerSstable = 4 * 1024 * 1024
    };

    enum UpdateType : uint8_t {
        kAdd = 0,
        kDelete = 1
    };

    std::atomic<uint32_t> log_suffix_max;
    std::atomic<uint32_t> log_suffix_min;

    std::mutex level_files_mutex;
    int level_file_nums[kMaxLevel + 1] = { 0 };
    unsigned long level_suffix_max[kMaxLevel + 1] = { 0 };
    unsigned long level_suffix_min[kMaxLevel + 1] = { 0 };
    std::unordered_map<std::string, uint64_t> level_file_refs;

    bool TestFileCanDelete(const std::string& filename) {
        std::filesystem::path path(filename);

        if (std::filesystem::exists(path)) {
            Writer writer;
            auto& write = writer.GetWriter();

            FileLock flock(dynamic_cast<std::fstream&>(write), F_WRLCK);
            if (!flock.LockWithoutWait()) {
                return false;
            }
        }
        std::filesystem::remove(path);

        return true;
    }

}

struct lsmtree::MergeNode {
    std::string_view key;
    std::string_view value;
    char flag;
};

bool lsmtree::LsmTree::Get(const std::string& key, std::shared_ptr<std::string>& value) {
    ///①Throught bloom filter first
    if (!bloom_->MayExist(key)) {
        return false;
    }


    ///②lookup at L1Cache second
    if (line_cache_.Search(key, value)) {
        return true;
    }


    ///③lookup at cur main SkipList
    std::string_view value_view(nullptr);
    MemTable* table = nullptr;
    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        MemTable* table = table_;
        assert(table);
    }

    table->Ref();
    MemTable::Status status = table->Search(key, value_view);
    if (status == MemTable::Status::kSuccess) {
        value = std::make_shared<std::string>(value_view.data(), value_view.size());
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
    std::pair<uint64_t, uint64_t> range_of_file_pre_level[kMaxLevel + 1];
    {
        std::lock_guard<std::mutex> lock(level_files_mutex);
        for (int i = 0; i <= kMaxLevel; ++i) {
            range_of_file_pre_level[i] = {level_suffix_min[i], level_suffix_max[i]};
            for (int j = level_suffix_min[i]; j < level_suffix_min[i] + SstableNumsPerLevel[0]; ++j) {
                std::string file_tag = std::to_string(i) + ":" + std::to_string(j);
                ++level_file_refs[file_tag];
            }
        }
    }

    int res = -1;
    for (int i = 0; i <= kMaxLevel; ++i) {
        if (res != -1) {
            break;
        }

        for (int j = range_of_file_pre_level[i].second; j >= range_of_file_pre_level[i].first; --j) {
            std::string filename = dir_name + std::string("level" + std::to_string(i) + "/") + filename_prefix + std::to_string(j);

            SSTable cur_table(filename.c_str(), &block_cache_);
            assert(cur_table.Valid());

            int res = cur_table.Get(key, value);
            if (res == 0 || res == -2) {
                break;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(level_files_mutex);
        for (int i = 0; i <= kMaxLevel; ++i) {
            range_of_file_pre_level[i] = {level_suffix_min[i], level_suffix_max[i]};
            for (int j = level_suffix_min[i]; j < level_suffix_min[i] + SstableNumsPerLevel[0]; ++j) {
                std::string file_tag = std::to_string(i) + ":" + std::to_string(j);
                if (--level_file_refs[file_tag] == 0) {
                    level_file_refs.erase(file_tag);
                }
            }
        }
    }

    // TODO: L1Cache add target that tag node is added or deleted
    if (res == 0) {
        line_cache_.Insert(key, value);
        return true;
    }

    return false;
}

void lsmtree::LsmTree::Add(const std::string &key, const std::string &value) {
    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        if (table_->IsFull()) {
            MemtableChange();
        }

        Log(UpdateType::kAdd, key, value);
    }

    table_->Add(key, value);
}

void lsmtree::LsmTree::Delete(const std::string &key) {
    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        if (table_->IsFull()) {
            MemtableChange();
        }

        Log(UpdateType::kAdd, key, "");
    }

    table_->Delete(key);
}

void lsmtree::LsmTree::BackgroundWorkerThreadEntry() {
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        cond_.wait(lock, [this]() {
            return !background_tables_.empty();
        });

        while (!background_tables_.empty()) {
            auto table = background_tables_.front();
            table_->UnRef();
            background_tables_.pop_front();
            lock.unlock();

            //TODO: how to recover from disk write failures or low space
            assert(MergeMemtableToDisk(table));    //if return false, this storage system is not in Consistent state, now we stop system immediately.

            lock.lock();
        }

        TryBestDoDelayDelete();
    }
}

void lsmtree::LsmTree::MemtableChange() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        background_tables_.push_back(table_);
        table_->Ref();
        ++log_suffix_max;
        cond_.notify_all();
    }

    MemTable* new_table = new MemTable();
    assert(new_table);

    table_ = new_table;
}

bool lsmtree::LsmTree::MergeMemtableToDisk(MemTable* table) {
    bool res = CheckAndDownSstableIfPossible();
    if (!res) {
        std::cerr << "Lack of space or Write failed!" << std::endl;
        return false;
    }

    std::string filename = dir_name + std::string("level0/") + filename_prefix + std::to_string(level_suffix_max[0] + 1);
    table->Dump(filename, next_block_id_);

    /// log can be deleted when it own memtable's data persist to disk
    assert(log_suffix_min <= log_suffix_max);
    std::string log_filename = config_dir_name + std::string("log/") + std::to_string(log_suffix_min) + ".log";
    std::filesystem::path path(filename);
    if (std::filesystem::exists(path)) {
        std::filesystem::remove(path);
    }

    if (log_suffix_min == log_suffix_max) {
        log_suffix_max = log_suffix_min = 0;
    } else {
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
                          : line_cache_(line_cache_capacity),
                            block_cache_(block_cache_capacity),
                            background_worker(&LsmTree::BackgroundWorkerThreadEntry, this) {
    //background_worker = std::move(std::thread(&LsmTree::BackgroundWorkerThreadEntry, this));
    CheckSstableInfo();
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
        lists.push_back(MergeTwoSstable(level, start_index + 2 * i, 2 * i + 1, all_alloc_block_ptrs));
    }

    std::list<lsmtree::MergeNode> older_list = MergeTwoMergerdList(lists[0], lists[1]);
    std::list<lsmtree::MergeNode> newer_list = MergeTwoMergerdList(lists[2], lists[3]);
    std::list<lsmtree::MergeNode> last_list = MergeTwoMergerdList(older_list, newer_list);

    bool res = DumpToNextLevel(level, last_list);
    for (auto block_ptr : all_alloc_block_ptrs) {
        delete block_ptr;
    }

    ///delay delete
    for (int i = start_index; i <= end_index; ++i) {
        file_need_delete_.insert(std::to_string(level) + ":" + std::to_string(i));
    }

    // TODO: how to store those var could not result in race condition
    level_suffix_min[level] = end_index + 1;
    level_file_nums[level] -= (end_index - start_index + 1);

    return res;
}

std::list<lsmtree::MergeNode> lsmtree::LsmTree::MergeTwoSstable(int level, int older_index, int newer_index, std::vector<Block*>& alloc_blocks) {
    std::string older_table_name = dir_name + std::string("level") + std::to_string(level) + "/sstable" + std::to_string(older_index);
    std::string newer_table_name = dir_name + std::string("level") + std::to_string(level) + "/sstable" + std::to_string(newer_index);

    lsmtree::SSTable older_table(older_table_name.c_str(), nullptr), newer_table(newer_table_name.c_str(), nullptr);
    lsmtree::SSTable::Iterator older_iter(&older_table), newer_iter(&newer_table);
    lsmtree::Block::Iterator older_block_iter(nullptr), newer_block_iter(nullptr);

    std::vector<Block*>& old_blocks = older_iter.GetAllocBlockAdress();
    std::vector<Block*>& new_blocks = newer_iter.GetAllocBlockAdress();

    std::list<lsmtree::MergeNode> merge_list;

    while (older_iter.Valid() && newer_iter.Valid()) {
        if (older_iter.Valid() && !older_block_iter.Valid()) {
            older_block_iter = older_iter.GetBlockIterator();
            older_iter.Next();
        }

        if (newer_iter.Valid() && !newer_block_iter.Valid()) {
            newer_block_iter = newer_iter.GetBlockIterator();
            newer_iter.Next();
        }

        while (older_block_iter.Valid() && newer_block_iter.Valid()) {
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
        }
    }

    lsmtree::SSTable::Iterator seq_iter = older_iter.Valid() ? older_iter : newer_iter;
    lsmtree::Block::Iterator seq_block_iter = older_iter.Valid() ? older_block_iter : newer_block_iter;
    while (seq_iter.Valid()) {
        struct MergeNode node;
        while (seq_block_iter.Valid()) {
            node.key = seq_block_iter.GetKey();
            node.value = seq_block_iter.GetValue();
            node.flag = seq_block_iter.GetFlag();
            merge_list.push_back(node);

            seq_block_iter.Next();
        }

        seq_iter.Next();
    }

    alloc_blocks.insert(alloc_blocks.end(), old_blocks.begin(), old_blocks.end());
    alloc_blocks.insert(alloc_blocks.end(), new_blocks.begin(), new_blocks.end());
    return std::move(merge_list);
}

std::list<lsmtree::MergeNode> lsmtree::LsmTree::MergeTwoMergerdList(std::list<MergeNode>& older_list, std::list<MergeNode>& newer_list) {
    std::list<lsmtree::MergeNode> merge_list;
    std::list<lsmtree::MergeNode>::const_iterator older_list_iter = older_list.begin(), newer_list_iter = newer_list.begin();

    while (older_list_iter != older_list.end() || newer_list_iter != newer_list.end()) {
        if (older_list_iter == older_list.end()) {
            merge_list.push_back(*newer_list_iter++);
        } else if (newer_list_iter == newer_list.end()) {
            merge_list.push_back(*older_list_iter++);
        } else {
            if (older_list_iter->key == newer_list_iter->key) {
                merge_list.push_back(*older_list_iter++);
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

    uint64_t start_index = level_suffix_max[level + 1] + 1;

    std::string target_level_dirname_prefix = dir_name + std::string("level") + std::to_string(level + 1) + "/sstable";

    std::list<MergeNode>::const_iterator iter = list.cbegin();
    while (iter != list.cend()) {
        std::string filename = target_level_dirname_prefix + std::to_string(start_index);
        ++start_index;

        bool res = DumpToOneFile(filename, iter);
        if (!res) {
            return false;
        }
    }

    level_file_nums[level] += (start_index - level_suffix_max[level + 1] - 1);
    level_suffix_max[level + 1] = start_index - 1;

    return true;
}

bool lsmtree::LsmTree::DumpToOneFile(const std::string &filename, std::list<MergeNode>::const_iterator& iter) {
    Writer file_writer;
    std::ofstream& writer = file_writer.GetWriter();
    writer.open(filename, std::ios::out | std::ios::binary);
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
    while (iter != std::list<MergeNode>::const_iterator(nullptr)) {
        char* write_start = const_cast<char*>(iter->key.data()) - 3;
        int write_size = 1 + 2 * sizeof(uint64_t) + iter->key.size() + iter->value.size();

        writer.write(write_start, write_size);
        if (writer.fail()) {
            return false;
        }

        cur_block_offset += write_size;
        if (cur_block_offset >= MaxBlockSize) {
            cur_table_offset += cur_block_offset;
            cur_block_offset = 0;
            block_offset.push_back(cur_table_offset);
            last_key_of_blocks.push_back({iter->key.data(), iter->key.size()});
            block_size.push_back(cur_block_offset);

            writer.flush();

            uint64_t all_size = cur_block_offset + last_key_of_blocks.size() * 4 * sizeof(uint64_t)
                                + std::accumulate(last_key_of_blocks.begin(), last_key_of_blocks.end(), 0, [](const std::pair<const char*, uint64_t>& lhs, const std::pair<const char*, uint64_t>& rhs) {
                                    return lhs.second + rhs.second;
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

    for (auto& file_tag : file_need_delete_) {
        {
            std::lock_guard<std::mutex> lock(level_files_mutex);
            if (!level_file_refs.count(file_tag)) {
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
                file_need_delete_.erase(file_tag);
            }
        }
    }
}

bool lsmtree::LsmTree::Log(uint8_t type, const std::string& key, const std::string& value) {
    Writer writer_handle;
    auto& writer = writer_handle.GetWriter();

    std::string log_filename = config_dir_name + std::string("log/") + std::to_string(log_suffix_max) + ".log";
    writer.open(log_filename, std::ios::binary | std::ios::out);
    if (!writer) {
        return false;
    }

    FileLock writer_lock(dynamic_cast<std::fstream&>(writer), F_WRLCK);
    writer_lock.Lock();

    writer.seekp(std::ios::end);

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




