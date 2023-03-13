//
// Created by 123456 on 2023/3/11.
//

#include "lsmtree.h"
#include <memtable.h>
#include <file.h>
#include <table.h>

namespace {
    const char* dir_name = "../storage";
    std::string filename_prefix = "sstable";

}

bool lsmtree::LsmTree::Get(const std::string& key, std::shared_ptr<std::string>& value) {
    ///Throught bloom filter first
    if (!bloom_->MayExist(key)) {
        return false;
    }

    ///lookup at L1Cache second
    if (line_cache_.Search(key, value)) {
        return true;
    }

    ///lookup at SkipList
    std::string_view value_view(nullptr);
    MemTable::Status status = table_->Search(key, value_view);
    if (status == MemTable::Status::kSuccess) {
        value = std::make_shared<std::string>(value_view.data(), value_view.size());
        return true;
    } else if (status == MemTable::Status::kFound) {
        return false;
    }

    ///Binary search block index that key may exist, foreach sstable from new to old, and lookup at L2Cache if possible.
    int max_suffix = FileOperator::FindLargestSuffix(dir_name, filename_prefix);
    while (max_suffix >= 0) {
        std::string filename = filename_prefix + std::to_string(max_suffix);
        --max_suffix;

        SSTable cur_table(filename.c_str(), &block_cache_);
        assert(cur_table.Valid());

        int res = cur_table.Get(key, value);
        if (res == 0) {
            value = std::make_shared<std::string>(value_view.data(), value_view.size());
            return true;
        } else if (res == -2) {
            return false;
        }
    }

    return false;
}

void lsmtree::LsmTree::Add(const std::string &key, const std::string &value) {
    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        if (table_->IsFull()) {
            MemtableChange();
        }
    }

    table_->Add(key, value);
}

void lsmtree::LsmTree::Delete(const std::string &key) {
    {
        std::lock_guard<std::mutex> lock(table_mutex_);
        if (table_->IsFull()) {
            MemtableChange();
        }
    }

    table_->Delete(key);
}

void lsmtree::LsmTree::BackgroundWorkerThreadEntry() {
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        cond_.wait(lock, [this]() {
            return !background_tables_.empty();
        });

        while(!background_tables_.empty()) {
            auto table = background_tables_.front();
            background_tables_.pop_front();
            lock.unlock();
            MergeMemtableToDisk(table);

            lock.lock();
        }
    }
}

void lsmtree::LsmTree::MemtableChange() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        background_tables_.push_back(table_);
        cond_.notify_all();
    }

    MemTable* new_table = new MemTable();
    assert(new_table);

    table_ = new_table;
}

void lsmtree::LsmTree::MergeMemtableToDisk(MemTable* table) {



}
