//
// Created by 123456 on 2023/3/11.
//

#include "lsmtree.h"
#include <memtable.h>
#include <file.h>

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

    ///lookup at L2Cache or sstable
    int max_suffix = FileOperator::FindLargestSuffix(dir_name, filename_prefix);
    for ()



    std::shared_ptr<Block> block_ptr(nullptr);
    if ()


    return false;
}
