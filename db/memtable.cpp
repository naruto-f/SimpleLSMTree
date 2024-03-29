//
// Created by 123456 on 2023/3/9.
//

#include <memtable.h>
#include <writer.h>

#include <iostream>


lsmtree::MemTable::Status lsmtree::MemTable::Search(const std::string& key, std::string_view& value) const {
    Table::Node* target = nullptr;
    if (table_.Exist(key, &target)) {
        assert(target);
        assert(target->GetKey() == key);

        if (target->GetFlag() == Table::NodeFlag::NODE_ADD) {
            value = std::string_view(target->GetValue().c_str());
            return Status::kSuccess;
        } else {
            return Status::kFound;
        }
    }
    return Status::kNotFound;
}

lsmtree::MemTable::Status lsmtree::MemTable::Add(const std::string &key, const std::string& value) {
    Table::Node* target = nullptr;
    if (table_.Exist(key, &target)) {
        assert(target);
        assert(target->GetKey() == key);

        if (target->GetFlag() == Table::NodeFlag::NODE_ADD) {
            target->SetValue(value);
        } else if (target->GetFlag() == Table::NodeFlag::NODE_DELETE) {
            target->SetFlag(Table::NodeFlag::NODE_ADD);
            target->SetValue(value);
        }
    } else {
        table_.Insert(key, value, Table::NodeFlag::NODE_ADD);
        UpdateCurSize(key, 1 + 2 * sizeof(uint64_t) + key.size() + value.size());
        //node_counts_.fetch_add(1, std::memory_order_relaxed);
    }

    return Status::kSuccess;
}

lsmtree::MemTable::Status lsmtree::MemTable::Delete(const std::string& key) {
    Table::Node* target = nullptr;
    if (table_.Exist(key, &target)) {
        assert(target);
        assert(target->GetKey() == key);

        target->SetFlag(Table::NodeFlag::NODE_DELETE);
        target->SetValue("");
    } else {
        table_.Insert(key, "", Table::NodeFlag::NODE_DELETE);
        UpdateCurSize(key, 1 + 2 * sizeof(uint64_t) + key.size());
        //node_counts_.fetch_add(1, std::memory_order_relaxed);
    }

    return Status::kSuccess;
}

lsmtree::MemTable::MemTable() : refs_(1), estimate_size_(3 * sizeof(uint64_t)), cur_block_size_(0) {

}

lsmtree::MemTable::~MemTable() {
    assert(refs_.load(std::memory_order_relaxed) == 0);
}

//uint32_t lsmtree::MemTable::GetCurNums() const {
//   // return node_counts_.load(std::memory_order_relaxed);
//}

void lsmtree::MemTable::Ref() {
    refs_.fetch_add(1, std::memory_order_relaxed);
}

void lsmtree::MemTable::UnRef() {
    refs_.fetch_sub(1, std::memory_order_relaxed);
    assert(refs_.load(std::memory_order_relaxed) >= 0);

    if (refs_.load(std::memory_order_relaxed) == 0) {
         delete this;
    }
}

lsmtree::MemTable::Status lsmtree::MemTable::Dump(const std::string &filename, std::atomic<uint64_t>& block_id) const {
    Writer file_writer;
    std::fstream& writer = file_writer.GetWriter();
    writer.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!writer) {
        std::cerr << "file " << filename << " open filed!" << std::endl;
        return Status::kError;
    }

    const uint64_t MaxBlockSize = 65535;
    uint64_t cur_block_offset = 0;
    uint64_t cur_table_offset = 0;
    uint64_t magic_num = 0x87654321;

    std::vector<uint64_t> block_offset(1, 0);
    std::vector<uint64_t> block_size;
    std::vector<std::pair<const char*, uint64_t>> last_key_of_blocks;

    Table::Iterator iter(&table_);
    iter.SeekToFirst();

    char flag;
    uint64_t key_size;
    uint64_t value_size;
    const char* key;
    const char* value;

    ///write data blocks to file.
    while (iter.Valid()) {
        flag = static_cast<uint8_t>((*iter)->GetFlag());
        key = (*iter)->GetKey().c_str();
        key_size = (*iter)->GetKey().size();
        value = (*iter)->GetValue().c_str();
        value_size = (*iter)->GetValue().size();


        writer.write(&flag, sizeof(flag));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(reinterpret_cast<char*>(&key_size), sizeof(uint64_t));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(key, key_size);
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(reinterpret_cast<char*>(&value_size), sizeof(uint64_t));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(value, value_size);
        if (writer.fail()) {
            return Status::kError;
        }
        writer.flush();

        cur_block_offset += (sizeof(char) + 2 * sizeof(uint64_t) + key_size + value_size);
        if (cur_block_offset >= MaxBlockSize) {
            cur_table_offset += cur_block_offset;
            block_size.push_back(cur_block_offset);
            block_offset.push_back(cur_table_offset);
            last_key_of_blocks.push_back({key, key_size});
            cur_block_offset = 0;
        }

        iter.Next();
    }

    if (cur_block_offset != 0) {
        cur_table_offset += cur_block_offset;
        block_size.push_back(cur_block_offset);
        last_key_of_blocks.push_back({key, key_size});
    }

    ///write data index block to file.
    auto block_num = block_size.size();
    for (int i = 0; i < block_num; ++i) {
        uint64_t id = block_id.fetch_add(1, std::memory_order_relaxed);
        writer.write(reinterpret_cast<char*>(&id), sizeof(uint64_t));
        if (writer.fail()) {
            return Status::kError;
        }

        auto last_key_info = last_key_of_blocks[i];
        writer.write(reinterpret_cast<char*>(&last_key_info.second), sizeof(uint64_t));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(last_key_info.first, last_key_info.second);
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(reinterpret_cast<char*>(&block_offset[i]), sizeof(uint64_t));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(reinterpret_cast<char*>(&block_size[i]), sizeof(uint64_t));
        if (writer.fail()) {
            return Status::kError;
        }
    }
    writer.flush();

    ///write Footer block to file
    writer.write(reinterpret_cast<char*>(&cur_table_offset), sizeof(uint64_t));
    if (writer.fail()) {
        return Status::kError;
    }

    uint64_t data_index_block_size = static_cast<uint64_t>(writer.tellp()) - cur_table_offset - sizeof(uint64_t);
    writer.write(reinterpret_cast<char*>(&data_index_block_size), sizeof(uint64_t));
    if (writer.fail()) {
        return Status::kError;
    }

    //The magic num is 0x87654321
    writer.write(reinterpret_cast<char*>(&magic_num), sizeof(uint64_t));
    if (writer.fail()) {
        return Status::kError;
    }
    writer.flush();

    return Status::kSuccess;
}

void lsmtree::MemTable::UpdateCurSize(const std::string& key, uint64_t add_size) {
    cur_block_size_ += add_size;
    if (cur_block_size_ >= 64 * 1024) {
        estimate_size_ += (cur_block_size_ + 4 * sizeof(uint64_t) + key.size());
        cur_block_size_ = 0;
    }
}


