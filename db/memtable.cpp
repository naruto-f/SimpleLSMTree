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

lsmtree::MemTable::Status lsmtree::MemTable::Add(const std::string &key, const std::string_view &value) {
    if (GetCurNums() >= kMaxNumsPerTable) {
        return Status::kFull;
    }

    Table::Node* target = nullptr;
    if (table_.Exist(key, &target)) {
        assert(target);
        assert(target->GetKey() == key);

        if (target->GetFlag() == Table::NodeFlag::NODE_ADD) {
            target->SetValue(std::string(value.data(), value.size()));
        } else if (target->GetFlag() == Table::NodeFlag::NODE_DELETE) {
            target->SetFlag(Table::NodeFlag::NODE_ADD);
            target->SetValue(std::string(value.data(), value.size()));
        }
    } else {
        table_.Insert(key, std::string(value.data(), value.size()), Table::NodeFlag::NODE_ADD);
        node_counts_.fetch_add(1, std::memory_order_relaxed);
    }

    return Status::kSuccess;
}

lsmtree::MemTable::Status lsmtree::MemTable::Delete(const std::string& key) {
    if (GetCurNums() >= kMaxNumsPerTable) {
        return Status::kFull;
    }

    Table::Node* target = nullptr;
    if (table_.Exist(key, &target)) {
        assert(target);
        assert(target->GetKey() == key);

        target->SetFlag(Table::NodeFlag::NODE_DELETE);
        target->SetValue("");
    } else {
        table_.Insert(key, "", Table::NodeFlag::NODE_DELETE);
        node_counts_.fetch_add(1, std::memory_order_relaxed);
    }

    return Status::kSuccess;
}

lsmtree::MemTable::MemTable() : refs_(0), node_counts_(0) {

}

lsmtree::MemTable::~MemTable() {
    assert(refs_.load(std::memory_order_relaxed) == 0);
}

uint32_t lsmtree::MemTable::GetCurNums() const {
    return node_counts_.load(std::memory_order_relaxed);
}

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

lsmtree::MemTable::Status lsmtree::MemTable::Dump(const std::string &filename, uint64_t* start_block_id) const {
    Writer file_writer;
    std::ofstream& writer = file_writer.GetWriter();
    writer.open(filename, std::ios::out | std::ios::binary);
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
        std::size_t value_size = (*iter)->GetValue().size();


        writer.write(&flag, sizeof(flag));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(reinterpret_cast<char*>(&key_size), sizeof(std::size_t));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(key, key_size);
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(reinterpret_cast<char*>(&value_size), sizeof(std::size_t));
        if (writer.fail()) {
            return Status::kError;
        }

        writer.write(value, value_size);
        if (writer.fail()) {
            return Status::kError;
        }

        cur_block_offset += (sizeof(char) + 2 * sizeof(std::size_t) + key_size + value_size);
        if (cur_block_offset >= MaxBlockSize) {
            cur_table_offset += cur_block_offset;
            cur_block_offset = 0;
            block_offset.push_back(cur_table_offset);
            last_key_of_blocks.push_back({key, key_size});
            block_size.push_back(cur_block_offset);

            writer.flush();
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
        writer.write(reinterpret_cast<char*>(start_block_id), sizeof(uint64_t));
        if (writer.fail()) {
            return Status::kError;
        }
        ++(*start_block_id);

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


