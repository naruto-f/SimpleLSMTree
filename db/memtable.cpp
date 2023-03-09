//
// Created by 123456 on 2023/3/9.
//

#include <memtable.h>


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


