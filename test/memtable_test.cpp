//
// Created by 123456 on 2023/3/9.
//

#include <memtable.h>

#include <iostream>

using MemTable = lsmtree::MemTable;

MemTable* mb;

void MemTableTestOfSingleThread() {
    std::cout << "SkipListTestOfMulitThread begin!" << std::endl;

    for (int i = 1; i <= 20000; ++i) {
        std::string tmp = std::to_string(i);
        auto status = mb->Add(tmp, tmp);
        assert(status == MemTable::Status::kSuccess);
    }

    for (int i = 1; i <= 20000; ++i) {
        std::string tmp = std::to_string(i);
        std::string_view tmp_view;
        auto status = mb->Search(tmp, tmp_view);
        assert(status == MemTable::Status::kSuccess);
    }

    for (int i = 10001; i <= 20000; ++i) {
        std::string tmp = std::to_string(i);
        auto status = mb->Delete(tmp);
        assert(status == MemTable::Status::kSuccess);
    }

    for (int i = 1; i <= 10000; ++i) {
        std::string tmp = std::to_string(i);
        std::string_view tmp_view;
        auto status = mb->Search(tmp, tmp_view);
        assert(status == MemTable::Status::kSuccess);
    }

    for (int i = 10001; i <= 20000; ++i) {
        std::string tmp = std::to_string(i);
        std::string_view tmp_view;
        auto status = mb->Search(tmp, tmp_view);
        assert(status == MemTable::Status::kFound);
    }

    for (int i = 20001; i <= 21000; ++i) {
        std::string tmp = std::to_string(i);
        std::string_view tmp_view;
        auto status = mb->Search(tmp, tmp_view);
        assert(status == MemTable::Status::kNotFound);
    }

    for (int i = 20001; i <= 20010; ++i) {
        std::string tmp = std::to_string(i);
        auto status = mb->Add(tmp, tmp);

        if (status == MemTable::Status::kFull) {
            std::cout << i << std::endl;
        }

        assert(status != MemTable::Status::kFull);
    }

    std::cout << "SkipListTestOfMulitThread end!" << std::endl;
}

void MemTableTestOfMuiltThread() {

}



int main() {
    mb = new MemTable();
    MemTableTestOfSingleThread();
}
