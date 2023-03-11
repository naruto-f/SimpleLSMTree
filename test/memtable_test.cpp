//
// Created by 123456 on 2023/3/9.
//

#include <memtable.h>
#include <fstream>
#include <iostream>
#include <thread>
#include <chrono>
#include <cstring>

using MemTable = lsmtree::MemTable;

MemTable* mb;

void MemTableTestOfSingleThread() {
    std::cout << "MemTableTestOfSingleThread begin!" << std::endl;

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

    std::cout << "MemTableTestOfSingleThread end!" << std::endl;
}

void AddThreadEntry(int start, int end) {
    for (int i = start; i <= end; ++i) {
        std::string tmp = std::to_string(i);
        auto status = mb->Add(tmp, tmp);
        assert(status == MemTable::Status::kSuccess);
    }
}

void DeleteThreadEntry(int start, int end) {
    for (int i = start; i <= end; ++i) {
        std::string tmp = std::to_string(i);
        auto status = mb->Delete(tmp);
        assert(status == MemTable::Status::kSuccess);
    }
}

void SearchThreadEntry(int start, int end) {
    for (int i = start; i <= end; ++i) {
        std::string tmp = std::to_string(i);
        std::string_view tmp_view;
        auto status = mb->Search(tmp, tmp_view);
        assert(status == MemTable::Status::kSuccess);
    }
}


void MemTableTestOfMuiltThread() {
    std::cout << "MemTableTestOfMulitThread begin!" << std::endl;

    std::thread addThread1(AddThreadEntry , 1, 10000);
    std::thread addThread2(AddThreadEntry , 10001, 20000);

    addThread1.join();
    addThread2.join();

    SearchThreadEntry(1, 20000);

    //std::thread insertThread3(InsertThreadEntry , 3, 20001, 30000);
    std::thread deleteThread1(DeleteThreadEntry, 10001, 20000);
    std::thread searchThread(SearchThreadEntry , 1, 10000);

    deleteThread1.join();
    searchThread.join();

    //should assert fail in here
    SearchThreadEntry(10001, 10001);

    std::cout << "MemTableTestOfMulitThread end!" << std::endl;
}

void SmokeTestOfDump(const char* filename) {
    std::cout << "SmokeTestOfDump begin!" << std::endl;

//    AddThreadEntry(0, 1000);
//
//    auto status = mb->Dump(filename);
//    assert(status == MemTable::Status::kSuccess);

    std::ifstream reader("/home/naruto/StudyDir/SimpleLSMTree/storage/test", std::ios::in | std::ios::binary);
    assert(reader);

    reader.seekg(-8, std::ios::end);
    uint64_t magic_num = 0;
    reader.read(reinterpret_cast<char*>(&magic_num), 8);
    assert(magic_num == 0x87654321);

    reader.seekg(0, std::ios::beg);
    char flag = 6;
    reader.read(&flag, 1);
    assert(flag == 0);

    uint64_t key_size = 0;
    reader.read(reinterpret_cast<char*>(&key_size), 8);
    assert(key_size == 1);

    char key;
    reader.read(&key, key_size);
    std::cout << "key == " << key << std::endl;
    //assert(std::strcmp(key, "1") == 0);

    uint64_t value_size = 0;
    reader.read(reinterpret_cast<char*>(&value_size), 8);

    assert(value_size == 1);

    char value[10] = { '\0' };
    reader.read(value, value_size);
    //assert(std::strcmp(value, "1") == 0);
    std::cout << "value = " << value << std::endl;

    std::cout << "SmokeTestOfDump end!" << std::endl;
}

void RdBufTest() {
    std::ifstream reader("/home/naruto/StudyDir/SimpleLSMTree/storage/test", std::ios::in | std::ios::binary);
    assert(reader);

    reader.seekg(std::ios::beg);

    char* file = reinterpret_cast<char*>(reader.get());

    std::cout << *file << std::endl;

    reader.close();
}


int main() {
    mb = new MemTable();
    //MemTableTestOfSingleThread();
    //MemTableTestOfMuiltThread();
    //SmokeTestOfDump("/home/naruto/StudyDir/SimpleLSMTree/storage/test");
    RdBufTest();

    return 0;
}
