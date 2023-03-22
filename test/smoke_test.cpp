//
// Created by 123456 on 2023/3/16.
//

#include <db.h>
#include <lsmtree.h>
#include <iostream>
#include <algorithm>

lsmtree::Db* db = nullptr;
//lsmtree::LsmTree db(10000, 10000);

std::string value(1000, '8');

void Add_thread_Entry(int start, int end) {
    for (int i = start; i <= end; ++i) {
        std::string cur = std::to_string(i);
        auto res = db->Add(cur, value);
        assert(res);
    }
}

//void Test_Multi_Thread_Add() {
//    std::vector<std::thread> threads;
//    for (int i = 0; i < 4; ++i) {
//        threads.push_back(std::thread(Add_thread_Entry, i * 1000 + 1, (i + 1) * 1000));
//    }
//
//    std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
//
//    std::cout << "add complete!" << std::endl;
//
//    std::shared_ptr<std::string> sp(nullptr);
//    for (int i = 1; i <= 4000; ++i) {
//        sp = nullptr;
//        std::string key = std::to_string(i);
//        bool res = db->Get(key, sp);
//        assert(res && sp && *sp == key);
//    }
//
//    return;
//}

void Test_Multi_Thread_Add_Multi_table(uint32_t thread_nums, uint32_t tasks_per_thread) {
    std::cout << "Test_Multi_Thread_Add_Multi_table starting!" << std::endl;

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_nums; ++i) {
        threads.push_back(std::thread(Add_thread_Entry, i * tasks_per_thread + 1, (i + 1) * tasks_per_thread));
    }

    std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));

    std::cout << "add complete!" << std::endl;

    std::vector<std::string> unfind;
    std::shared_ptr<std::string> sp(nullptr);
    for (int i = 1; i <= thread_nums * tasks_per_thread; ++i) {
        sp = nullptr;
        std::string key = std::to_string(i);
        bool res = db->Get(key, sp);
        //assert(res && sp && (*sp == key));
        if (!res) {
            unfind.push_back(key);
        }
    }

    if (!unfind.empty()) {
        for (auto& k : unfind) {
            std::cout << k << std::endl;
        }

        std::cout << "size: " << unfind.size() << std::endl;
    }

    std::cout << "Test_Multi_Thread_Add_Multi_table starting!" << std::endl;
}

void Test_Read_After_Redo_log(int start, int end) {
    std::shared_ptr<std::string> sp(nullptr);
    std::vector<std::string> unfind;
    for (int i = start; i <= end; ++i) {
        sp = nullptr;
        std::string key = std::to_string(i);
        bool res = db->Get(key, sp);
        //assert(res);
        if (!res) {
            unfind.push_back(key);
        }
    }

    if (!unfind.empty()) {
        for (auto& k : unfind) {
            std::cout << k << std::endl;
        }

        std::cout << "size: " << unfind.size() << std::endl;
    }

//    for (int i = end + 1; i <= end + 5000; ++i) {
//        sp = nullptr;
//        std::string key = std::to_string(i);
//        db->Get(key, sp);
//        assert(!sp);
//    }
}





int main(int argc, char* argv[]) {
    db = new lsmtree::LsmTree(10000, 1000);

    //Test_Multi_Thread_Add();
    //Test_Read_After_Redo_log();

    uint32_t thread_nums = 10;
    uint32_t tasks_per_thread = 20000;

    Test_Multi_Thread_Add_Multi_table(thread_nums, tasks_per_thread);
    //Test_Read_After_Redo_log(1, thread_nums * tasks_per_thread);

    //db->Close();
    delete db;
    return 0;
}
