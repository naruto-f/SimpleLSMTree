//
// Created by 123456 on 2023/3/8.
//

#include <skiplist.h>

#include <thread>
#include <iostream>
#include <chrono>


class Compare {
public:
    int operator()(int lhs, int rhs) const {
        if (lhs == rhs) {
            return 0;
        } else if (lhs > rhs) {
            return 1;
        } else {
            return -1;
        }
    }
};

using SkipList = lsmtree::SkipList<int, std::string, Compare>;
SkipList list;

void NodeTestOfSigleThread() {
    using Node = SkipList::Node;
    std::cout << "NodeTest begin!" << std::endl;

    Node node(SkipList::NodeFlag::NODE_ADD, 8, 1, "test");
    std::cout << "Key = " << node.GetKey() << std::endl;
    std::cout << "Value = " << node.GetValue() << std::endl;
    std::cout << "Flag = " << static_cast<int>(node.GetFlag()) << std::endl;

    node.SetNextWithSync(0, nullptr);
    assert(node.GetNextWithoutSync(0) == nullptr);

    node.SetNextWithSync(1, reinterpret_cast<Node*>(0xff));
    std::cout << reinterpret_cast<uint64_t>(node.GetNextWithSync(1)) << std::endl;

    std::cout << "NodeTest end!" << std::endl;
};

void SkipListTestOfSigleThread() {
    std::cout << "SkipListTest begin!" << std::endl;

    SkipList list;

    SkipList::Node* node = nullptr;
    list.Exist(5, &node);
    assert(!node);

    //10w个key太耗时了，插入+删除10w个key需要几分钟
    for(int i = 1; i <= 10000; ++i) {
        list.Insert(i, std::to_string(i), SkipList::NodeFlag::NODE_ADD);
    }

    for(int i = 1; i <= 10000; ++i) {
        list.Exist(i, &node);
        if(node) {
            std::cout << i << std::endl;
        }

        node = nullptr;
        //assert(node);
        //std::cout << node->GetKey() << " : " << node->GetValue() << std::endl;
    }

    std::cout << "SkipListTest end!" << std::endl;
};

void InsertThreadEntry(int threadId ,int start, int end) {
    //10w个key太耗时了，插入+删除10w个key需要几分钟
    for(int i = start; i <= end; ++i) {
        list.Insert(i, std::to_string(i), SkipList::NodeFlag::NODE_ADD);
        //std::cout << threadId << " insert " << i << "to list" << std::endl;
    }
}

void SearchThreadEntry(int threadId ,int start, int end) {
    //10w个key太耗时了，插入+删除10w个key需要几分钟
    SkipList::Node* node = nullptr;
    for(int i = start; i <= end; ++i) {
        list.Exist(i, &node);
/*        if(!node) {
            std::cout << i << std::endl;
        }*/

        node = nullptr;
        //assert(node);
        //std::cout << node->GetKey() << " : " << node->GetValue() << std::endl;
    }
}


void SkipListTestOfMulitThread() {
    std::cout << "SkipListTestOfMulitThread begin!" << std::endl;

    std::thread insertThread1(InsertThreadEntry , 1, 1, 10000);
    std::thread insertThread2(InsertThreadEntry , 2, 10001, 20000);
    //std::thread insertThread3(InsertThreadEntry , 3, 20001, 30000);
    std::thread searchThread1(SearchThreadEntry , 3, 1, 10000);
    std::thread searchThread2(SearchThreadEntry , 4, 10001, 20000);

    insertThread1.join();
    insertThread2.join();
    searchThread1.join();
    searchThread2.join();

    std::cout << "Insert complete!" << std::endl;

    //std::this_thread::sleep_for(std::chrono::seconds(60));


    SkipList::Node* node = nullptr;
    for(int i = 1; i <= 20000; ++i) {
        list.Exist(i, &node);
        if(!node) {
            std::cout << i << std::endl;
        }

        assert(node);
        node = nullptr;
        //assert(node);
        //std::cout << node->GetKey() << " : " << node->GetValue() << std::endl;
    }

    std::cout << "SkipListTestOfMulitThread end!" << std::endl;
};



int main() {
//    lsmtree::SkipList<int, std::string, Compare> list;
//
//    for(int i = 0; i < 10; ++i) {
//        list.Insert(i, "test", lsmtree::SkipList<int, std::string, Compare>::NodeFlag::NODE_ADD);
//    }
//
//    lsmtree::SkipList<int, std::string, Compare>::Iterator iter(&list);
//    iter.SeekToFirst();
//
//    while(iter.Valid()) {
//        std::cout << *iter << std::endl;
//        iter.Next();
//    }

    //NodeTestOfSigleThread();

    //SkipListTestOfSigleThread();
    SkipListTestOfMulitThread();

    return 0;
}

