//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_CACHE_H
#define SIMPLELSMTREE_CACHE_H

#include <memory>
#include <unordered_map>
#include <mutex>


#include <string_view>
#include <cassert>


namespace lsmtree {

//class Block;

///the memory cache template class use LRU

template<typename Key, typename Value>
class Cache {
public:
    Cache(uint32_t capacity)
         : capacity_(capacity), size_(0), head_(new Node()), tail_(new Node()) {
        head_->next_ = tail_;
        tail_->prev_ = head_;
    }

    ~Cache() {
        Node* cur = head_->next_;
        while (cur != tail_) {
            Node* next = cur->next_;
            delete cur;
            cur = next;
        }

        delete head_;
        delete tail_;
    }

    void Insert(Key key, Value value);

    bool Search(Key key, Value& value);

    uint32_t Size() { return size_; }
private:
    struct Node {
        Node(Key key = Key{}, Value value = Value{})
             : key_(key), value_(value), next_(nullptr), prev_(nullptr) {}

        Key key_;
        Value value_;
        Node* next_;
        Node* prev_;
    };

    void MoveNodeToHead(Node* target);

    void DeleteTailNode();

    void AddNewNode(const Key& key, const Value& value);
private:
    uint32_t capacity_;
    uint32_t size_;
    std::unordered_map<Key, Node*> map_;
    Node* head_;
    Node* tail_;
    mutable std::mutex mutex_;
};

template<typename Key, typename Value>
void Cache<Key, Value>::Insert(Key key, Value value) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (map_.count(key)) {
        Node* target = map_[key];
        target->value_ = value;
        MoveNodeToHead(target);
    } else {
        if (size_ == capacity_) {
            DeleteTailNode();
        }

        AddNewNode(key, value);
    }
}

template<typename Key, typename Value>
bool Cache<Key, Value>::Search(Key key, Value &value) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!map_.count(key)) {
        return false;
    }

    Node* target = map_[key];
    value = target->value_;
    MoveNodeToHead(target);

    return true;
}

template<typename Key, typename Value>
void Cache<Key, Value>::MoveNodeToHead(Node* target) {
    assert(target);

    //broken links
    target->prev_->next_ = target->next_;
    target->next_->prev_ = target->prev_;

    //insert to list head
    head_->next_->prev_ = target;
    target->next_ = head_->next_;
    head_->next_ = target;
    target->prev_ = head_;
}

template<typename Key, typename Value>
void Cache<Key, Value>::DeleteTailNode() {
    assert(size_ >= 1);

    Node* target = tail_->prev_;
    target->prev_->next_ = target->next_;
    tail_->prev_ = target->prev_;
    map_.erase(target->key_);
    delete target;
    --size_;
}

template<typename Key, typename Value>
void Cache<Key, Value>::AddNewNode(const Key& key, const Value& value) {
    Node* target = new Node(key, value);

    map_.insert({key, target});
    head_->next_->prev_ = target;
    target->next_ = head_->next_;
    head_->next_ = target;
    target->prev_ = head_;

    ++size_;
}

};  //namespace lsmtree

#endif //SIMPLELSMTREE_CACHE_H
