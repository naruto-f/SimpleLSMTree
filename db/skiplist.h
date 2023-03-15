//
// Created by 123456 on 2023/3/8.
//
//

#ifndef SIMPLELSMTREE_SKIPLIST_H
#define SIMPLELSMTREE_SKIPLIST_H

#include <atomic>
#include <algorithm>
#include <memory>
#include <cassert>
#include <random>
#include <cstring>

namespace lsmtree {

template<typename Key, typename Value, typename Comparator>
class SkipList {
public:
    struct Node;

public:
    enum class NodeFlag : uint8_t {
        NODE_ADD = 0,
        NODE_DELETE
    };

    explicit SkipList(Comparator cmp = Comparator{})
            : cur_max_level_(0), head_(new Node(NodeFlag::NODE_ADD, kMaxLevel - 1)), cmp_(cmp) {}

    ~SkipList() {
        Node* cur = head_->GetNextWithSync(0);
        while(cur) {
            Node* next = cur->GetNextWithSync(0);
            delete cur;
            cur = next;
        }

        delete head_;
    }

    SkipList(const SkipList& rhs) = delete;
    SkipList& operator=(const SkipList& rhs) = delete;

    ///Insert key/value pair to skiplist that it's key not exists in the skiplist
    void Insert(const Key& key, const Value& value, NodeFlag flag);

    ///Returns whether the specified key value already exists in the skiplist
    bool Exist(const Key& key, Node** target) const ;
public:
    class Iterator {
    public:
        ///The beginning status of Iterator is invalid.
        Iterator(const SkipList* skiplist) : skiplist_(skiplist), cur_(nullptr) { }

        ///Seek to the first position that key >= target
        void Seek(const Key& target) {
            cur_ = skiplist_->FindGreaterOrEqual(target, nullptr);
        }

        bool Valid() {
            return cur_ != nullptr;
        }

        void Next() {
            assert(Valid());
            cur_ = cur_->GetNextWithSync(0);
        }

        void Prev() {
            assert(Valid());
            cur_ = skiplist_->FindLessThan();
            if (cur_ == skiplist_->head_) {
               cur_ = nullptr;
            }
        }

        void SeekToFirst() {
            cur_ = skiplist_->head_->GetNextWithoutSync(0);
        }

        void SeekToLast() {
            cur_ = skiplist_->FindLast();
            if (cur_ == skiplist_->head_) {
                cur_ = nullptr;
            }
        }

        Node* operator*() {
            return cur_;
        }

    private:
        const SkipList* skiplist_;
        Node* cur_;
    };

private:
    enum : uint8_t {
        kMaxLevel = 18
    };

    bool Equal(const Key& lhs, const Key& rhs) const { return cmp_(lhs, rhs) == 0; }
    bool KeyIsAfterNode(const Key& key, Node* node) const {
        if(!node) {
            return false;
        }

        return cmp_(key, node->GetKey()) > 0 ? true : false;
    }

    ///Return the first node which it's key >= target key if node exist, else return nullptr.
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

    ///Return the last node in the skiplist, Return nullptr if skiplist is empty.
    Node* FindLast() const;

    Node* FindLessThan(const Key& key) const;

    int GetCurMaxLevel() const { return cur_max_level_.load(std::memory_order_relaxed); }

    int GetRandomLevel() {
        std::random_device rd;
        //std::mt19937 gen(rd());

        std::array<int, std::mt19937::state_size> seed_data{};
        std::generate(std::begin(seed_data), std::end(seed_data), std::ref(rd));
        std::seed_seq seq(std::begin(seed_data), std::end(seed_data));
        auto eng = std::mt19937{seq};
        std::uniform_int_distribution<int> distrib(0, 1);

        int level = 0;
        while (level < kMaxLevel - 1 && distrib(eng) == 1) {
            ++level;
        }

        return level;
    }


    std::atomic<int> cur_max_level_;
    //std::unique_ptr<Node> head_;
    Node* head_;
    Comparator cmp_;
};

template<typename Key, typename Value, typename Comparator>
struct SkipList<Key, Value, Comparator>::Node {
    Node(NodeFlag flag, int level, Key key = Key{}, Value value = Value{})
        : key_(key), value_(value), flag_(flag), kMaxLevel_(level) {
        for (int i = 0; i <= kMaxLevel_; ++i) {
            SetNextWithoutSync(i, nullptr);
        }
    }

    Node(const Node& rhs) = delete;
    Node& operator=(const Node& rhs) = delete;

    const Key& GetKey() { return key_; }

    const Value& GetValue() { return value_; }
    void SetValue(const Value& value) { value_ = value; }

    NodeFlag GetFlag() { return flag_; }
    void SetFlag(NodeFlag flag) { flag_ = flag; }

    Node* GetNextWithSync(int level) {
        assert(level >= 0 && level <= kMaxLevel_);

        return next_[level].load(std::memory_order_acquire);
    }

    void SetNextWithSync(int level, Node* next) {
        assert(level >= 0 && level <= kMaxLevel_);

        next_[level].store(next, std::memory_order_release);
    }

    ///The following two functions only can use in a few position that you believe doesn't need sync at all
    Node* GetNextWithoutSync(int level) {
        assert(level >= 0 && level <= kMaxLevel_);

        return next_[level].load(std::memory_order_relaxed);
    }

    void SetNextWithoutSync(int level, Node* next) {
        assert(level >= 0 && level <= kMaxLevel_);

        next_[level].store(next, std::memory_order_relaxed);
    }

private:
    std::atomic<Node*> next_[kMaxLevel];
    Key const key_;
    //std::unique_ptr<Value> value_;
    Value value_;
    NodeFlag flag_;
    const int kMaxLevel_;
};

template<typename Key, typename Value, typename Comparator>
typename SkipList<Key, Value, Comparator>::Node* SkipList<Key, Value, Comparator>::FindGreaterOrEqual(const Key &key, SkipList::Node **prev) const {
    int level = GetCurMaxLevel();
    Node* cur = head_;

    while(true) {
        Node* next = cur->GetNextWithSync(level);

        if (KeyIsAfterNode(key, next)) {
            cur = next;
        } else {
            if (prev) {
                prev[level] = cur;
            }

            if (level == 0)
            {
                return next;
            }
            else {
                --level;
            }
        }
    }
}

template<typename Key, typename Value, typename Comparator>
void SkipList<Key, Value, Comparator>::Insert(const Key &key, const Value &value, NodeFlag flag) {
    int level = GetRandomLevel();
    assert(level < kMaxLevel);

    Node* prev[kMaxLevel] = { nullptr };
    Node* target = FindGreaterOrEqual(key, prev);
    assert(target == nullptr || !Equal(target->GetKey(), key));

    if (level > GetCurMaxLevel()) {
        for (int i = GetCurMaxLevel() + 1; i <= level; ++i)
        {
            prev[i] = head_;
        }

        cur_max_level_.store(level, std::memory_order_relaxed);
    }

    Node* node = new Node(flag, level, key, value);
    for (int i = 0; i <= level; ++i) {
        node->SetNextWithoutSync(i, prev[i]->GetNextWithSync(i));
        prev[i]->SetNextWithSync(i, node);
    }
}

template<typename Key, typename Value, typename Comparator>
typename SkipList<Key, Value, Comparator>::Node *SkipList<Key, Value, Comparator>::FindLast() const {
    int level = GetCurMaxLevel();
    Node* cur = head_;

    while (true) {
        Node* next = cur->GetNextWithSync(level);

        if (!next) {
            if (level == 0) {
                return cur;
            } else {
                --level;
            }
        } else {
            cur = next;
        }
    }
}

template<typename Key, typename Value, typename Comparator>
typename SkipList<Key, Value, Comparator>::Node *SkipList<Key, Value, Comparator>::FindLessThan(const Key& key) const {
    int level = GetCurMaxLevel();
    Node* cur = head_;

    while (true) {
        Node* next = cur->GetNextWithSync(level);

        if (!next || KeyIsAfterNode(key, next)) {
            if (level == 0) {
                return cur;
            } else {
                --level;
            }
        } else {
            cur = next;
        }
    }
}



template<typename Key, typename Value, typename Comparator>
bool SkipList<Key, Value, Comparator>::Exist(const Key &key, SkipList::Node** target) const {
    Node* node = FindGreaterOrEqual(key, nullptr);

    if (!node || !Equal(node->GetKey(), key)) {
        return false;
    }

    *target = node;
    return true;
}


}  //namespace lsmtree

#endif //SIMPLELSMTREE_SKIPLIST_H
