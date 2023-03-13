//
// Created by 123456 on 2023/3/13.
//

#ifndef SIMPLELSMTREE_BLOOM_H
#define SIMPLELSMTREE_BLOOM_H

#include <cstdint>
#include <string>
#include <vector>

// TODO : implement a complete and robust bloom filter

///just a simple filter for 10000000 key nums, not all implement and not calculate accurate false positive rates
class SimpleBloomFilter {
public:
    SimpleBloomFilter(uint64_t expect_key_num, uint8_t hash_func_nums);

    ~SimpleBloomFilter();

    void Insert(const std::string& key);

    ///return false if key must not exist
    bool MayExist(const std::string& key);
private:
    int LoadToMem(const char* filename);

    void BloomHash(const void* key, int len);

    void Setbit(uint64_t bit_loc);

    bool IsSetbit(uint64_t bit_loc);
private:
    uint64_t bit_nums_;
    uint64_t byte_nums_;
    char* begin_;
    uint8_t hash_func_nums_;
    std::vector<uint64_t> cur_hash_bit_loc_;
};


#endif //SIMPLELSMTREE_BLOOM_H
