//
// Created by 123456 on 2023/3/13.
//

#include "bloom.h"
#include <filesystem>
#include <reader.h>
#include <writer.h>
#include <iostream>


#define MIX_UINT64(v)   ((uint32_t)((v>>32)^(v)))

namespace {
    const char* bloom_file = "/home/naruto/StudyDir/SimpleLSMTree/db/bloom_content";

    uint64_t MurmurHash2_x64 (const void* key, int len, uint32_t seed)
    {
        const uint64_t m = 0xc6a4a7935bd1e995;
        const int r = 47;

        uint64_t h = seed ^ (len * m);

        const uint64_t * data = (const uint64_t *)key;
        const uint64_t * end = data + (len/8);

        while(data != end)
        {
            uint64_t k = *data++;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        const uint8_t * data2 = (const uint8_t*)data;

        switch(len & 7)
        {
            case 7: h ^= ((uint64_t)data2[6]) << 48;
            case 6: h ^= ((uint64_t)data2[5]) << 40;
            case 5: h ^= ((uint64_t)data2[4]) << 32;
            case 4: h ^= ((uint64_t)data2[3]) << 24;
            case 3: h ^= ((uint64_t)data2[2]) << 16;
            case 2: h ^= ((uint64_t)data2[1]) << 8;
            case 1: h ^= ((uint64_t)data2[0]);
                h *= m;
        };

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }
}

SimpleBloomFilter::SimpleBloomFilter(uint64_t expect_key_num, uint8_t hash_func_nums) : bit_nums_(expect_key_num * 10), hash_func_nums_(hash_func_nums) {
    byte_nums_ = bit_nums_ / 8 == 0 ? bit_nums_ / 8 : bit_nums_ / 8 + 1;
    begin_ = new char[byte_nums_];
    cur_hash_bit_loc_.resize(hash_func_nums_, 0);

    std::filesystem::path bloom_path(bloom_file);
    if (std::filesystem::exists(bloom_path)) {
        if (LoadToMem(bloom_file) == -1) {
            std::cerr << "load bloom file to mem failed!" << std::endl;
        }
    }
}

int SimpleBloomFilter::LoadToMem(const char *filename) {
    Reader reader_handle;
    std::ifstream& reader = reader_handle.GetReader();

    reader.open(filename, std::ios::binary | std::ios::in);
    if (!reader) {
        return -1;
    }

    reader.read(begin_, byte_nums_);
    if (reader.fail()) {
       return -1;
    }

    return 0;
}

SimpleBloomFilter::~SimpleBloomFilter() {
    Writer writer_handle;
    std::ofstream& writer = writer_handle.GetWriter();

    writer.open(bloom_file, std::ios::binary | std::ios::in | std::ios::trunc);
    if (writer) {
        writer.write(begin_, byte_nums_);
    }

    delete[] begin_;
}

//、双重散列封装
void SimpleBloomFilter::BloomHash(const void *key, int len) {
    uint64_t hash1 = MurmurHash2_x64(key, len, 0xdeedbeef);
    uint64_t hash2 = MurmurHash2_x64(key, len, MIX_UINT64(hash1));
    for (int i = 0; i < hash_func_nums_; i++)
    {
        // k0 = (hash1 + 0*hash2) % dwFilterBits; // dwFilterBits bit向量的长度
        // k1 = (hash1 + 1*hash2) % dwFilterBits;
        cur_hash_bit_loc_[i] = (hash1 + i*hash2) % bit_nums_;
    }
}

void SimpleBloomFilter::Insert(const std::string &key) {
    BloomHash(key.data(), key.size());
    for (auto bit_loc : cur_hash_bit_loc_) {
        Setbit(bit_loc);
    }
}

bool SimpleBloomFilter::MayExist(const std::string &key) {
    BloomHash(key.data(), key.size());
    for (auto bit_loc : cur_hash_bit_loc_) {
        if (!IsSetbit(bit_loc)) {
            return false;
        }
    }

    return true;
}

void SimpleBloomFilter::Setbit(uint64_t bit_loc) {
    begin_[bit_loc / 8] |= (1 << (bit_loc % 8));
}

bool SimpleBloomFilter::IsSetbit(uint64_t bit_loc) {
    if (begin_[bit_loc / 8] & (1 << (bit_loc % 8))) {
        return true;
    }

    return false;
}
