//
// Created by 123456 on 2023/3/12.
//

#include <cache.h>
#include <iostream>
#include <string_view>


void test() {
    lsmtree::Cache<int, int> cache(2);

    int value = 0;
    bool res = cache.Search(0, value);
    assert(!res);

    cache.Insert(0, 0);
    res = cache.Search(0, value);
    assert(res && value == 0);

    cache.Insert(1, 1);
    res = cache.Search(1, value);
    assert(res && value == 1);

    cache.Insert(2, 2);
    res = cache.Search(0, value);
    assert(!res);

    std::cout << cache.Size();
}

int main() {
    const char* a = "123456";
    char b[10] = "123456";

    std::string_view av(a, 6);
    std::string_view bv(b, 6);

    if (av == bv) {
        std::cout << "true" << std::endl;
    } else {
        std::cout << "false" << std::endl;
    }


    //std::cout << av > bv ? "true" : "false" << std::endl;

    return 0;
}

