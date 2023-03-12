//
// Created by 123456 on 2023/3/12.
//

#include <cache.h>
#include <iostream>


int main() {
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

    return 0;
}

