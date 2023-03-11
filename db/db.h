//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_DB_H
#define SIMPLELSMTREE_DB_H

#include <string_view>

namespace lsmtree {

///Interface(abstract base class)
class Db {
public:
    virtual void Open() = 0;

    virtual void Close() = 0;

    virtual void Add(const std::string_view& key, const std::string_view& value) = 0;

    virtual void Delete(const std::string_view& key) = 0;

    virtual void Get(const std::string_view& key, std::string_view& value) = 0;
};

};  //namespace lsmtree

#endif //SIMPLELSMTREE_DB_H
