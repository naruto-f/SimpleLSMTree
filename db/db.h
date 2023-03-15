//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_DB_H
#define SIMPLELSMTREE_DB_H

#include <memory>

namespace lsmtree {

///Interface(abstract base class)
class Db {
public:
    //virtual void Open() = 0;

    virtual void Close() = 0;

    virtual bool Add(const std::string& key, const std::string& value) = 0;

    virtual bool Delete(const std::string& key) = 0;

    virtual bool Get(const std::string& key, std::shared_ptr<std::string>& value) = 0;

    virtual ~Db() { }
};

};  //namespace lsmtree

#endif //SIMPLELSMTREE_DB_H
