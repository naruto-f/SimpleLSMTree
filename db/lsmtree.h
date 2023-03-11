//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_LSMTREE_H
#define SIMPLELSMTREE_LSMTREE_H

#include <db.h>

namespace lsmtree {

class LsmTree : public Db {
public:
    void Add(const std::string_view& key, const std::string_view& value) override;

    void Delete(const std::string_view& key) override;

    void Get(const std::string_view& key, std::string_view& value) override;


private:


};

};  //namespace lsmtree

#endif //SIMPLELSMTREE_LSMTREE_H
