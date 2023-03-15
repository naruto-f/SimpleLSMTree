//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_WRITER_H
#define SIMPLELSMTREE_WRITER_H

#include <fstream>


///this handler class just for RAII
class Writer {
public:
    ~Writer() {
        if (writer_.is_open()) {
            writer_.close();
        } else if (writer_.fail()) {
            //writer_.clear();
            writer_.close();
        }
    }

    std::fstream& GetWriter() {
        return writer_;
    }
private:
    std::fstream writer_;
};


#endif //SIMPLELSMTREE_WRITER_H
