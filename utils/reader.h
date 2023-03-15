//
// Created by 123456 on 2023/3/11.
//

#ifndef SIMPLELSMTREE_READER_H
#define SIMPLELSMTREE_READER_H

#include <fstream>

///this handler class just for RAII
class Reader {
public:
    ~Reader() {
        if (reader_.is_open()) {
            reader_.close();
        }
    }

    std::fstream& GetReader() {
        return reader_;
    }
private:
    std::fstream reader_;
};


#endif //SIMPLELSMTREE_READER_H
