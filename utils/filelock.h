//
// Created by 123456 on 2023/3/14.
//

#ifndef SIMPLELSMTREE_FILELOCK_H
#define SIMPLELSMTREE_FILELOCK_H

#include <fstream>
#include <fcntl.h>


class FileLock {
public:
    FileLock(const std::fstream& file, short lock_type);

    bool LockWithoutWait();

    void Lock();

    ~FileLock();
private:
    int fd_;
    struct flock lock_;
};


#endif //SIMPLELSMTREE_FILELOCK_H
