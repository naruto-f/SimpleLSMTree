//
// Created by 123456 on 2023/3/14.
//

#include "filelock.h"

namespace {
    auto helper = [](std::filebuf& fb) -> int {
        class Helper : public std::filebuf {
        public:
            int handle() { return _M_file.fd(); }
        };

        return static_cast<Helper&>(fb).handle();
    };
}

FileLock::FileLock(const std::fstream &file, short lock_type) : fd_(helper(*file.rdbuf())) {
    //set for lock all file
    lock_.l_whence = SEEK_SET;
    lock_.l_len = 0;
    lock_.l_start = 0;

    lock_.l_type = lock_type;
}

FileLock::~FileLock() {
    fcntl(fd_, F_GETLK, &lock_);
    if (lock_.l_type != F_UNLCK) {
        lock_.l_type = F_UNLCK;
        fcntl(fd_, F_SETLK, &lock_);
    }
}

bool FileLock::LockWithoutWait() {
    if (fcntl(fd_, F_SETLK, &lock_) == -1) {
        return false;
    }

    return true;
}

void FileLock::Lock() {
    fcntl(fd_, F_SETLKW, &lock_);
}
