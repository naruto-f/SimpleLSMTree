//
// Created by 123456 on 2023/3/13.
//

#include "file.h"

#include <filesystem>

int FileOperator::FindLargestSuffix(const char* directory_name, const std::string &filename_prefix) {
    std::filesystem::path dir_name(directory_name);
    if (!std::filesystem::exists(dir_name)) {
        return -1;
    }

    int max_suffix = 0;
    std::filesystem::directory_entry entry(dir_name);

    while (true) {
        std::filesystem::path filename(filename_prefix + std::to_string(max_suffix));
        if (!std::filesystem::exists(dir_name)) {
            break;
        }

        ++max_suffix;
    }

    return max_suffix - 1;
}
