add_executable(skiplist_test skiplist_test.cpp )
target_link_libraries(skiplist_test pthread)

add_executable(memtable_test memtable_test.cpp ../db/memtable.cpp)
