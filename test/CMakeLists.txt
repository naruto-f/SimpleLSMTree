

add_executable(skiplist_test skiplist_test.cpp )
target_link_libraries(skiplist_test pthread)

add_executable(memtable_test memtable_test.cpp ../db/memtable.cpp ../utils/writer.cpp ../utils/writer.h ../utils/reader.cpp ../utils/reader.h)
target_link_libraries(memtable_test pthread)


add_executable(cache_test cache_test.cpp )
target_link_libraries(cache_test pthread)


add_executable(smoke_test smoke_test.cpp ../db/lsmtree.cpp ../db/memtable.cpp ../db/cache.cpp ../table/block.cpp ../table/table.cpp ../utils/bloom.cpp ../utils/reader.cpp ../utils/writer.cpp ../utils/filelock.cpp)
target_link_libraries(smoke_test pthread)