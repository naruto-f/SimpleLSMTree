add_executable(skiplist_test skiplist_test.cpp )
target_link_libraries(skiplist_test pthread)

add_executable(memtable_test memtable_test.cpp ../db/memtable.cpp ../utils/writer.cpp ../utils/writer.h ../utils/reader.cpp ../utils/reader.h ../table/table.cpp ../table/block.cpp ../db/cache.cpp ../db/cache.h ../db/db.h ../db/lsmtree.cpp ../db/lsmtree.h cache_test.cpp ../utils/file.cpp ../utils/file.h ../utils/bloom.cpp ../utils/filelock.cpp)
target_link_libraries(memtable_test pthread)


add_executable(cache_test cache_test.cpp )
target_link_libraries(cache_test pthread)