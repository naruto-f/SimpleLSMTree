add_executable(skiplist_test skiplist_test.cpp )
target_link_libraries(skiplist_test pthread)

add_executable(memtable_test memtable_test.cpp ../db/memtable.cpp ../utils/writer.cpp ../utils/writer.h ../utils/reader.cpp ../utils/reader.h ../table/table.cpp ../table/block.cpp ../db/cache.cpp ../db/cache.h ../db/db.h ../db/lsmtree.cpp ../db/lsmtree.h)
target_link_libraries(memtable_test pthread)