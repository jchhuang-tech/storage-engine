add_library(indexmanager skiplist.cc pskiplist.cc)
target_link_libraries(indexmanager table file)
