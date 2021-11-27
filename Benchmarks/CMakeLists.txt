add_library(perf perf.cc)
add_executable(simple_bench simple_bench.cc)
target_link_libraries(simple_bench gtest glog gflags perf buffermanager indexmanager lockmanager logmanager)

