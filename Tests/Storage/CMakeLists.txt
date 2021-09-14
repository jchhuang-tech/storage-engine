add_executable(basefile_test basefile_test.cc)
target_link_libraries(basefile_test gtest glog gflags basefile pthread) 
add_test(NAME basefile_test COMMAND basefile_test)
add_custom_target(basefile_test_custom COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS basefile_test)
