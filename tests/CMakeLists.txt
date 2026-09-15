find_package(GTest REQUIRED)

add_executable(rdtp_tests test_rdtp.cpp)
target_link_libraries(rdtp_tests PRIVATE rdtp GTest::gtest_main Threads::Threads)
include(GoogleTest)
gtest_discover_tests(rdtp_tests)
