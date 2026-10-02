cmake_minimum_required(VERSION 3.16)
project(RDTP LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(Boost REQUIRED COMPONENTS system filesystem program_options)
find_package(Threads REQUIRED)

add_library(rdtp
    src/rdtp_client.cpp
    src/rdtp_server.cpp
    src/rdtp_wire.cpp
)

target_include_directories(rdtp PUBLIC include)
target_link_libraries(rdtp PUBLIC Boost::system Boost::filesystem Boost::program_options)

add_executable(rdtp_server examples/server.cpp)
target_link_libraries(rdtp_server PRIVATE rdtp Threads::Threads)

add_executable(rdtp_client examples/client.cpp)
target_link_libraries(rdtp_client PRIVATE rdtp)

# Unit tests
enable_testing()
add_subdirectory(tests)
