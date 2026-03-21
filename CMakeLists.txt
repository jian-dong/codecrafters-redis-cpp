cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)
set(REDIS_SOURCES ${SOURCE_FILES})
list(REMOVE_ITEM REDIS_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)
find_package(asio CONFIG REQUIRED)

add_executable(redis ${SOURCE_FILES})
target_include_directories(redis PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(redis PRIVATE asio asio::asio)
target_link_libraries(redis PRIVATE Threads::Threads)

enable_testing()

add_executable(redis_tests tests/command_processor_test.cpp ${REDIS_SOURCES})
target_include_directories(redis_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(redis_tests PRIVATE asio asio::asio)
target_link_libraries(redis_tests PRIVATE Threads::Threads)

add_test(NAME redis_tests COMMAND redis_tests)
