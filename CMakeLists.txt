cmake_minimum_required(VERSION 3.14)
project(raft_kv CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find threads
find_package(Threads REQUIRED)

include_directories(include)

set(RAFT_SOURCES
    src/raft.cpp
    src/log.cpp
    src/kv_store.cpp
)

# Main demo executable
add_executable(raft_demo
    ${RAFT_SOURCES}
    src/main.cpp
)
target_link_libraries(raft_demo Threads::Threads)

# Test executable
add_executable(raft_tests
    ${RAFT_SOURCES}
    tests/test_main.cpp
    tests/test_election.cpp
    tests/test_replication.cpp
    tests/test_persistence.cpp
)
target_link_libraries(raft_tests Threads::Threads)

# Compiler flags
target_compile_options(raft_demo PRIVATE -Wall -Wextra -O2)
target_compile_options(raft_tests PRIVATE -Wall -Wextra -O2)
