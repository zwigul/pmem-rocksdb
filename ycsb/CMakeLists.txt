cmake_minimum_required(VERSION 3.10)

project(rocksdb-ycsb)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_FLAGS "-Wall -Wsign-compare -O3 -g -DNDEBUG")

##
# Compiler options
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wfatal-errors ")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DON_DCPMM")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17 -march=native -fno-rtti ")

#include_directories(../build/include)
#include_directories(${PROJECT_SOURCE_DIR})
link_directories(../build/lib)

add_executable(ycsb
  src/main.cc
  src/rocksdb_client.cc
  core/core_workload.cc
  )
#../build/lib/librocksdb.a

target_include_directories(ycsb PRIVATE
  ../build/include
  ${PROJECT_SOURCE_DIR}
  )
target_link_libraries(ycsb
  pthread
  stdc++fs
  numa
  rocksdb
  pmem
  pmemobj
  dl
  zstd
  lz4
  bz2
  snappy
  z)
