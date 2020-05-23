cmake_minimum_required(VERSION 3.13)
project(OS_EX3)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS -pthread)
add_executable(OS_EX3 main.cpp MapReduceFramework.cpp)