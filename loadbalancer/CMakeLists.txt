cmake_minimum_required(VERSION 3.20)

project(LoadBalancer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED on)

add_library(consistentHasher src/consistent_hasher.cpp)
target_include_directories(consistentHasher PUBLIC include)