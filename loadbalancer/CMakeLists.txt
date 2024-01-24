cmake_minimum_required(VERSION 3.20)

project(LoadBalancer LANGUAGES CXX)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(Python3 REQUIRED COMPONENTS Development)
find_package(Boost COMPONENTS python REQUIRED)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED on)


add_library(consistent_hasher MODULE src/consistent_hasher.cpp)
set_target_properties(consistent_hasher PROPERTIES PREFIX "")
target_link_libraries(consistent_hasher ${Boost_LIBRARIES} ${Python3_Libraries})
target_include_directories(consistent_hasher PRIVATE include ${Python3_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS})

# copy the output module to bin directory after compilation
add_custom_command(TARGET consistent_hasher POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:consistent_hasher> ${CMAKE_SOURCE_DIR}/)