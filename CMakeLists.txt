cmake_minimum_required(VERSION 3.13)
project(aws_io)

set(CMAKE_CXX_STANDARD 14)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

find_package(Python3 COMPONENTS Development)

find_package(Torch REQUIRED)

find_package(AWSSDK REQUIRED COMPONENTS s3)

find_package(absl REQUIRED)
#add_executable(aws_io main.cpp)

file(GLOB HEADERS awsio/csrc/io/s3/*.h)
file(GLOB SOURCES awsio/csrc/io/s3/*.cpp)
add_library(aws_io SHARED ${SOURCES})

#add_executable(aws_io main.cpp)
target_compile_features(aws_io PUBLIC cxx_std_11)

target_link_libraries(aws_io ${AWSSDK_LINK_LIBRARIES} absl::strings)
#target_link_libraries(aws_io PRIVATE ${TORCH_LIBRARIES} Python3::Python)
