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
file(GLOB CPP_SOURCES awsio/csrc/io/s3/*.cpp)
file(GLOB PY_SOURCES awsio/python/lib/io/s3/*.cpp)
add_library(aws_io SHARED ${CPP_SOURCES} ${PY_SOURCES})

find_package(pybind11 REQUIRED)
pybind11_add_module(_pywrap_s3_io ${PY_SOURCES})

#add_executable(aws_io main.cpp)
target_compile_features(aws_io PUBLIC cxx_std_11)

target_link_libraries(aws_io ${AWSSDK_LINK_LIBRARIES} absl::strings Python3::Python)

target_include_directories(aws_io PRIVATE awsio)
