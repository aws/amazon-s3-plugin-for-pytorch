cmake_minimum_required(VERSION 3.13)
project(_pywrap_s3_io)

set(CMAKE_CXX_STANDARD 17)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

find_package(Python3 COMPONENTS Development)

find_package(Torch REQUIRED)

find_package(AWSSDK REQUIRED COMPONENTS s3 transfer)

find_package(absl REQUIRED)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(INCLUDE_DIRS "awsio/csrc/io/s3")

set(SOURCES "${INCLUDE_DIRS}/s3_io.cpp" )

include_directories(${INCLUDE_DIRS})
find_package(pybind11 REQUIRED)
pybind11_add_module(_pywrap_s3_io ${SOURCES} "${INCLUDE_DIRS}/s3_file_wrapper.cpp")
target_link_libraries(_pywrap_s3_io PRIVATE ${AWSSDK_LINK_LIBRARIES} absl::strings Python3::Python)
