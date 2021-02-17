cmake_minimum_required(VERSION 3.13)
project(_pywrap_s3_io)

set(CMAKE_CXX_STANDARD 17)
SET(TORCH_MIN_VERSION "1.5.1")

find_package(Python3 COMPONENTS Interpreter Development)

find_package(AWSSDK REQUIRED COMPONENTS s3 transfer)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(INCLUDE_DIRS "awsio/csrc/io/s3")

set(SOURCES "${INCLUDE_DIRS}/s3_io.cpp" )

include_directories(${INCLUDE_DIRS})
find_package(pybind11 REQUIRED)
pybind11_add_module(_pywrap_s3_io ${SOURCES} "${INCLUDE_DIRS}/s3_file_wrapper.cpp")

Message(STATUS "All linked libs: ${AWSSDK_LINK_LIBRARIES}")

target_link_libraries(_pywrap_s3_io PRIVATE ${AWSSDK_LINK_LIBRARIES} ${AWSSDK_PLATFORM_DEPS})
