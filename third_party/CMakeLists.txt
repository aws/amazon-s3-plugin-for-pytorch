cmake_minimum_required(VERSION 3.1)

if(POLICY CMP0054)
    cmake_policy(SET CMP0054 NEW)
endif()

project(AWSIO_DEPS)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

include(ExternalProject)

set(AWS_DEPS_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}" CACHE PATH "Dependencies install directory.")
set(AWS_DEPS_BUILD_DIR "${CMAKE_BINARY_DIR}/build" CACHE PATH "Dependencies build directory.")
set(AWS_DEPS_DOWNLOAD_DIR "${AWS_DEPS_BUILD_DIR}/downloads" CACHE PATH "Dependencies download directory.")

set(AWS_C_COMMON_URL "https://github.com/awslabs/aws-c-common.git")
set(AWS_C_COMMON_TAG "v0.4.15")
include(BuildAwsSDK)

add_dependencies(AwsSDK)