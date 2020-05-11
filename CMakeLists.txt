cmake_minimum_required(VERSION 3.13)
project(aws_io)

set(CMAKE_CXX_STANDARD 14)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

find_package(Python3 COMPONENTS Development)

#set(Torch_DIR "/home/ubuntu/anaconda3/envs/pytorch_p36/lib/python3.6/site-packages/torch")
find_package(Torch REQUIRED)

#set(aws-sdk-cpp_DIR "$ENV{HOME}/aws-sdk-cpp/build")
#find_package(aws-sdk-cpp REQUIRED COMPONENTS s3)
#find_package(aws-sdk-cpp)

#file(GLOB HEADERS awsio/csrc/io/*.h)
find_package(AWSSDK REQUIRED COMPONENTS s3)
#target_link_libraries(${PROJECT_NAME} PRIVATE ${TORCH_LIBRARIES} Python3::Python)

add_executable(aws_io main.cpp)

target_compile_features(aws_io PUBLIC cxx_std_11)

target_link_libraries(aws_io ${AWSSDK_LINK_LIBRARIES})

#target_link_libraries(awsio aws-cpp-sdk-core)
