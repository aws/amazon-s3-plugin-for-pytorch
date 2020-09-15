find_package(AWSSD REQUIRED COMPONENTS transfer s3-encryption dynamodb)
target_link_libraries(target ${AWSSDK_LINK_LIBRARIES})

