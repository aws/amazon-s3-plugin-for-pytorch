//
// Created by Nagmote, Roshani on 5/12/20.
//

#ifndef AWSIO_S3_IO_H
#define AWSIO_S3_IO_H

#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>

namespace {


// In memory stream implementation
// AWS Streams destroy the buffer (buf) passed, so creating a new
// IOStream that retains the buffer so the calling function
// can control it's lifecycle
    class S3UnderlyingStream : public Aws::IOStream {
    public:
        using Base = Aws::IOStream;

        // provide a customer controlled streambuf, so as to put all transferred data into this in memory buffer.
        S3UnderlyingStream(std::streambuf *buf) : Base(buf) {}

        virtual ~S3UnderlyingStream() = default;
    };

}
#endif //AWSIO_S3_IO_H

