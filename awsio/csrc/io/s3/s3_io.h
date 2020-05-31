//
// Created by Nagmote, Roshani on 5/12/20.
//

#ifndef AWSIO_S3_IO_H
#define AWSIO_S3_IO_H

#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>
#include <mutex>

namespace awsio {
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

    class S3Init {
    private:
        std::shared_ptr <Aws::S3::S3Client> s3_client_;
	std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> executor_;
        std::shared_ptr <Aws::Transfer::TransferManager> transfer_manager_;

    public:
        S3Init();

        ~S3Init();

        std::mutex initialization_lock_;

        std::shared_ptr <Aws::S3::S3Client> initializeS3Client();
        std::shared_ptr <Aws::Utils::Threading::PooledThreadExecutor> initializeExecutor();
        std::shared_ptr <Aws::Transfer::TransferManager> initializeTransferManager();
        
        void s3_read(const std::string &file_url, bool use_tm);
    };
}

#endif //AWSIO_S3_IO_H

