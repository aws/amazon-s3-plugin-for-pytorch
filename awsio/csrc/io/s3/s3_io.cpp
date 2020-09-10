//
// Created by Nagmote, Roshani on 5/12/20.
//
#include "s3_io.h"

#include <aws/core/Aws.h>
#include <aws/core/config/AWSProfileConfigLoader.h>
#include <aws/core/utils/FileSystemUtils.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/transfer/TransferManager.h>

#include <fstream>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace awsio {
namespace {
// static const char *kS3FileSystemAllocationTag = "S3FileSystemAllocation";
static const size_t s3ReadBufferSize = 16 * 1024 * 1024;               // 16 MB
static const uint64_t s3MultiPartDownloadChunkSize = 2 * 1024 * 1024;  // 50 MB
static const int downloadRetries = 3;
static const int64_t s3TimeoutMsec = 300000;
static const int executorPoolSize = 25;
static const int S3GetFilesMaxKeys = 100;

Aws::Client::ClientConfiguration &setUpS3Config() {
    static Aws::Client::ClientConfiguration cfg;
    Aws::String config_file;
    // If AWS_CONFIG_FILE is set then use it, otherwise use ~/.aws/config.
    const char *config_file_env = getenv("AWS_CONFIG_FILE");
    if (config_file_env) {
        config_file = config_file_env;
    } else {
        const char *home_env = getenv("HOME");
        if (home_env) {
            config_file = home_env;
            config_file += "/.aws/config";
        }
    }
    Aws::Config::AWSConfigFileProfileConfigLoader loader(config_file);
    loader.Load();

    const char *use_https = getenv("S3_USE_HTTPS");
    if (use_https) {
        if (use_https[0] == '0') {
            cfg.scheme = Aws::Http::Scheme::HTTP;
        } else {
            cfg.scheme = Aws::Http::Scheme::HTTPS;
        }
    }
    const char *verify_ssl = getenv("S3_VERIFY_SSL");
    if (verify_ssl) {
        if (verify_ssl[0] == '0') {
            cfg.verifySSL = false;
        } else {
            cfg.verifySSL = true;
        }
    }

    const char *region = getenv("AWS_REGION");
    if (region) {
        cfg.region = region;
    } else {
        cfg.region = "us-west-2";
    }
    return cfg;
}

void ShutdownClient(std::shared_ptr<Aws::S3::S3Client> *s3_client) {
    if (s3_client != nullptr) {
        delete s3_client;
        Aws::SDKOptions options;
        Aws::ShutdownAPI(options);
    }
}

void ShutdownTransferManager(
    std::shared_ptr<Aws::Transfer::TransferManager> *transfer_manager) {
    if (transfer_manager != nullptr) {
        delete transfer_manager;
    }
}

void parseS3Path(const std::string &fname, std::string *bucket,
                 std::string *object) {
    if (fname.empty()) {
        throw std::invalid_argument{"The filename cannot be an empty string."};
    }

    if (fname.size() < 5 || fname.substr(0, 5) != "s3://") {
        throw std::invalid_argument{
            "The filename must start with the S3 scheme."};
    }

    std::string path = fname.substr(5);

    if (path.empty()) {
        throw std::invalid_argument{"The filename cannot be an empty string."};
    }

    auto pos = path.find_first_of('/');
    if (pos == 0) {
        throw std::invalid_argument{
            "The filename does not contain a bucket name."};
    }

    *bucket = path.substr(0, pos);
    *object = path.substr(pos + 1);
    if (pos == std::string::npos) {
        *object = "";
    }
}

class S3FS {
   public:
    S3FS(const std::string &bucket, const std::string &object,
         const bool multi_part_download,
         std::shared_ptr<Aws::Transfer::TransferManager> transfer_manager,
         std::shared_ptr<Aws::S3::S3Client> s3_client)
        : bucket_name_(bucket),
          object_name_(object),
          multi_part_download_(multi_part_download),
          transfer_manager_(transfer_manager),
          s3_client_(s3_client) {}

    bool read(uint64_t offset, size_t n, char *buffer,
              StringContainer *result) {
        if (multi_part_download_) {
            return readS3TransferManager(offset, n, buffer, result);
        } else {
            return readS3Client(offset, n, buffer, result);
        }
    }

    bool readS3Client(uint64_t offset, size_t n, char *buffer,
                      StringContainer *result) {
        std::cout << "Read File from S3 s3://" << this->bucket_name_ << "/"
                  << this->object_name_ << " from " << offset << " for n:" << n
                  << std::endl;

        Aws::S3::Model::GetObjectRequest getObjectRequest;

        getObjectRequest.WithBucket(this->bucket_name_.c_str())
            .WithKey(this->object_name_.c_str());

        std::string bytes = absl::StrCat("bytes=", offset, "-", offset + n - 1);

        getObjectRequest.SetRange(bytes.c_str());

        // When you don’t want to load the entire file into memory,
        // you can use IOStreamFactory in AmazonWebServiceRequest to pass a
        // lambda to create a string stream.
        getObjectRequest.SetResponseStreamFactory(
            []() { return Aws::New<Aws::StringStream>("S3IOAllocationTag"); });
        // get the object
        auto getObjectOutcome = this->s3_client_->GetObject(getObjectRequest);

        if (!getObjectOutcome.IsSuccess()) {
            auto error = getObjectOutcome.GetError();
            std::cout << "ERROR: " << error.GetExceptionName() << ": "
                      << error.GetMessage() << std::endl;
            return false;
        } else {
            n = getObjectOutcome.GetResult().GetContentLength();
            // read data as a block:
            getObjectOutcome.GetResult().GetBody().read(buffer, n);
            *result = StringContainer(buffer, n);
            return true;
        }
    }

    bool readS3TransferManager(uint64_t offset, size_t n, char *buffer,
                               StringContainer *result) {
        std::cout << "ReadFilefromS3 s3:// using Transfer Manager API: ";

        auto create_stream_fn = [&]() {  // create stream lambda fn
            return Aws::New<S3UnderlyingStream>(
                "S3ReadStream",
                Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
                    "S3ReadStream", reinterpret_cast<unsigned char *>(buffer),
                    n));
        };

        std::cout << "Created stream to read with transferManager";

        // This buffer is what we used to initialize streambuf and is in memory
        std::shared_ptr<Aws::Transfer::TransferHandle> downloadHandle =
            this->transfer_manager_.get()->DownloadFile(
                this->bucket_name_.c_str(), this->object_name_.c_str(), offset,
                n, create_stream_fn);
        downloadHandle->WaitUntilFinished();
        std::cout << "File download to memory finished!" << std::endl;

        Aws::OFStream storeFile(object_name_.c_str(),
                                Aws::OFStream::out | Aws::OFStream::trunc);

        if (downloadHandle->GetStatus() !=
            Aws::Transfer::TransferStatus::COMPLETED) {
            auto error = downloadHandle->GetLastError();
            if (error.GetResponseCode() ==
                Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE) {
                n = 0;
                *result = StringContainer(buffer, n);
                std::cout << "ERROR: " << error.GetExceptionName() << ": "
                          << error.GetMessage() << std::endl;
            }
            std::cout << "ERROR: " << error.GetExceptionName() << ": "
                      << error.GetMessage() << std::endl;
        } else {
            n = downloadHandle->GetBytesTotalSize();
            *result =
                StringContainer(buffer, downloadHandle->GetBytesTransferred());
            return true;
        }
    }

   private:
    std::string bucket_name_;
    std::string object_name_;
    bool multi_part_download_;
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    std::shared_ptr<Aws::Transfer::TransferManager> transfer_manager_;
};
}  // namespace

S3Init::S3Init()
    : s3_client_(nullptr, ShutdownClient),
      transfer_manager_(nullptr, ShutdownTransferManager),
      initialization_lock_() {
    // Load reading parameters
    bufferSize = s3ReadBufferSize;
    const char *bufferSizeStr = getenv("S3_BUFFER_SIZE");
    if (bufferSizeStr) {
        bufferSize = std::stoull(bufferSizeStr);
    }
}

S3Init::~S3Init() {}

std::shared_ptr<Aws::S3::S3Client> S3Init::initializeS3Client() {
    std::lock_guard<std::mutex> lock(this->initialization_lock_);
    if (this->s3_client_.get() == nullptr) {
        Aws::SDKOptions options;
        options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;

        Aws::InitAPI(options);
        // Set up the request
        this->s3_client_ =
            std::shared_ptr<Aws::S3::S3Client>(new Aws::S3::S3Client(
                setUpS3Config(),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false));
    }
    return this->s3_client_;
}

std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor>
S3Init::initializeExecutor() {
    if (this->executor_.get() == nullptr) {
        this->executor_ =
            Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
                "executor", executorPoolSize);
    }
    return executor_;
}

std::shared_ptr<Aws::Transfer::TransferManager>
S3Init::initializeTransferManager() {
    std::shared_ptr<Aws::S3::S3Client> s3_client = initializeS3Client();
    std::lock_guard<std::mutex> lock(this->initialization_lock_);

    if (this->transfer_manager_.get() == nullptr) {
        Aws::Transfer::TransferManagerConfiguration transfer_config(
            initializeExecutor().get());
        transfer_config.s3Client = s3_client;
        // This buffer is what we used to initialize streambuf and is in memory
        transfer_config.bufferSize = s3MultiPartDownloadChunkSize;
        transfer_config.transferBufferMaxHeapSize =
            (executorPoolSize + 1) * s3MultiPartDownloadChunkSize;
        this->transfer_manager_ =
            Aws::Transfer::TransferManager::Create(transfer_config);
    }
    return transfer_manager_;
}

void S3Init::s3_read(const std::string &file_url, std::string *result,
                     bool use_tm) {
    std::string bucket, object;
    parseS3Path(file_url, &bucket, &object);

    // existence already checked in `get_file_size()`.
    // if (!this->file_exists(bucket, object)) {
    //     throw std::invalid_argument{"The specified file doesn't exist."};
    // }

    std::unique_ptr<char[]> buffer(new char[bufferSize]);
    std::stringstream ss;

    S3FS s3handler(bucket, object, use_tm, initializeTransferManager(),
                   initializeS3Client());

    uint64_t offset = 0;
    uint64_t result_size = 0;
    uint64_t file_size = this->get_file_size(bucket, object);
    std::size_t part_count = (std::max)(
        static_cast<size_t>((file_size + bufferSize - 1) / bufferSize),
        static_cast<std::size_t>(1));
    result->resize(file_size);

    for (int i = 0; i < part_count; i++) {
        offset = i * bufferSize;
        StringContainer read_chunk;
        bool flag =
            s3handler.read(offset, bufferSize, buffer.get(), &read_chunk);

        if (read_chunk.size() != 0) {
            ss.write(read_chunk.data(), read_chunk.size());
            result_size += read_chunk.size();
        }
        if (result_size == file_size) {
            break;
        }
        if (read_chunk.size() != bufferSize) {
            std::cout << "Result size and buffer size did not match";
            break;
        }
    }
    memcpy((char *)(result->data()), ss.str().data(),
           static_cast<size_t>(file_size));
}
bool S3Init::file_exists(const std::string &bucket, const std::string &object) {
    Aws::S3::Model::HeadObjectRequest headObjectRequest;
    headObjectRequest.WithBucket(bucket.c_str()).WithKey(object.c_str());
    // headObjectRequest.SetResponseStreamFactory([]() {
    //     return
    //     Aws::New<Aws::StringStream>(kS3FileSystemAllocationTag);
    // });
    auto headObjectOutcome =
        this->initializeS3Client()->HeadObject(headObjectRequest);
    if (headObjectOutcome.IsSuccess()) {
        return true;
    }
    return false;
}

uint64_t S3Init::get_file_size(const std::string &bucket,
                               const std::string &object) {
    // Assume the bucket and object both exist
    Aws::S3::Model::HeadObjectRequest headObjectRequest;
    headObjectRequest.WithBucket(bucket.c_str()).WithKey(object.c_str());
    // headObjectRequest.SetResponseStreamFactory([]() {
    //     return
    //     Aws::New<Aws::StringStream>(kS3FileSystemAllocationTag);
    // });
    auto headObjectOutcome =
        this->initializeS3Client()->HeadObject(headObjectRequest);
    if (headObjectOutcome.IsSuccess()) {
        return headObjectOutcome.GetResult().GetContentLength();
    }
    throw std::invalid_argument{"The specified file doesn't exist."};
    return 0;
}

void S3Init::list_files(const std::string &bucket, const std::string &prefix,
                        std::vector<std::string> *filenames) {
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucket.c_str())
        .WithPrefix(prefix.c_str())
        .WithMaxKeys(S3GetFilesMaxKeys)
        .WithDelimiter("/");

    Aws::S3::Model::ListObjectsResult result;
    do {
        auto outcome = this->initializeS3Client()->ListObjects(request);
        if (!outcome.IsSuccess()) {
            throw std::invalid_argument{
                "The specified bucket/folder doesn't exist."};
        }

        result = outcome.GetResult();
        for (const auto &object : result.GetContents()) {
            Aws::String key = object.GetKey();
            Aws::String entry = key.substr(prefix.length());
            if (entry.length() > 0) {
                filenames->push_back(entry.c_str());
            }
        }
        request.SetMarker(result.GetNextMarker());
    } while (result.GetIsTruncated());
}

}  // namespace awsio
