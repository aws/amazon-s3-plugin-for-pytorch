#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Bucket.h>
#include <iostream>

int main(int argc, char** argv)
{
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::S3::S3Client s3_client;
        auto outcome = s3_client.ListBuckets();

        if (outcome.IsSuccess()) {
            std::cout << "Your Amazon S3 buckets:" << std::endl;

            Aws::Vector<Aws::S3::Model::Bucket> bucket_list =
            outcome.GetResult().GetBuckets();

            for (auto const &bucket: bucket_list) {
                std::cout << "  * " << bucket.GetName() << std::endl;
            }
        } else {
            std::cout << "ListBuckets error: "
            << outcome.GetError().GetExceptionName() << " - "
            << outcome.GetError().GetMessage() << std::endl;
        }
    }
    Aws::ShutdownAPI(options);
}

