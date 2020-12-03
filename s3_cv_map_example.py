
from awsio.python.lib.io.s3.s3dataset import S3Dataset


url_list = ['s3://mansmane-dev/imagenet/train/n01440764/n01440764_10026.JPEG',
 's3://mansmane-dev/imagenet/train/n01440764/n01440764_10027.JPEG',
 's3://mansmane-dev/imagenet/train/n01440764/n01440764_10029.JPEG']


dataset = S3Dataset(url_list)

for i in range(len(dataset)):
    print(dataset[i])
