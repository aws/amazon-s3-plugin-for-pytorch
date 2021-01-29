
from awsio.python.lib.io.s3.s3dataset import S3Dataset
from torch.utils.data import DataLoader

url_list = ['s3://image-data-bucket/train/n01440764/n01440764_10026.JPEG',
 's3://image-data-bucket/train/n01440764/n01440764_10027.JPEG',
 's3://image-data-bucket/train/n01440764/n01440764_10029.JPEG']

dataset = S3Dataset(url_list)
dataloader = DataLoader(dataset,
        batch_size=2,
        num_workers=64)

for i, (image, label) in enumerate(dataloader):
    print(type(image), len(image))

