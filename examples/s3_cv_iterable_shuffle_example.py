from torch.utils.data import IterableDataset, DataLoader
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset
from PIL import Image
import io
from torchvision import transforms


class ImageNetS3(IterableDataset):
    def __init__(self, url_list, shuffle_urls=False, transform=None):
        self.s3_iter_dataset = S3IterableDataset(url_list,
                                                 shuffle_urls)
        self.transform = transform


    def data_generator(self):
        try:
            while True:
                # Based on aplhabetical order of files sequence of label and image will change.
                # e.g. for files 0186304.cls 0186304.jpg, 0186304.cls will be fetched first
                label_fname, label_fobj = next(self.s3_iter_dataset_iterator)
                image_fname, image_fobj = next(self.s3_iter_dataset_iterator)
                label = int(label_fobj)
                image_np = Image.open(io.BytesIO(image_fobj)).convert('RGB')

                # Apply torch visioin transforms if provided
                if self.transform is not None:
                    image_np = self.transform(image_np)
                yield image_np, label

        except StopIteration:
            raise StopIteration

    def set_epoch(self, epoch):
        self.s3_iter_dataset.set_epoch(epoch)

    def __iter__(self):
        self.s3_iter_dataset_iterator = iter(self.s3_iter_dataset)
        return self.data_generator()


url_list = ["s3://pt-s3plugin-test-data-west2/integration_tests/imagenet-train-000000.tar"]
# Torchvision transforms to apply on data

preproc = transforms.Compose([
    transforms.ToTensor(),
    transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
    transforms.Resize((100, 100))
])

dataset = ImageNetS3(url_list, transform=preproc, shuffle_urls=True)

dataloader = DataLoader(dataset, num_workers=4, batch_size=32)

for e in range(5):
    dataset.set_epoch(e)
