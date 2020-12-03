from torch.utils.data import IterableDataset
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset
from itertools import islice
from PIL import Image
import io
import numpy as np
from torchvision import transforms

class ImageNetS3(IterableDataset):
    def __init__(self, url_list, shuffle_urls=False, transform=None):
        self.url_list = url_list
        self.s3_iter_dataset = S3IterableDataset(self.url_list,
                                                 shuffle_urls)
        self.transform = transform

    def bytes_to_npimage(self, image_fobj):
        # Convert bytes image object to numpy array
        image = Image.open(io.BytesIO(image_fobj)).convert('RGB')
        # Add Image proprocessing steps here
        image = image.resize((256, 256))
        return np.array(image)

    def my_generator(self):
        try:
            while True:
                # Based on aplhabetical order of files sequence of label and image will change.
                # e.g. for files 0186304.cls 0186304.jpg, 0186304.cls will be fetched first
                label_fname, label_fobj = next(self.s3_iter_dataset_iterator)
                image_fname, image_fobj = next(self.s3_iter_dataset_iterator)
                label = int(label_fobj)
                image_np = self.bytes_to_npimage(image_fobj)
                if self.transform is not None:
                    image_np = self.transform(image_np)
                yield image_np, label

        except StopIteration:
            raise StopIteration

    def __iter__(self):
        self.s3_iter_dataset_iterator = iter(self.s3_iter_dataset)
        return self.my_generator()


url_list = ["s3://ydaiming-test-data2/integration_tests/imagenet-train-000000.tar"]
normalize = transforms.Normalize(
    mean=[0.485, 0.456, 0.406],
    std=[0.229, 0.224, 0.225])

preproc = transforms.Compose([
    transforms.ToTensor(),
    normalize,
])

dataset = ImageNetS3(url_list, transform=preproc)

for image, label in islice(dataset, 0, 3):
    print(image.shape, label)