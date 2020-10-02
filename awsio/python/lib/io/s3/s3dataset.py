import tarfile
import io
import zipfile
import re
from torch.utils.data import IterableDataset, Dataset
import torch
import _pywrap_s3_io
import random
from itertools import chain

meta_prefix = "__"
meta_suffix = "__"
handler = _pywrap_s3_io.S3Init()


def reraise_exception(exn):
    """Called in an exception handler to re-raise the exception."""
    raise exn


def tardata(fileobj, skip_meta=r"__[^/]*__($|/)", handler=reraise_exception):
    """Iterator yielding filename, content pairs for the given tar stream.
    """
    try:
        stream = tarfile.open(fileobj=io.BytesIO(fileobj), mode="r|*")
        for tarinfo in stream:
            try:
                if not tarinfo.isreg():
                    continue
                fname = tarinfo.name
                if fname is None:
                    continue
                if ("/" not in fname and fname.startswith(meta_prefix)
                        and fname.endswith(meta_suffix)):
                    # skipping metadata for now
                    continue
                if skip_meta is not None and re.match(skip_meta, fname):
                    continue
                data = stream.extractfile(tarinfo).read()
                yield fname, data
            except Exception as exn:
                if handler(exn):
                    continue
                else:
                    break
        del stream
    except Exception as exn:
        handler(exn)


def zipdata(fileobj, handler=reraise_exception):
    """Iterator yielding filename, content pairs for the given zip stream.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(fileobj), 'r') as zfile:
            try:
                for file_ in zfile.namelist():
                    data = zfile.read(file_)
                    yield file_, data
            except Exception as exn:
                print("Error:", exn)
    except Exception as exn:
        print("Error:", exn)


def file_exists(url):
    """Return if file exists or not"""
    return handler.file_exists(url)


def get_file_size(url):
    """Return the file size of the specified file"""
    return handler.get_file_size(url)


def list_files(url):
    """Returns a list of entries under the same prefix.
    """
    return [url + filename for filename in handler.list_files(url)]


class S3Dataset(Dataset):
    """A mapped-style dataset for objects in s3.
    """
    def __init__(self, urls_list, batch_size=1):
        """
        Args:
            urls_list (string or list of strings): the prefix(es) and
                filenames starting with 's3://'. Each string is assumed
                as a filename first. If the file doesn't exist, the string
                is assumed as a prefix.
            batch_size (int, optional): the number of samples in a batch.
        """
        urls = [urls_list] if isinstance(urls_list, str) else urls_list
        self.handler = _pywrap_s3_io.S3Init()
        self.urls_list = list()
        for url in urls:
            if not file_exists(url):
                #self.urls_list.extend(list_files(url))
                self.urls_list.extend([urls_list + f for f in self.handler.list_files(url)])
            elif self.urls_list:
                self.urls_list.append(url)
            else:
                self.urls_list = [url]
        self.batch_size = batch_size

    @property
    def shuffled_list(self):
        return random.sample(self.urls_list, len(self.urls_list))

    def download_data(self, filename):
        if filename[-3:] == "tar":
            tarfile = tardata(self.handler.s3_read(filename))
            for fname, content in tarfile:
                yield fname, content
        elif filename[-3:] == "zip":
            zipfile = zipdata(self.handler.s3_read(filename))
            for fname, content in zipfile:
                yield fname, content
        else:
            yield self.handler.s3_read(filename)

    def get_stream(self, urls_list):
        return chain.from_iterable(map(self.download_data, urls_list))

    def get_by_batches(self):
        return zip(*[
            self.get_stream(self.shuffled_list) for _ in range(self.batch_size)
        ])

    def __len__(self):
        return len(self.urls_list)

    def __getitem__(self, idx):
        filename = self.urls_list[idx]
        fileobj = self.handler.s3_read(filename)
        return filename, fileobj

import boto3
from boto3.s3.transfer import TransferConfig

class S3BotoSet(Dataset):
    """A mapped-style dataset for objects in s3.
    """
    def __init__(self, urls_list, bucket_name, batch_size=1):
        """
        Args:
            urls_list (string or list of strings): the prefix(es) and
                filenames starting with 's3://'. Each string is assumed
                as a filename first. If the file doesn't exist, the string
                is assumed as a prefix.
            batch_size (int, optional): the number of samples in a batch.
        """
        urls = [urls_list] if isinstance(urls_list, str) else urls_list
        self.handler = _pywrap_s3_io.S3Init()
        self.urls_list = list()
        for url in urls:
            if not file_exists(url):
                #self.urls_list.extend(list_files(url))
                self.urls_list.extend([urls_list + f for f in self.handler.list_files(url)])
            elif self.urls_list:
                self.urls_list.append(url)
            else:
                self.urls_list = [url]
        self.batch_size = batch_size
        MB = 1024**2
        self.config = TransferConfig(max_concurrency=10,
                        multipart_threshold = 20 * MB)
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')

    def __len__(self):
        return len(self.urls_list)

    def __getitem__(self, idx):
        filename = self.urls_list[idx]
        filename = filename.replace('s3://' + self.bucket_name + '/', '')
        fs = io.BytesIO()
        print("Downloading..." + self.bucket_name + filename)
        s= boto3.client('s3')
        s.download_fileobj(self.bucket_name,
                                filename,
                                fs,
                                Config=self.config)
        return filename

class S3IterableDataset(IterableDataset):
    """Iterate over s3 dataset.
    It handles some bookkeeping related to DataLoader.
    """
    def __init__(self, urls_list, shuffle_urls=False):
        urls = [urls_list] if isinstance(urls_list, str) else urls_list
        self.handler = handler
        self.shuffle_urls = shuffle_urls
        self.urls_list = list()
        for url in urls:
            if not file_exists(url):
                self.urls_list.extend(self.handler.list_files(url))
            elif self.urls_list:
                self.urls_list.append(url)
            else:
                self.urls_list = [url]

    @property
    def shuffled_list(self):
        if self.shuffle_urls:
            return random.sample(self.urls_list, len(self.urls_list))
        else:
            return self.urls_list

    def download_data(self, filename):
        if filename[-3:] == "tar":
            tarfile = tardata(self.handler.s3_read(filename))
            for fname, content in tarfile:
                yield fname, content
        elif filename[-3:] == "zip":
            zipfile = zipdata(self.handler.s3_read(filename))
            for fname, content in zipfile:
                yield fname, content
        else:
            yield filename, self.handler.s3_read(filename)

    def get_stream(self, urls_list):
        return chain.from_iterable(map(self.download_data, urls_list))

    def worker_dist(self, urls):
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is not None:
            wid = worker_info.id
            num_workers = worker_info.num_workers
            return urls[wid::num_workers]
        else:
            return urls

    def __iter__(self):
        urls = self.worker_dist(self.shuffled_list)
        return self.get_stream(urls)

    def __len__(self):
        return len(self.urls_list)
