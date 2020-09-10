import os
import torch
import tarfile
import io
import zipfile
from zipfile import ZipFile
import re
from torch.utils.data import IterableDataset
import _pywrap_s3_io
import random
from itertools import chain, cycle

meta_prefix = "__"
meta_suffix = "__"

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
                if (
                        "/" not in fname
                        and fname.startswith(meta_prefix)
                        and fname.endswith(meta_suffix)
                ):
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
    print(fileobj)
    try:
        with zipfile.ZipFile(io.BytesIO(fileobj), 'r') as zfile:
            print(zfile.namelist())
            try:
                for file_ in zfile.namelist():
                    data = zfile.read(file_)
                    yield file_, data
            except Exception as exn:
                print("Error:", exn)
    except Exception as exn:
        print("Error:", exn)


def list_files(bucket, prefix):
    """Returns a list of entries contained within a directory.
    """
    handler = _pywrap_s3_io.S3Init()
    return [
        's3://'+bucket+'/'+prefix+filename
        for filename in handler.list_files(bucket, prefix)
    ]


class S3Dataset(IterableDataset):
    """Iterate over s3 dataset.
    It handles some bookkeeping related to DataLoader.
    """
    def __init__(self, urls_list, batch_size=1, compression=None):
        self.urls_list = [urls_list] if isinstance(urls_list, str) else urls_list
        self.batch_size = batch_size
        self.handler = _pywrap_s3_io.S3Init()
        self.compression = compression
        if compression=="tar":
            print(self.urls_list[0])
            data = self.handler.s3_read(self.urls_list[0], False)
            self.data = tardata(data)
        elif compression=="zip":
            print(urls_list[0])
            data = self.handler.s3_read(urls_list[0], False)
            self.data = zipdata(data)
    
    @property
    def shuffled_list(self):
        return random.sample(self.urls_list, len(self.urls_list))

    def download_data(self, filename):
     #   if filename[-3:] =="tar":
      #      data = self.handler.s3_read(filename, True)
            
       #     tar_files = tardata(data)
        #    return tar_files
    #    elif filename[-3:] =="zip":
     #       yield zipdata(self.handler.s3_read(filename, True))
      #  else:
       #     print(filename)
            yield self.handler.s3_read(filename, True)

    def get_stream(self, urls_list):
        return chain.from_iterable(map(self.download_data, urls_list))

    def get_by_batches(self):
        return zip(*[self.get_stream(self.shuffled_list)
                     for _ in range(self.batch_size)])

    def __iter__(self):
        if self.compression == "tar" or self.compression == "zip":
            print("in iterrrrr")
            return iter(self.data)
        else:
            return self.get_by_batches()

