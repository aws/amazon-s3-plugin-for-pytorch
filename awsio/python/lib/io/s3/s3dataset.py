import os
import torch
import tarfile
import io
import zipfile
from zipfile import ZipFile
import re
from torch.utils.data import IterableDataset
import _pywrap_s3_io

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

            
class S3Dataset(IterableDataset):
    """Iterate over s3 dataset.
    It handles some bookkeeping related to DataLoader.
    """
    def __init__(self, url, compression=None, transforms=None):
        self.handler = _pywrap_s3_io.S3Init()
        data = self.handler.s3_read(url, False)
        if compression=="tar":
            self.data = tardata(data)
        elif compression=="zip":
            self.data = zipdata(data)
        else:
            self.data=data

    def __iter__(self):
       # data = self.shard_fn(self.data)
        return iter(self.data)




