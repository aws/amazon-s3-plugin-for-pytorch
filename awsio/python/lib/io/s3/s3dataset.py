import tarfile
import io
import zipfile
import re
from torch.utils.data import IterableDataset
import _pywrap_s3_io
import random
from itertools import chain

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
    handler = _pywrap_s3_io.S3Init()
    return handler.file_exists(url)


def get_file_size(url):
    """Return the file size of the specified file"""
    handler = _pywrap_s3_io.S3Init()
    return handler.get_file_size(url)


def list_files(url):
    """Returns a list of entries under the same prefix.
    """
    handler = _pywrap_s3_io.S3Init()
    return [url + filename for filename in handler.list_files(url)]


class S3Dataset(IterableDataset):
    """Iterate over s3 dataset.
    It handles some bookkeeping related to DataLoader.
    """
    def __init__(self, urls_list, batch_size=1):
        urls = [urls_list] if isinstance(urls_list, str) else urls_list
        self.handler = _pywrap_s3_io.S3Init()
        self.urls_list = list()
        for url in urls:
            if not file_exists(url):
                self.urls_list.extend(self.handler.list_files(url))
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

    def __iter__(self):
        return self.get_by_batches()

    def __len__(self):
        return len(self.urls_list)
