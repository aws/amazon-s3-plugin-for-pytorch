#   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
  
#       http://www.apache.org/licenses/LICENSE-2.0
  
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import tarfile
import io
import zipfile
import re
from torch.utils.data import IterableDataset, Dataset
import torch
import torch.distributed as dist
import random
from itertools import chain

meta_prefix = "__"
meta_suffix = "__"

def reraise_exception(exn): # pragma: no cover
    """Called in an exception handler to re-raise the exception."""
    raise exn


def tardata(fileobj, skip_meta=r"__[^/]*__($|/)", handler=reraise_exception):
    """Iterator yielding filename, content pairs for the given tar stream.
    """
    # eliminated from test coverage since checking requires invalid tarfile
    try:
        stream = tarfile.open(fileobj=io.BytesIO(fileobj), mode="r|*")
        for tarinfo in stream:
            try:
                if not tarinfo.isreg(): # pragma: no cover
                    continue
                fname = tarinfo.name
                if fname is None: # pragma: no cover
                    continue
                if ("/" not in fname and fname.startswith(meta_prefix)
                        and fname.endswith(meta_suffix)): # pragma: no cover
                    # skipping metadata for now
                    continue
                if skip_meta is not None and re.match(skip_meta, fname): # pragma: no cover
                    continue
                data = stream.extractfile(tarinfo).read()
                yield fname, data
            except Exception as exn: # pragma: no cover
                if handler(exn):
                    continue
                else:
                    break
        del stream
    except Exception as exn: # pragma: no cover
        handler(exn)


def zipdata(fileobj, handler=reraise_exception):
    """Iterator yielding filename, content pairs for the given zip stream.
    """
    # eliminated from test coverage since checking requires invalid zipfile
    try:
        with zipfile.ZipFile(io.BytesIO(fileobj), 'r') as zfile:
            try:
                for file_ in zfile.namelist():
                    data = zfile.read(file_)
                    yield file_, data
            except Exception as exn: # pragma: no cover
                print("Error:", exn)
    except Exception as exn: # pragma: no cover
        print("Error:", exn)



class FSXIterableDataset(IterableDataset):
    def __init__(self, urls_list, shuffle_urls=False):
        self.epoch = 0
        self.shuffle_urls = shuffle_urls
        self.urls_list = urls_list

    @property
    def shuffled_list(self):
        if self.shuffle_urls:
            random.seed(self.epoch)
            return random.sample(self.urls_list, len(self.urls_list))
        else:
            return self.urls_list

    def download_data(self, filename):
        if filename[-3:] == "tar":
            tarfile = tardata(open(filename, 'rb').read())
            for fname, content in tarfile:
                yield fname, content
        elif filename[-3:] == "zip":
            zipfile = zipdata(open(filename, 'rb').read())
            for fname, content in zipfile:
                yield fname, content
        else:
            yield filename, open(filename, 'rb').read()

    def get_stream(self, urls_list):
        return chain.from_iterable(map(self.download_data, urls_list))

    def worker_dist(self, urls):
        if dist.is_initialized():
            world_size = dist.get_world_size()
            rank = dist.get_rank()
            total_size = len(urls)
            urls = urls[rank:total_size:world_size]

        worker_info = torch.utils.data.get_worker_info()
        if worker_info is not None:
            wid = worker_info.id
            num_workers = worker_info.num_workers
            length = len(urls)
            return urls[wid:length:num_workers]
        else:
            return urls

    def __iter__(self):
        urls = self.worker_dist(self.shuffled_list)
        return self.get_stream(urls)

    def __len__(self):
        return len(self.urls_list)

    def set_epoch(self, epoch):
        self.epoch = epoch
