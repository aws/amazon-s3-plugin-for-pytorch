import mmcv
import numpy as np
import os
import os.path as osp
import sys
sys.path.append(os.getcwd())
sys.path.append( '.' )
sys.path.append( osp.join(os.getcwd(),'../' ) )

from .builder import DATASETS
from webdataset import Dataset as WebDataset
from .pipelines import Compose

import numpy as np
from torch.utils.data import Dataset


@DATASETS.register_module()
class ImageNetWeb(WebDataset):

    def __init__(self, urls, pipeline=None):
        super(ImageNetWeb, self).__init__(urls)
        self.map(Compose(pipeline))

    def __iter__(self):
        urls = self.shard_fn()
        return self.samples(urls)

    def evaluate(self, results, metric='accuracy', logger=None):
        """Evaluate the dataset.

        Args:
            results (list): Testing results of the dataset.
            metric (str | list[str]): Metrics to be evaluated.
                Default value is `accuracy`.
            logger (logging.Logger | None | str): Logger used for printing
                related information during evaluation. Default: None.
        Returns:
            dict: evaluation results
        """
        if not isinstance(metric, str):
            assert len(metric) == 1
            metric = metric[0]
        allowed_metrics = ['accuracy']
        if metric not in allowed_metrics:
            raise KeyError(f'metric {metric} is not supported')
        eval_results = {}
        if metric == 'accuracy':
            nums = []
            for result in results:
                nums.append(result['num_samples'].item())
                for topk, v in result['accuracy'].items():
                    if topk not in eval_results:
                        eval_results[topk] = []
                    eval_results[topk].append(v.item())
            assert sum(nums) == len(self.data_infos)
            for topk, accs in eval_results.items():
                eval_results[topk] = np.average(accs, weights=nums)
        return eval_results

    def load_annotations(self):
        pass




