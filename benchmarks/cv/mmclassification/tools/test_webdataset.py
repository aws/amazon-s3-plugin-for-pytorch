import mmcv
import numpy as np
import os
import os.path as osp
import sys

sys.path.append(os.getcwd())
sys.path.append('../mmcls/datasets')
sys.path.append(osp.join(os.getcwd(), '../mmcls/'))
from mmcls.datasets.imagenet_webds import ImageNetWeb
from torch.utils.data import DataLoader

if __name__ == "__main__":
    url = "s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{000000..000554}.tar"
    url = f"pipe:aws s3 cp {url} - || true"
    pipeline = None
    img_norm_cfg = dict(
        mean=[123.675, 116.28, 103.53], std=[58.395, 57.12, 57.375], to_rgb=True)
    train_pipeline = [
        dict(type='LoadImageFromBytes'),
        dict(type='RandomResizedCrop', size=224),
        dict(type='RandomFlip', flip_prob=0.5, direction='horizontal'),
        dict(type='Normalize', **img_norm_cfg),
        dict(type='ImageToTensor', keys=['img']),
        dict(type='ToTensor', keys=['gt_label']),
        dict(type='Collect', keys=['img', 'gt_label'])
    ]

    dataset = ImageNetWeb(url, train_pipeline)

    # Test dataset
    # for i, sample in enumerate(dataset):
    #     for key, value in sample.items():
    #         print(key, repr(value)[:50])
    #     if i == 5:
    #         break

    batch_size = 16
    num_workers=1
    shuffle=False
    data_loader = DataLoader(
        dataset,
        batch_size=batch_size,
        pin_memory=False,
        shuffle=shuffle)
    for epoch in range(1):
        for j, batch in enumerate(data_loader):
            for key, item in batch.items():
                print(key, item.shape)

            if j == 5:
                break
