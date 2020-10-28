# dataset settings
dataset_type = 'ImageNetWeb'
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
test_pipeline = [
    dict(type='LoadImageFromBytes'),
    dict(type='Resize', size=(256, -1)),
    dict(type='CenterCrop', crop_size=224),
    dict(type='Normalize', **img_norm_cfg),
    dict(type='ImageToTensor', keys=['img']),
    dict(type='ToTensor', keys=['gt_label']),
    dict(type='Collect', keys=['img', 'gt_label'])
]
urls_train= "s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{000000..001281}.tar"
urls_train = f"pipe:aws s3 cp {urls_train} - || true"
no_of_train_imgs = 1281167

urls_val = "s3://mansmane-dev/imagenet_web_dataset/val/imagenet-val-{000000..000049}.tar"
urls_val = f"pipe:aws s3 cp {urls_val} - || true"
no_of_val_imgs = 50000

data = dict(
    samples_per_gpu=64,
    workers_per_gpu=2,
    train=dict(
        type=dataset_type,
        data_prefix=urls_train,
        pipeline=train_pipeline,
        length=no_of_train_imgs//64),
    val=dict(
        type=dataset_type,
        data_prefix=urls_val,
        pipeline=test_pipeline,
        length=no_of_val_imgs//64),
    test=dict(
        # replace `data/val` with `data/test` for standard test
        type=dataset_type,
        data_prefix=urls_val,
        pipeline=test_pipeline,
        length=no_of_val_imgs//64))
evaluation = dict(interval=1, metric='accuracy')
