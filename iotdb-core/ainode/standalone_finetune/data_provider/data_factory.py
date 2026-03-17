import logging
from typing import Any, Dict, Optional, Tuple

import torch
from torch.utils.data import DataLoader, DistributedSampler

from standalone_finetune.data_provider.datasets.base_dataset import BasicTimeSeriesDataset
from standalone_finetune.data_provider.datasets.csv_dataset import CSVDataset, ParquetDataset
from standalone_finetune.data_provider.datasets.tsfile_tree_dataset import TsFileTreeDataset
from standalone_finetune.data_provider.datasets.iotdb_tree_dataset import IoTDBTreeDataset
from standalone_finetune.hparams.data_args import DataArguments
from standalone_finetune.hparams.model_args import ModelArguments

logger = logging.getLogger(__name__)


def create_dataset(
    model_args: ModelArguments,
    data_args: DataArguments,
    flag: str = "train",
) -> BasicTimeSeriesDataset:
    """
    Create a dataset based on data arguments.

    Args:
        model_args: Model-related arguments
        data_args: Data-related arguments
        flag: Dataset split type ("train", "val", "test")

    Returns:
        Dataset instance
    """
    if flag == "train":
        use_rate = data_args.train_ratio
        offset_rate = 0.0
    elif flag == "val":
        use_rate = data_args.val_ratio
        offset_rate = data_args.train_ratio
    else:
        use_rate = data_args.test_ratio
        offset_rate = data_args.train_ratio + data_args.val_ratio

    common_kwargs = dict(
        seq_len=model_args.seq_len,
        input_token_len=model_args.input_token_len,
        output_token_lens=model_args.output_token_lens,
        window_step=data_args.window_step,
        scale=data_args.scale,
        scaler_type=data_args.scaler_type,
        use_rate=use_rate,
        offset_rate=offset_rate,
    )

    dataset_type = data_args.dataset_type

    if dataset_type == "iotdb.tree":
        dataset = IoTDBTreeDataset(
            sql=data_args.sql,
            iotdb_ip=data_args.iotdb_ip,
            iotdb_port=data_args.iotdb_port,
            iotdb_username=data_args.iotdb_username,
            iotdb_password=data_args.iotdb_password,
            train_ratio=data_args.train_ratio,
            **common_kwargs,
        )

    elif dataset_type == "tsfile":
        dataset = TsFileTreeDataset(
            file_path=data_args.data_path,
            train_ratio=data_args.train_ratio,
            **common_kwargs,
        )

    elif dataset_type == "csv":
        dataset = CSVDataset(
            data_path=data_args.data_path,
            variables=data_args.variable_names,
            train_ratio=data_args.train_ratio,
            multivariate=data_args.multivariate,
            **common_kwargs,
        )

    elif dataset_type == "parquet":
        dataset = ParquetDataset(
            data_path=data_args.data_path,
            variables=data_args.variable_names,
            train_ratio=data_args.train_ratio,
            multivariate=data_args.multivariate,
            **common_kwargs,
        )

    else:
        raise ValueError(f"Unsupported dataset_type: {dataset_type}")

    logger.info(f"Created {dataset_type} dataset for {flag}: {len(dataset)} samples")
    return dataset


def create_dataloader(
    dataset: BasicTimeSeriesDataset,
    batch_size: int,
    shuffle: bool = False,
    num_workers: int = 0,
    pin_memory: bool = True,
    drop_last: bool = True,
    distributed: bool = False,
    collate_fn: Optional[Any] = None,
) -> DataLoader:
    """
    Create a dataloader for the given dataset.

    Args:
        dataset: Dataset instance
        batch_size: Batch size
        shuffle: Whether to shuffle data
        num_workers: Number of worker processes
        pin_memory: Whether to pin memory for GPU
        drop_last: Whether to drop the last incomplete batch
        distributed: Whether to use distributed sampler
        collate_fn: Optional collate function

    Returns:
        DataLoader instance
    """
    sampler = None
    if distributed:
        sampler = DistributedSampler(dataset, shuffle=shuffle)
        shuffle = False

    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle if sampler is None else False,
        num_workers=num_workers,
        pin_memory=pin_memory,
        drop_last=drop_last,
        sampler=sampler,
        collate_fn=collate_fn,
    )

    return dataloader


def create_train_val_dataloaders(
    model_args: ModelArguments,
    data_args: DataArguments,
    training_args: Any,
    distributed: bool = False,
) -> Tuple[DataLoader, DataLoader]:
    """
    Create train and validation dataloaders.

    Args:
        model_args: Model arguments
        data_args: Data arguments
        training_args: Training arguments
        distributed: Whether to use distributed training

    Returns:
        Tuple of (train_dataloader, val_dataloader)
    """
    train_dataset = create_dataset(model_args, data_args, flag="train")
    val_dataset = create_dataset(model_args, data_args, flag="val")

    train_loader = create_dataloader(
        train_dataset,
        batch_size=training_args.train_batch_size,
        shuffle=True,
        num_workers=data_args.num_workers,
        pin_memory=data_args.pin_memory,
        drop_last=True,
        distributed=distributed,
    )

    val_loader = create_dataloader(
        val_dataset,
        batch_size=training_args.val_batch_size,
        shuffle=False,
        num_workers=data_args.num_workers,
        pin_memory=data_args.pin_memory,
        drop_last=False,
        distributed=distributed,
    )

    return train_loader, val_loader
