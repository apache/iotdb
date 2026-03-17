import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch

from standalone_finetune.data_provider.datasets.base_dataset import (
    BasicTimeSeriesDataset,
)
from standalone_finetune.data_provider.processor.data_scaler import ScalerType


class CSVDataset(BasicTimeSeriesDataset):
    """
    Dataset for loading time series data from CSV files.

    Args:
        variables: Ordered list of column names to select from the CSV.
            Convention: the **last** entry is the prediction target; all
            preceding entries are covariates.  If None, all columns are used.
    """

    def __init__(
        self,
        data_path: str,
        seq_len: int,
        input_token_len: int,
        output_token_lens: List[int],
        variables: Optional[List[str]] = None,
        train_ratio: float = 0.7,
        window_step: int = 1,
        scale: bool = False,
        scaler_type: ScalerType = "standard",
        use_rate: float = 1.0,
        offset_rate: float = 0.0,
        multivariate: bool = False,
    ):
        super().__init__(
            seq_len,
            input_token_len,
            output_token_lens,
            window_step,
            scale,
            scaler_type,
            use_rate,
            offset_rate,
        )

        self.data_path = data_path
        self.variables = variables
        self.multivariate = multivariate
        self.train_ratio = train_ratio

        self._load_data()

    def _load_data(self):
        if os.path.isdir(self.data_path):
            csv_files = sorted(
                [
                    os.path.join(self.data_path, f)
                    for f in os.listdir(self.data_path)
                    if f.endswith(".csv")
                ]
            )
            if not csv_files:
                raise ValueError(f"No CSV files found in {self.data_path}")
            df_list = [pd.read_csv(f) for f in csv_files]
            df_raw = pd.concat(df_list, ignore_index=True)
        else:
            df_raw = pd.read_csv(self.data_path)

        if self.variables is not None:
            df_raw = df_raw[self.variables]

        if isinstance(df_raw[df_raw.columns[0]][2], str):
            data = df_raw[df_raw.columns[1:]].values
        else:
            data = df_raw.values

        data = data.astype(np.float32)
        data_len = len(data)
        window_length = self.seq_len + self.output_token_len

        full_window_count = (data_len - window_length) // self.window_step + 1
        use_window_count = int(full_window_count * self.use_rate)
        self._window_start_offset = int(full_window_count * self.offset_rate)
        self.total_windows = use_window_count

        if self.scale:
            train_end = int(data_len * self.train_ratio)
            train_data = data[0:train_end]
            self.fit_scaler(train_data)
            data = self.transform(data)

        self.data = data
        self.n_var = self.data.shape[-1] if len(self.data.shape) > 1 else 1

        if not self.multivariate and self.n_var > 1:
            self._window_count = use_window_count
            self.total_windows = use_window_count * self.n_var
        else:
            self._window_count = use_window_count

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        if index < 0 or index >= self.total_windows:
            raise IndexError(f"Index {index} out of range [0, {self.total_windows})")

        if self.multivariate or self.n_var <= 1:
            actual_window_idx = index + self._window_start_offset
            s_begin = actual_window_idx * self.window_step
            s_end = s_begin + self.seq_len
            r_begin = s_begin + self.input_token_len
            r_end = s_end + self.output_token_len

            if len(self.data.shape) == 1:
                seq_x = self.data[s_begin:s_end]
                seq_y = self.data[r_begin:r_end]
            else:
                seq_x = self.data[s_begin:s_end, :]
                seq_y = self.data[r_begin:r_end, :]
        else:
            window_idx = index // self.n_var
            feat_id = index % self.n_var
            actual_window_idx = window_idx + self._window_start_offset
            s_begin = actual_window_idx * self.window_step
            s_end = s_begin + self.seq_len
            r_begin = s_begin + self.input_token_len
            r_end = s_end + self.output_token_len

            seq_x = self.data[s_begin:s_end, feat_id]
            seq_y = self.data[r_begin:r_end, feat_id]

        loss_mask = np.ones(self.token_num, dtype=np.int32)

        return {
            "seq_x": torch.tensor(seq_x, dtype=torch.float32),
            "seq_y": torch.tensor(seq_y, dtype=torch.float32),
            "loss_mask": torch.from_numpy(loss_mask).float(),
        }

    def __len__(self) -> int:
        return self.total_windows


class ParquetDataset(CSVDataset):
    """Dataset for loading time series data from Parquet files."""

    def _load_data(self):
        if os.path.isdir(self.data_path):
            parquet_files = sorted(
                [
                    os.path.join(self.data_path, f)
                    for f in os.listdir(self.data_path)
                    if f.endswith(".parquet")
                ]
            )
            if not parquet_files:
                raise ValueError(f"No Parquet files found in {self.data_path}")
            df_list = [pd.read_parquet(f) for f in parquet_files]
            df_raw = pd.concat(df_list, ignore_index=True)
        else:
            df_raw = pd.read_parquet(self.data_path)

        if self.variables is not None:
            df_raw = df_raw[self.variables]

        if isinstance(df_raw[df_raw.columns[0]][2], str):
            data = df_raw[df_raw.columns[1:]].values
        else:
            data = df_raw.values

        data = data.astype(np.float32)
        data_len = len(data)
        window_length = self.seq_len + self.output_token_len

        full_window_count = (data_len - window_length) // self.window_step + 1
        use_window_count = int(full_window_count * self.use_rate)
        self._window_start_offset = int(full_window_count * self.offset_rate)
        self.total_windows = use_window_count

        if self.scale:
            train_end = int(data_len * self.train_ratio)
            train_data = data[0:train_end]
            self.fit_scaler(train_data)
            data = self.transform(data)

        self.data = data
        self.n_var = self.data.shape[-1] if len(self.data.shape) > 1 else 1

        if not self.multivariate and self.n_var > 1:
            self._window_count = use_window_count
            self.total_windows = use_window_count * self.n_var
        else:
            self._window_count = use_window_count
