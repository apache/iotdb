import logging
import os
from typing import Dict, List, Optional

import numpy as np
import torch

from standalone_finetune.data_provider.datasets.base_dataset import (
    BasicTimeSeriesDataset,
)
from standalone_finetune.data_provider.processor.data_scaler import ScalerType

logger = logging.getLogger(__name__)


class TreeTsFileReader:
    """
    Reader for tree-model TsFile files (single field: ``field0``).

    Reads all rows via ``query_table_on_tree(["field0"])``, sorts by time,
    and stores the values as a single contiguous float32 array.
    """

    def __init__(self, file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"TsFile not found: {file_path}")

        self.file_path = file_path

        try:
            from tsfile import TsFileReader as OfficialReader

            self._reader = OfficialReader(file_path)
        except ImportError:
            raise ImportError(
                "The 'tsfile' package is required. "
                "Please install it: pip install tsfile"
            )
        except Exception as e:
            raise ValueError(f"Failed to open TsFile: {e}")

        self._data = self._load()

    def _load(self) -> np.ndarray:
        time_parts: List[np.ndarray] = []
        value_parts: List[np.ndarray] = []

        with self._reader.query_table_on_tree(["field0"]) as rs:
            while rs.next():
                df = rs.read_data_frame(max_row_num=65536)
                if df.empty:
                    break
                time_parts.append(df["time"].values)
                value_parts.append(df["field0"].values)

        if not time_parts:
            logger.warning(f"No data in TsFile: {self.file_path}")
            return np.array([], dtype=np.float32)

        times = np.concatenate(time_parts)
        values = np.concatenate(value_parts)

        order = np.argsort(times, kind="mergesort")
        return values[order].astype(np.float32)

    @property
    def length(self) -> int:
        return len(self._data)

    def close(self):
        if hasattr(self, "_reader"):
            try:
                self._reader.close()
            except Exception:
                pass

    def __del__(self):
        self.close()


class TsFileTreeDataset(BasicTimeSeriesDataset):
    """
    Dataset for loading data from tree-model TsFile files (single-variable).

    All ``field0`` values across devices and files are concatenated into one
    continuous series, then sliced with a sliding window.
    """

    def __init__(
        self,
        file_path: str,
        train_ratio: float = 0.7,
        seq_len: int = 2880,
        input_token_len: int = 16,
        output_token_lens: List[int] = None,
        window_step: int = 1,
        scale: bool = False,
        scaler_type: ScalerType = "standard",
        use_rate: float = 1.0,
        offset_rate: float = 0.0,
    ):
        if output_token_lens is None:
            output_token_lens = [96]
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

        self.file_path = file_path
        self.train_ratio = train_ratio

        self._data: Optional[np.ndarray] = None
        self.total_windows: int = 0
        self._window_offset: int = 0
        self._readers: List[TreeTsFileReader] = []

        self._init_dataset()

    def _init_dataset(self):
        if os.path.isdir(self.file_path):
            file_paths = sorted(
                os.path.join(root, f)
                for root, _, files in os.walk(self.file_path)
                for f in files
                if f.endswith(".tsfile")
            )
        else:
            file_paths = [self.file_path]

        if not file_paths:
            logger.warning(f"No TsFile files found in {self.file_path}")
            return

        parts: List[np.ndarray] = []
        for fp in file_paths:
            try:
                reader = TreeTsFileReader(fp)
                self._readers.append(reader)
                if reader.length > 0:
                    parts.append(reader._data)
            except Exception as e:
                logger.error(f"Failed to open TsFile {fp}: {e}")

        if not parts:
            logger.warning("No valid data loaded from TsFile(s)")
            return

        self._data = np.concatenate(parts) if len(parts) > 1 else parts[0]

        if self.scale:
            train_end = int(len(self._data) * self.train_ratio)
            self.fit_scaler(self._data[:train_end])
            self._data = self.transform(self._data)

        window_length = self.seq_len + self.output_token_len
        if len(self._data) < window_length:
            logger.warning(
                f"Data length ({len(self._data)}) < window length ({window_length})"
            )
            return

        full_count = (len(self._data) - window_length) // self.window_step + 1
        self.total_windows = int(full_count * self.use_rate)
        self._window_offset = int(full_count * self.offset_rate)
        
        logger.info(f"Data window amount is {self.total_windows}")

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        if index < 0 or index >= self.total_windows:
            raise IndexError(f"Index {index} out of range [0, {self.total_windows})")

        start = (index + self._window_offset) * self.window_step
        window_length = self.seq_len + self.output_token_len
        window = self._data[start : start + window_length]

        seq_x = torch.from_numpy(window[: self.seq_len].copy())
        seq_y = torch.from_numpy(window[self.input_token_len : window_length].copy())
        loss_mask = torch.ones(self.token_num, dtype=torch.float32)

        return {"seq_x": seq_x, "seq_y": seq_y, "loss_mask": loss_mask}

    def __len__(self) -> int:
        return self.total_windows

    def close(self):
        for reader in self._readers:
            reader.close()
        self._readers.clear()
