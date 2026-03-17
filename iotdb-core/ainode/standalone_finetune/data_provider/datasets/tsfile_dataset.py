import bisect
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


class TsFileReader:
    """
    TsFile Reader wrapper around the official tsfile package.
    """

    CATEGORY_TAG = 0
    CATEGORY_FIELD = 1

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

        self.series_paths: List[str] = []
        self.series_info: Dict[str, Dict] = {}
        self._timestamps_cache: Dict[str, np.ndarray] = {}
        self._series_data_cache: Dict[str, np.ndarray] = {}

        self._cache_metadata()

    def __del__(self):
        self.close()

    def close(self):
        if hasattr(self, "_reader"):
            try:
                self._reader.close()
            except Exception:
                pass

    def _cache_metadata(self):
        """
        Scan TsFile to build series_paths, series_info, and timestamps cache.
        Uses the official tsfile table model API.
        """
        table_schemas = self._reader.get_all_table_schemas()

        if not table_schemas:
            raise ValueError("No tables found in TsFile")

        for table_name in table_schemas.keys():
            table_schema = self._reader.get_table_schema(table_name)

            field_columns = []
            column_schemas = table_schema.get_columns()
            for col_schema in column_schemas:
                col_name = col_schema.get_column_name()
                col_category = col_schema.get_category()
                if col_name.lower() != "time" and col_category == self.CATEGORY_FIELD:
                    field_columns.append(col_name)

            if not field_columns:
                continue

            with self._reader.query_table(table_name, [field_columns[0]]) as result_set:
                timestamps_list = []
                while result_set.next():
                    df = result_set.read_data_frame(max_row_num=65536)
                    if df.empty:
                        break
                    time_col = "Time" if "Time" in df.columns else "time"
                    timestamps_list.append(df[time_col].values)
                timestamps = (
                    np.concatenate(timestamps_list) if timestamps_list else np.array([])
                )

            if len(timestamps) == 0:
                continue

            for col in field_columns:
                series_path = f"{table_name}.{col}"
                self.series_paths.append(series_path)
                self._timestamps_cache[series_path] = np.array(
                    timestamps, dtype=np.int64
                )
                self.series_info[series_path] = {
                    "length": len(timestamps),
                    "min_time": int(timestamps[0]),
                    "max_time": int(timestamps[-1]),
                    "table_name": table_name,
                    "column_name": col,
                }

        if not self.series_paths:
            raise ValueError("No valid numeric series found in TsFile")

    def get_all_series(self) -> List[str]:
        return self.series_paths.copy()

    def get_series_length(self, series_path: str) -> int:
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")
        return self.series_info[series_path]["length"]

    def read_series(self, series_path: str) -> List[float]:
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")
        if series_path in self._series_data_cache:
            return self._series_data_cache[series_path].tolist()
        length = self.series_info[series_path]["length"]
        return self.read_series_range(series_path, 0, length)

    def read_series_range(self, series_path: str, start: int, end: int) -> List[float]:
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")

        if series_path in self._series_data_cache:
            return self._series_data_cache[series_path][start:end].tolist()

        info = self.series_info[series_path]
        timestamps = self._timestamps_cache[series_path]

        start_time = int(timestamps[start])
        end_time = int(timestamps[end - 1])

        with self._reader.query_table(
            info["table_name"],
            [info["column_name"]],
            start_time=start_time,
            end_time=end_time,
        ) as result_set:
            data_list = []
            while result_set.next():
                df = result_set.read_data_frame(max_row_num=65536)
                if df.empty:
                    break
                data_list.append(df[info["column_name"]].astype(float).values)

            if data_list:
                return np.concatenate(data_list).tolist()
            return []

    def cache_series_data(self, series_path: str):
        """Preload entire series into memory for faster subsequent access."""
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")
        if series_path not in self._series_data_cache:
            data = self.read_series(series_path)
            self._series_data_cache[series_path] = np.array(data, dtype=np.float32)

    def is_series_cached(self, series_path: str) -> bool:
        return series_path in self._series_data_cache


class _MergedSeries:
    """Virtual concatenation of one series across multiple TsFile readers."""

    __slots__ = ("_segments", "_offsets", "total_length")

    def __init__(self):
        self._segments: List[tuple] = []  # [(reader, series_path, length), ...]
        self._offsets: List[int] = [0]  # cumulative offsets, len = segments + 1
        self.total_length: int = 0

    def add(self, reader: TsFileReader, series_path: str) -> None:
        length = reader.get_series_length(series_path)
        self._segments.append((reader, series_path, length))
        self.total_length += length
        self._offsets.append(self.total_length)

    def read_range(self, start: int, end: int) -> List[float]:
        """Read [start, end) from the merged view, spanning segments as needed."""
        idx = bisect.bisect_right(self._offsets, start) - 1
        result: List[float] = []
        while idx < len(self._segments) and start < end:
            reader, series_path, length = self._segments[idx]
            seg_offset = self._offsets[idx]
            local_start = start - seg_offset
            local_end = min(end - seg_offset, length)
            result.extend(reader.read_series_range(series_path, local_start, local_end))
            start = seg_offset + local_end
            idx += 1
        return result

    def read_all(self) -> List[float]:
        return self.read_range(0, self.total_length)

    def cache_all(self) -> None:
        for reader, series_path, _ in self._segments:
            reader.cache_series_data(series_path)


class TsFileDataset(BasicTimeSeriesDataset):
    """
    Dataset for loading data from TsFile files.

    Supports two modes:
    - Single-variable: Each series is treated independently
    - Multi-variable: Multiple series are combined into a feature matrix
    """

    def __init__(
        self,
        file_path: str,
        variables: Optional[List[str]] = None,
        preload: bool = True,
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
        self.variables = variables
        self.preload = preload
        self.train_ratio = train_ratio

        self._is_multivariate = variables is not None and len(variables) > 1

        self._readers: Dict[str, TsFileReader] = {}
        self.series_index: List[Dict] = []
        self._variable_info: List[Dict] = []
        self.total_windows: int = 0
        self.prefix_sum: List[int] = [0]

        self._init_dataset()

    def _init_dataset(self):
        if os.path.isdir(self.file_path):
            file_paths = sorted(
                [
                    os.path.join(root, f)
                    for root, _, files in os.walk(self.file_path)
                    for f in files
                    if f.endswith(".tsfile")
                ]
            )
        else:
            file_paths = [self.file_path]

        if not file_paths:
            logger.warning(f"No TsFile files found in {self.file_path}")
            return

        for fp in file_paths:
            try:
                self._readers[fp] = TsFileReader(fp)
            except Exception as e:
                logger.error(f"Failed to open TsFile {fp}: {e}")

        if not self._readers:
            logger.warning("No valid TsFile readers created")
            return

        if self._is_multivariate:
            self._build_index_multi_variable()
        else:
            self._build_index_single_variable()

        if self.scale:
            self._compute_scale_params()

        if self.preload:
            self._preload_data()

    def _build_merged_series(self) -> Dict[str, _MergedSeries]:
        """Group the same series across all readers into merged views."""
        merged: Dict[str, _MergedSeries] = {}
        for reader in self._readers.values():
            all_series = reader.get_all_series()
            if self.variables:
                all_series = [s for s in all_series if s in self.variables]
            for series_path in all_series:
                if series_path not in merged:
                    merged[series_path] = _MergedSeries()
                merged[series_path].add(reader, series_path)
        return merged

    def _build_index_single_variable(self):
        """Build index for single-variable mode: each series independently."""
        window_length = self.seq_len + self.output_token_len
        merged = self._build_merged_series()
        total_windows = 0

        for series_path, ms in merged.items():
            data_length = ms.total_length
            if data_length < window_length:
                continue

            full_count = (data_length - window_length) // self.window_step + 1
            use_count = int(full_count * self.use_rate)
            offset = int(full_count * self.offset_rate)

            if use_count > 0:
                self.series_index.append(
                    {
                        "series_path": series_path,
                        "merged_series": ms,
                        "data_length": data_length,
                        "window_count": use_count,
                        "window_offset": offset,
                        "window_start": total_windows,
                        "window_end": total_windows + use_count,
                    }
                )
                total_windows += use_count

        self.total_windows = total_windows

    def _build_index_multi_variable(self):
        """Build index for multi-variable mode: aligned windows across variables."""
        merged = self._build_merged_series()

        for var_path in self.variables:
            if var_path not in merged:
                raise ValueError(f"Variable not found in any TsFile: {var_path}")
            ms = merged[var_path]
            self._variable_info.append(
                {
                    "series_path": var_path,
                    "merged_series": ms,
                    "data_length": ms.total_length,
                }
            )

        window_length = self.seq_len + self.output_token_len
        min_length = min(info["data_length"] for info in self._variable_info)

        if min_length < window_length:
            logger.warning(
                f"Data length ({min_length}) < window length ({window_length})"
            )
            self.total_windows = 0
            return

        full_count = (min_length - window_length) // self.window_step + 1
        self.total_windows = int(full_count * self.use_rate)
        self._window_offset = int(full_count * self.offset_rate)

    def _compute_scale_params(self):
        """Compute scale parameters from training portion of all series."""
        items = self._variable_info if self._is_multivariate else self.series_index
        all_train_data = []

        for info in items:
            data = info["merged_series"].read_all()
            if len(data) > 0:
                train_end = int(len(data) * self.train_ratio)
                all_train_data.append(np.array(data[:train_end], dtype=np.float32))

        if all_train_data:
            self.fit_scaler(np.concatenate(all_train_data))

    def _preload_data(self):
        """Preload all series data into reader cache."""
        items = self._variable_info if self._is_multivariate else self.series_index
        for info in items:
            info["merged_series"].cache_all()

    def _locate_series(self, index: int):
        """
        Locate series and local window index using binary search.
        """
        left, right = 0, len(self.series_index) - 1
        while left <= right:
            mid = (left + right) // 2
            info = self.series_index[mid]
            if index < info["window_start"]:
                right = mid - 1
            elif index >= info["window_end"]:
                left = mid + 1
            else:
                local_window_idx = index - info["window_start"]
                return info, local_window_idx
        raise IndexError(f"Index {index} out of range")

    def _getitem_single_variable(self, index: int) -> Dict[str, torch.Tensor]:
        """Get item for single-variable mode."""
        series_info, local_window_idx = self._locate_series(index)
        ms = series_info["merged_series"]
        offset = series_info["window_offset"]

        data_start = (local_window_idx + offset) * self.window_step
        window_length = self.seq_len + self.output_token_len
        data_end = data_start + window_length

        data_window = ms.read_range(data_start, data_end)
        data_window = np.array(data_window, dtype=np.float32)

        if self.scale:
            data_window = self.transform(data_window)

        seq_x = data_window[: self.seq_len]
        seq_y = data_window[self.input_token_len : window_length]
        loss_mask = np.ones(self.token_num, dtype=np.int32)

        return {
            "seq_x": torch.from_numpy(seq_x),
            "seq_y": torch.from_numpy(seq_y),
            "loss_mask": torch.from_numpy(loss_mask).float(),
        }

    def _getitem_multi_variable(self, index: int) -> Dict[str, torch.Tensor]:
        """Get item for multi-variable mode."""
        offset = getattr(self, "_window_offset", 0)
        data_start = (index + offset) * self.window_step
        window_length = self.seq_len + self.output_token_len
        data_end = data_start + window_length

        x_list = []
        y_list = []

        for var_info in self._variable_info:
            data_window = var_info["merged_series"].read_range(data_start, data_end)
            data_window = np.array(data_window, dtype=np.float32)

            if self.scale:
                data_window = self.transform(data_window)

            x_list.append(data_window[: self.seq_len])
            y_list.append(data_window[self.input_token_len : window_length])

        seq_x = np.stack(x_list, axis=-1).astype(np.float32)
        seq_y = np.stack(y_list, axis=-1).astype(np.float32)
        loss_mask = np.ones(self.token_num, dtype=np.int32)

        return {
            "seq_x": torch.from_numpy(seq_x),
            "seq_y": torch.from_numpy(seq_y),
            "loss_mask": torch.from_numpy(loss_mask).float(),
        }

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        if index < 0 or index >= self.total_windows:
            raise IndexError(f"Index {index} out of range [0, {self.total_windows})")

        if self._is_multivariate:
            return self._getitem_multi_variable(index)
        else:
            return self._getitem_single_variable(index)

    def __len__(self) -> int:
        return self.total_windows

    def close(self):
        for reader in self._readers.values():
            reader.close()
        self._readers.clear()
