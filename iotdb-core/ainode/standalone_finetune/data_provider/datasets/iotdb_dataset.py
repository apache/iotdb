import logging
from typing import Any, Dict, List, Optional

import numpy as np
import torch

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.IoTDBConstants import TSDataType
from timecho.ainode.core.data_provider.datasets.base_dataset import (
    BasicTimeSeriesDataset,
)
from timecho.ainode.core.data_provider.processor.data_scaler import ScalerType

logger = logging.getLogger(__name__)

DEFAULT_TAG = "__DEFAULT_TAG__"


def get_field_value(field):
    """
    Extract value from IoTDB Field based on its data type.
    """
    try:
        from iotdb.utils.IoTDBConstants import TSDataType

        data_type = field.get_data_type()
        if data_type == TSDataType.INT32:
            return field.get_int_value()
        elif data_type == TSDataType.INT64:
            return field.get_long_value()
        elif data_type == TSDataType.FLOAT:
            return field.get_float_value()
        elif data_type == TSDataType.DOUBLE:
            return field.get_double_value()
        else:
            return field.get_string_value()
    except ImportError:
        return float(field.get_string_value())


class IoTDBDataset(BasicTimeSeriesDataset):
    """
    Dataset for loading data from IoTDB using table model queries.

    Supports two modes:
    - Single-variable: Each series is treated independently (default)
    - Multi-variable: Multiple series are combined into a feature matrix

    Note: For multiprocess dataloader (num_workers > 0), connection is
    established per-worker in __getitem__ or via worker_init_fn.
    """

    def __init__(
        self,
        data_schema_list: List[Any],
        aggregation_interval: str = "1s",
        multivariate: bool = False,
        train_ratio: float = 0.7,
        seq_len: int = 2880,
        input_token_len: int = 16,
        output_token_lens: List[int] = None,
        window_step: int = 1,
        scale: bool = False,
        scaler_type: ScalerType = "standard",
        use_rate: float = 1.0,
        offset_rate: float = 0.0,
        ip: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
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

        self.data_schema_list = data_schema_list
        self.aggregation_interval = aggregation_interval
        self.multivariate = multivariate
        self.train_ratio = train_ratio

        cfg = AINodeDescriptor().get_config()
        self.ip = ip or cfg.get_ain_cluster_ingress_address()
        self.port = port or cfg.get_ain_cluster_ingress_port()
        self.username = username or cfg.get_ain_cluster_ingress_username()
        self.password = password or cfg.get_ain_cluster_ingress_password()
        self._time_zone = cfg.get_ain_cluster_ingress_time_zone()
        self._use_ssl = cfg.get_ain_cluster_ingress_ssl_enabled()
        self._ca_certs = cfg.get_ain_thrift_ssl_cert_file()

        self.series_map: Dict[str, List[float]] = {}
        self.prefix_sum: List[int] = [0]
        self.tag_list: List[str] = []
        self._min_series_len: int = 0

        self._fetch_data()

    def _get_session(self):
        try:
            config = TableSessionConfig(
                node_urls=[f"{self.ip}:{self.port}"],
                username=self.username,
                password=self.password,
                time_zone=self._time_zone,
                use_ssl=self._use_ssl,
                ca_certs=self._ca_certs,
            )
            return TableSession(config)
        except ImportError:
            logger.warning("iotdb package not available")
            return None

    def _fetch_data(self):
        """
        Fetch data from IoTDB using table model queries.
        """
        session = self._get_session()

        if session is None:
            logger.warning("No IoTDB session available")
            self._compute_index()
            return

        try:
            for schema in self.data_schema_list:
                schema_name = schema.schemaName

                try:
                    with session.execute_query_statement(schema_name) as result_set:
                        while result_set.has_next():
                            cur_data = result_set.next()
                            fields = cur_data.get_fields()

                            time_col, value_col, tag_col = -1, -1, -1
                            for i, field in enumerate(fields):
                                dt = field.get_data_type()
                                if dt == TSDataType.TIMESTAMP:
                                    time_col = i
                                elif dt in (
                                    TSDataType.INT32,
                                    TSDataType.INT64,
                                    TSDataType.FLOAT,
                                    TSDataType.DOUBLE,
                                ):
                                    value_col = i
                                elif dt == TSDataType.TEXT:
                                    tag_col = i

                            if time_col == -1 or value_col == -1:
                                continue

                            if tag_col == -1:
                                tag = DEFAULT_TAG
                            else:
                                tag = fields[tag_col].get_string_value()

                            if tag not in self.series_map:
                                self.series_map[tag] = []
                                self.tag_list.append(tag)

                            self.series_map[tag].append(
                                get_field_value(fields[value_col])
                            )
                except Exception as e:
                    logger.error(f"Error fetching data for {schema_name}: {e}")

        except ImportError:
            logger.warning("iotdb package not available")
        finally:
            try:
                session.close()
            except Exception:
                pass

        self._compute_index()

    def _compute_index(self):
        """
        Compute scaler parameters and sliding window index for all series.

        This method performs two steps:

        1. Scaler fitting (if scale=True):
           - For each series, extract its training portion (first train_ratio of total data)
           - Concatenate all training portions across series and fit the scaler
           - Then transform all series data (full length) using the fitted scaler
           - This ensures val/test splits also use a scaler fitted only on training data

        2. Sliding window index construction:
           - window_length = seq_len + max(output_token_lens) (total points needed per window)
           - full_window_count = how many non-overlapping/step windows fit in the data
           - use_rate controls what fraction of windows to use (set by data_factory per split)
           - offset_rate controls the starting position (e.g. val starts after train portion)

           Multivariate mode:
             All series are aligned by the shortest series length (_min_series_len).
             A single global window count is computed, stored in prefix_sum[1].

           Single-variable mode:
             Each series has its own window count. prefix_sum is a cumulative list
             used by _find_series_by_index() for O(log n) binary search to locate
             which series a global index belongs to.
        """
        # Step 1: Fit scaler on training portion of data
        if self.scale and len(self.series_map) > 0:
            all_train_data = []
            for tag, series_data in self.series_map.items():
                series_array = np.array(series_data, dtype=np.float32)
                train_end = int(len(series_array) * self.train_ratio)
                all_train_data.append(series_array[:train_end])

            if all_train_data:
                concatenated_train_data = np.concatenate(all_train_data)
                self.fit_scaler(concatenated_train_data)

                # Apply the fitted scaler to All data (not just training portion)
                for tag in self.tag_list:
                    series_data = np.array(self.series_map[tag], dtype=np.float32)
                    self.series_map[tag] = self.transform(series_data).tolist()

        # Step 2: Build sliding window index
        self.prefix_sum = [0]
        window_length = self.seq_len + self.output_token_len

        if self.multivariate and len(self.tag_list) > 1:
            # Multivariate: align all series to the shortest one
            self._min_series_len = min(
                len(self.series_map[tag]) for tag in self.tag_list
            )
            full_window_count = max(
                0, (self._min_series_len - window_length) // self.window_step + 1
            )
            use_window_count = int(full_window_count * self.use_rate)
            self._window_offset = int(full_window_count * self.offset_rate)
            self.prefix_sum.append(use_window_count)
        else:
            # Single-variable: each series has independent window count and offset
            self._series_window_offset = []
            for tag in self.tag_list:
                data_len = len(self.series_map[tag])
                full_window_count = max(
                    0, (data_len - window_length) // self.window_step + 1
                )
                use_window_count = int(full_window_count * self.use_rate)
                per_series_offset = int(full_window_count * self.offset_rate)
                self._series_window_offset.append(per_series_offset)
                # prefix_sum[i+1] = cumulative window count up to series i
                cumulative_count = self.prefix_sum[-1] + use_window_count
                self.prefix_sum.append(cumulative_count)

    def _find_series_by_index(self, index: int):
        """
        Find series index and local window index using prefix_sum.
        """
        left, right = 0, len(self.tag_list) - 1
        while left < right:
            mid = (left + right) // 2
            if self.prefix_sum[mid + 1] <= index:
                left = mid + 1
            else:
                right = mid

        series_idx = left
        local_window_idx = index - self.prefix_sum[series_idx]
        return series_idx, local_window_idx

    def _getitem_single_variable(self, index: int) -> Dict[str, torch.Tensor]:
        """Get item for single-variable mode."""
        series_idx, local_window_idx = self._find_series_by_index(index)
        tag = self.tag_list[series_idx]
        series_data = self.series_map[tag]

        # Apply per-series offset so val/test splits read from the correct data range
        offset = self._series_window_offset[series_idx]
        s_begin = (local_window_idx + offset) * self.window_step
        s_end = s_begin + self.seq_len

        r_begin = s_begin + self.input_token_len
        r_end = s_end + self.output_token_len

        seq_x = np.array(series_data[s_begin:s_end], dtype=np.float32)
        seq_y = np.array(series_data[r_begin:r_end], dtype=np.float32)
        loss_mask = np.ones(self.token_num, dtype=np.int32)

        return {
            "seq_x": torch.from_numpy(seq_x),
            "seq_y": torch.from_numpy(seq_y),
            "loss_mask": torch.from_numpy(loss_mask).float(),
        }

    def _getitem_multi_variable(self, index: int) -> Dict[str, torch.Tensor]:
        """Get item for multi-variable mode."""
        offset = getattr(self, "_window_offset", 0)
        s_begin = (index + offset) * self.window_step
        s_end = s_begin + self.seq_len
        r_begin = s_begin + self.input_token_len
        r_end = s_end + self.output_token_len

        x_list = []
        y_list = []

        for tag in self.tag_list:
            series_data = self.series_map[tag]
            x_list.append(series_data[s_begin:s_end])
            y_list.append(series_data[r_begin:r_end])

        seq_x = np.stack(x_list, axis=-1).astype(np.float32)
        seq_y = np.stack(y_list, axis=-1).astype(np.float32)
        loss_mask = np.ones(self.token_num, dtype=np.int32)

        return {
            "seq_x": torch.from_numpy(seq_x),
            "seq_y": torch.from_numpy(seq_y),
            "loss_mask": torch.from_numpy(loss_mask).float(),
        }

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        if index < 0 or index >= len(self):
            raise IndexError(f"Index {index} out of range [0, {len(self)})")

        if self.multivariate and len(self.tag_list) > 1:
            return self._getitem_multi_variable(index)
        else:
            return self._getitem_single_variable(index)

    def __len__(self) -> int:
        return self.prefix_sum[-1] if len(self.prefix_sum) > 1 else 0
