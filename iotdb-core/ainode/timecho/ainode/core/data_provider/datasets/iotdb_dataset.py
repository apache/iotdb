import logging
import sys
import uuid
from typing import Any, Dict, List, Optional

import numpy as np
import torch

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.util.cache import MemoryLRUCache
from iotdb.Session import Session
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


class IoTDBTableModelDataset(BasicTimeSeriesDataset):
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
        output_token_len: int = None,
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
        if output_token_len is None:
            output_token_len = 96
        super().__init__(
            seq_len,
            input_token_len,
            output_token_len,
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
        self._use_ssl = cfg.get_ain_cluster_ingress_ssl_enabled()
        self._ca_certs = cfg.get_ain_thrift_ssl_cert_file()

        self.series_map: Dict[str, List[List[float]]] = {}
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

                            time_col, tag_col = -1, -1
                            value_cols = []
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
                                    value_cols.append(i)
                                elif dt == TSDataType.TEXT:
                                    tag_col = i

                            if time_col == -1:
                                raise ValueError(
                                    "Finetune failed because no time column found."
                                )

                            if tag_col == -1:
                                tag = DEFAULT_TAG
                            else:
                                raise ValueError(
                                    "Finetune failed because tag column is not supported."
                                )

                            if len(value_cols) > 1:
                                self.multivariate = True
                            if tag not in self.series_map:
                                self.series_map[tag] = []
                                for _ in range(len(value_cols)):
                                    self.series_map[tag].append([])
                                self.tag_list.append(tag)
                            for i, value_col in enumerate(value_cols):
                                self.series_map[tag][i].append(
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
            for tag, series_dataset in self.series_map.items():
                for series_data in series_dataset:
                    series_array = np.array(series_data, dtype=np.float32)
                    train_end = int(len(series_array) * self.train_ratio)
                    all_train_data.append(series_array[:train_end])

            if all_train_data:
                concatenated_train_data = np.concatenate(all_train_data)
                self.fit_scaler(concatenated_train_data)

                # Apply the fitted scaler to All data (not just training portion)
                for tag in self.tag_list:
                    for index in range(len(self.series_map[tag])):
                        series_data = np.array(
                            self.series_map[tag][index], dtype=np.float32
                        )
                        self.series_map[tag][index] = self.transform(
                            series_data
                        ).tolist()

        # Step 2: Build sliding window index
        self.prefix_sum = [0]
        window_length = self.seq_len + self.output_token_len

        if self.multivariate and len(self.tag_list) > 1:
            # Multivariate: align all series to the shortest one
            self._min_series_len = sys.maxsize
            for tag in self.tag_list:
                for series_data in self.series_map[tag]:
                    self._min_series_len = min(self._min_series_len, len(series_data))
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
                # Supports only 1 series
                data_len = len(self.series_map[tag][0])
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
        series_data = self.series_map[tag][0]

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
            for series_data in self.series_map[tag]:
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

        if self.multivariate:
            return self._getitem_multi_variable(index)
        else:
            return self._getitem_single_variable(index)

    def __len__(self) -> int:
        return self.prefix_sum[-1] if len(self.prefix_sum) > 1 else 0


class IoTDBTreeModelDataset(BasicTimeSeriesDataset):
    cache = MemoryLRUCache()

    def __init__(
        self,
        data_schema_list: list,
        seq_len: int = 2880,
        input_token_len: int = 16,
        output_token_len: int = None,
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
        if output_token_len is None:
            output_token_len = 96
        super().__init__(
            seq_len,
            input_token_len,
            output_token_len,
            window_step,
            scale,
            scaler_type,
            use_rate,
            offset_rate,
        )

        cfg = AINodeDescriptor().get_config()
        self.ip = ip or cfg.get_ain_cluster_ingress_address()
        self.port = port or cfg.get_ain_cluster_ingress_port()
        self.username = username or cfg.get_ain_cluster_ingress_username()
        self.password = password or cfg.get_ain_cluster_ingress_password()
        self._use_ssl = cfg.get_ain_cluster_ingress_ssl_enabled()
        self._ca_certs = cfg.get_ain_thrift_ssl_cert_file()

        self.SHOW_TIMESERIES = "show timeseries %s%s"
        self.COUNT_SERIES_SQL = "select count(%s) from %s%s"
        self.FETCH_SERIES_SQL = "select %s from %s%s"
        self.FETCH_SERIES_RANGE_SQL = "select %s from %s offset %s limit %s%s"

        self.TIME_CONDITION = " where time>=%s and time<%s"

        self.session = Session.init_from_node_urls(
            node_urls=[f"{self.ip}:{self.port}"],
            user=self.username,
            password=self.password,
            use_ssl=self._use_ssl,
            ca_certs=self._ca_certs,
        )
        self.session.open(False)
        self.use_rate = use_rate
        self.offset_rate = offset_rate
        self.cache_key_prefix = uuid.uuid4().hex
        self._fetch_schema(data_schema_list)

    def _fetch_schema(self, data_schema_list: list):
        series_to_length = {}
        for schema in data_schema_list:
            path_pattern = schema.schemaName
            series_list = []
            time_condition = (
                self.TIME_CONDITION % (schema.timeRange[0], schema.timeRange[1])
                if schema.timeRange
                else ""
            )
            with self.session.execute_query_statement(
                self.SHOW_TIMESERIES % (path_pattern, time_condition)
            ) as show_timeseries_result:
                while show_timeseries_result.has_next():
                    # parse the name of time series according to data_schema_list
                    series_list.append(
                        get_field_value(show_timeseries_result.next().get_fields()[0])
                    )

            for series in series_list:
                split_series = series.split(".")
                with self.session.execute_query_statement(
                    self.COUNT_SERIES_SQL
                    % (split_series[-1], ".".join(split_series[:-1]), time_condition)
                ) as count_series_result:
                    while count_series_result.has_next():
                        length = get_field_value(
                            count_series_result.next().get_fields()[0]
                        )
                        # count the number (length) of time series data points
                        # structure: {series_name: (split_series_name, length, time_condition)}
                        series_to_length[series] = (
                            split_series,
                            length,
                            time_condition,
                        )

        sorted_series = sorted(series_to_length.items(), key=lambda x: x[1][1])
        # structure: [(split_series_name, the number of windows of this series, prefix sum of window number, window start offset, time_condition of this series), ...]
        sorted_series_with_prefix_sum = []
        window_sum = 0
        for seq_name, seq_value in sorted_series:
            # calculate and sum the number of tuning data windows for each time series
            window_count = (
                seq_value[1] - self.seq_len - self.output_token_len + 1
            ) // self.window_step
            if window_count <= 1:
                continue
            use_window_count = int(window_count * self.use_rate)
            window_sum += use_window_count
            sorted_series_with_prefix_sum.append(
                (
                    seq_value[0],
                    use_window_count,
                    window_sum,
                    int(window_count * self.offset_rate),
                    seq_value[2],
                )
            )

        self.total_window_count = window_sum
        self.sorted_series = sorted_series_with_prefix_sum

    def __getitem__(self, index) -> Dict[str, torch.Tensor]:
        window_index = index
        # locate the series to be queried
        series_index = 0
        while self.sorted_series[series_index][2] < window_index:
            series_index += 1
        # locate the window of this series to be queried
        if series_index != 0:
            window_index -= self.sorted_series[series_index - 1][2]
        series = self.sorted_series[series_index][0]
        window_index += self.sorted_series[series_index][3]
        time_condition = self.sorted_series[series_index][4]

        cache_key = self.cache_key_prefix + ".".join(series) + time_condition
        series_data = self.cache.get(cache_key)
        if series_data is not None:
            # try to get the tuning data window from cache first
            series_data = torch.tensor(series_data).float()
            result = series_data[
                window_index * self.window_step : window_index * self.window_step
                + self.seq_len
                + self.output_token_len
            ]
            return {
                "seq_x": result[0 : self.seq_len],
                "seq_y": result[
                    self.input_token_len : self.seq_len + self.output_token_len
                ],
                "loss_mask": torch.from_numpy(
                    np.ones(self.token_num, dtype=np.int32)
                ).float(),
            }
        series_data = []
        sql = ""
        try:
            # fetch all data points of the specific series when cache is enabled
            sql = self.FETCH_SERIES_SQL % (
                series[-1],
                ".".join(series[0:-1]),
                time_condition,
            )
            with self.session.execute_query_statement(sql) as query_result:
                while query_result.has_next():
                    series_data.append(
                        get_field_value(query_result.next().get_fields()[0])
                    )
        except Exception as e:
            logger.error("Executing sql: {} with exception: {}".format(sql, e))

        self.cache.put(cache_key, series_data)
        result = series_data[
            window_index * self.window_step : window_index * self.window_step
            + self.seq_len
            + self.output_token_len
        ]
        result = torch.tensor(result).float()
        return {
            "seq_x": result[0 : self.seq_len],
            "seq_y": result[
                self.input_token_len : self.seq_len + self.output_token_len
            ],
            "loss_mask": torch.from_numpy(
                np.ones(self.token_num, dtype=np.int32)
            ).float(),
        }

    def __len__(self) -> int:
        return self.total_window_count
