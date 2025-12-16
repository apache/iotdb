# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import numpy as np
import torch
from torch.utils.data import Dataset

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.ingress.dataset import BasicDatabaseForecastDataset
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.util.cache import MemoryLRUCache
from iotdb.ainode.core.util.decorator import singleton
from iotdb.Session import Session
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType

logger = Logger()


def get_field_value(field: Field):
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


def _cache_enable() -> bool:
    return AINodeDescriptor().get_config().get_ain_data_storage_cache_size() > 0


class IoTDBTreeModelDataset(BasicDatabaseForecastDataset):
    cache = MemoryLRUCache()

    def __init__(
        self,
        model_id: str,
        seq_len: int,
        input_token_len: int,
        output_token_len: int,
        window_step: int,
        data_schema_list: list,
        ip: str = AINodeDescriptor().get_config().get_ain_cluster_ingress_address(),
        port: int = AINodeDescriptor().get_config().get_ain_cluster_ingress_port(),
        username: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_username(),
        password: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_password(),
        time_zone: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_time_zone(),
        use_rate: float = 1.0,
        offset_rate: float = 0.0,
    ):
        super().__init__(
            ip, port, seq_len, input_token_len, output_token_len, window_step
        )

        self.SHOW_TIMESERIES = "show timeseries %s%s"
        self.COUNT_SERIES_SQL = "select count(%s) from %s%s"
        self.FETCH_SERIES_SQL = "select %s from %s%s"
        self.FETCH_SERIES_RANGE_SQL = "select %s from %s offset %s limit %s%s"

        self.TIME_CONDITION = " where time>=%s and time<%s"

        self.session = Session.init_from_node_urls(
            node_urls=[f"{ip}:{port}"],
            user=username,
            password=password,
            zone_id=time_zone,
            use_ssl=AINodeDescriptor()
            .get_config()
            .get_ain_cluster_ingress_ssl_enabled(),
            ca_certs=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file(),
        )
        self.session.open(False)
        self.use_rate = use_rate
        self.offset_rate = offset_rate
        self._fetch_schema(data_schema_list)
        self.cache_enable = _cache_enable()
        self.cache_key_prefix = model_id + "_"

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
            # calculate and sum the number of training data windows for each time series
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

    def __getitem__(self, index):
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
        if self.cache_enable:
            cache_key = self.cache_key_prefix + ".".join(series) + time_condition
            series_data = self.cache.get(cache_key)
            if series_data is not None:
                # try to get the training data window from cache first
                series_data = torch.tensor(series_data)
                result = series_data[
                    window_index * self.window_step : window_index * self.window_step
                    + self.seq_len
                    + self.output_token_len
                ]
                return (
                    result[0 : self.seq_len],
                    result[self.input_token_len : self.seq_len + self.output_token_len],
                    np.ones(self.token_num, dtype=np.int32),
                )
        series_data = []
        sql = ""
        try:
            if self.cache_enable:
                # fetch all data points of the specific series when cache is enabled
                sql = self.FETCH_SERIES_SQL % (
                    series[-1],
                    ".".join(series[0:-1]),
                    time_condition,
                )
            else:
                # TODO: fetch only the specific data window, current logic is wrong
                sql = self.FETCH_SERIES_RANGE_SQL % (
                    series[-1],
                    ".".join(series[0:-1]),
                    window_index,
                    self.seq_len,
                    time_condition,
                )
            with self.session.execute_query_statement(sql) as query_result:
                while query_result.has_next():
                    series_data.append(
                        get_field_value(query_result.next().get_fields()[0])
                    )
        except Exception as e:
            logger.error("Executing sql: {} with exception: {}".format(sql, e))
        if self.cache_enable:
            self.cache.put(cache_key, series_data)
        result = series_data[
            window_index * self.window_step : window_index * self.window_step
            + self.seq_len
            + self.output_token_len
        ]
        result = torch.tensor(result)
        return (
            result[0 : self.seq_len],
            result[self.input_token_len : self.seq_len + self.output_token_len],
            np.ones(self.token_num, dtype=np.int32),
        )

    def __len__(self):
        return self.total_window_count


class IoTDBTableModelDataset(BasicDatabaseForecastDataset):

    DEFAULT_TAG = "__DEFAULT_TAG__"

    def __init__(
        self,
        model_id: str,
        seq_len: int,
        input_token_len: int,
        output_token_len: int,
        window_step: int,
        data_schema_list: list,
        ip: str = AINodeDescriptor().get_config().get_ain_cluster_ingress_address(),
        port: int = AINodeDescriptor().get_config().get_ain_cluster_ingress_port(),
        username: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_username(),
        password: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_password(),
        time_zone: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_time_zone(),
        use_rate: float = 1.0,
        offset_rate: float = 0.0,
    ):
        super().__init__(
            ip, port, seq_len, input_token_len, output_token_len, window_step
        )

        table_session_config = TableSessionConfig(
            node_urls=[f"{ip}:{port}"],
            username=username,
            password=password,
            time_zone=time_zone,
            use_ssl=AINodeDescriptor()
            .get_config()
            .get_ain_cluster_ingress_ssl_enabled(),
            ca_certs=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file(),
        )
        self.session = TableSession(table_session_config)
        self.use_rate = use_rate
        self.offset_rate = offset_rate

        # used for caching data
        self._fetch_schema(data_schema_list)

    def _fetch_schema(self, data_schema_list: list):
        series_map = {}
        for target_sql in data_schema_list:
            target_sql = target_sql.schemaName
            with self.session.execute_query_statement(target_sql) as target_data:
                while target_data.has_next():
                    cur_data = target_data.next()
                    # TODO: currently, we only support the following simple table form
                    time_col, value_col, tag_col = -1, -1, -1
                    for i, field in enumerate(cur_data.get_fields()):
                        if field.get_data_type() == TSDataType.TIMESTAMP:
                            time_col = i
                        elif field.get_data_type() in (
                            TSDataType.INT32,
                            TSDataType.INT64,
                            TSDataType.FLOAT,
                            TSDataType.DOUBLE,
                        ):
                            value_col = i
                        elif field.get_data_type() == TSDataType.TEXT:
                            tag_col = i
                    if time_col == -1 or value_col == -1:
                        raise ValueError(
                            "The training cannot start due to invalid data schema"
                        )
                    if tag_col == -1:
                        tag = self.DEFAULT_TAG
                    else:
                        tag = cur_data.get_fields()[tag_col].get_string_value()
                    if tag not in series_map:
                        series_map[tag] = []
                    series_list = series_map[tag]
                    series_list.append(
                        get_field_value(cur_data.get_fields()[value_col])
                    )

        # TODO: Unify the following implementation
        # structure: [(series_name, the number of windows of this series, prefix sum of window number, window start offset, series_data), ...]
        series_with_prefix_sum = []
        window_sum = 0
        for seq_name, seq_values in series_map.items():
            # calculate and sum the number of training data windows for each time series
            window_count = (
                len(seq_values) - self.seq_len - self.output_token_len + 1
            ) // self.window_step
            if window_count <= 1:
                continue
            use_window_count = int(window_count * self.use_rate)
            window_sum += use_window_count
            series_with_prefix_sum.append(
                (
                    seq_name,
                    use_window_count,
                    window_sum,
                    int(window_count * self.offset_rate),
                    seq_values,
                )
            )

        self.total_window_count = window_sum
        self.series_with_prefix_sum = series_with_prefix_sum

    def __getitem__(self, index):
        window_index = index
        # locate the series to be queried
        series_index = 0
        while self.series_with_prefix_sum[series_index][1] < window_index:
            series_index += 1
        # locate the window of this series to be queried
        if series_index != 0:
            window_index -= self.series_with_prefix_sum[series_index - 1][2]
        window_index += self.series_with_prefix_sum[series_index][3]
        result = self.series_with_prefix_sum[series_index][4][
            window_index * self.window_step : window_index * self.window_step
            + self.seq_len
            + self.output_token_len
        ]
        result = torch.tensor(result)
        return (
            result[0 : self.seq_len],
            result[self.input_token_len : self.seq_len + self.output_token_len],
            np.ones(self.token_num, dtype=np.int32),
        )

    def __len__(self):
        return self.total_window_count


def register_dataset(key: str, dataset: Dataset):
    DatasetFactory().register(key, dataset)


@singleton
class DatasetFactory(object):

    def __init__(self):
        self.dataset_list = {
            "iotdb.table": IoTDBTableModelDataset,
            "iotdb.tree": IoTDBTreeModelDataset,
        }

    def register(self, key: str, dataset: Dataset):
        if key not in self.dataset_list:
            self.dataset_list[key] = dataset
        else:
            raise KeyError(f"Dataset {key} already exists")

    def deregister(self, key: str):
        del self.dataset_list[key]

    def get_dataset(self, key: str):
        if key not in self.dataset_list.keys():
            raise KeyError(f"Dataset {key} does not exist")
        return self.dataset_list[key]
