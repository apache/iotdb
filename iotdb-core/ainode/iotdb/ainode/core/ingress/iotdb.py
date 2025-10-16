1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1import numpy as np
1import torch
1from torch.utils.data import Dataset
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.ingress.dataset import BasicDatabaseForecastDataset
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.util.cache import MemoryLRUCache
1from iotdb.ainode.core.util.decorator import singleton
1from iotdb.Session import Session
1from iotdb.table_session import TableSession, TableSessionConfig
1from iotdb.utils.Field import Field
1from iotdb.utils.IoTDBConstants import TSDataType
1
1logger = Logger()
1
1
1def get_field_value(field: Field):
1    data_type = field.get_data_type()
1    if data_type == TSDataType.INT32:
1        return field.get_int_value()
1    elif data_type == TSDataType.INT64:
1        return field.get_long_value()
1    elif data_type == TSDataType.FLOAT:
1        return field.get_float_value()
1    elif data_type == TSDataType.DOUBLE:
1        return field.get_double_value()
1    else:
1        return field.get_string_value()
1
1
1def _cache_enable() -> bool:
1    return AINodeDescriptor().get_config().get_ain_data_storage_cache_size() > 0
1
1
1class IoTDBTreeModelDataset(BasicDatabaseForecastDataset):
1    cache = MemoryLRUCache()
1
1    def __init__(
1        self,
1        model_id: str,
1        seq_len: int,
1        input_token_len: int,
1        output_token_len: int,
1        window_step: int,
1        data_schema_list: list,
1        ip: str = AINodeDescriptor().get_config().get_ain_cluster_ingress_address(),
1        port: int = AINodeDescriptor().get_config().get_ain_cluster_ingress_port(),
1        username: str = AINodeDescriptor()
1        .get_config()
1        .get_ain_cluster_ingress_username(),
1        password: str = AINodeDescriptor()
1        .get_config()
1        .get_ain_cluster_ingress_password(),
1        time_zone: str = AINodeDescriptor()
1        .get_config()
1        .get_ain_cluster_ingress_time_zone(),
1        use_rate: float = 1.0,
1        offset_rate: float = 0.0,
1    ):
1        super().__init__(
1            ip, port, seq_len, input_token_len, output_token_len, window_step
1        )
1
1        self.SHOW_TIMESERIES = "show timeseries %s%s"
1        self.COUNT_SERIES_SQL = "select count(%s) from %s%s"
1        self.FETCH_SERIES_SQL = "select %s from %s%s"
1        self.FETCH_SERIES_RANGE_SQL = "select %s from %s offset %s limit %s%s"
1
1        self.TIME_CONDITION = " where time>=%s and time<%s"
1
1        self.session = Session.init_from_node_urls(
1            node_urls=[f"{ip}:{port}"],
1            user=username,
1            password=password,
1            zone_id=time_zone,
1            use_ssl=AINodeDescriptor()
1            .get_config()
1            .get_ain_cluster_ingress_ssl_enabled(),
1            ca_certs=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file(),
1        )
1        self.session.open(False)
1        self.use_rate = use_rate
1        self.offset_rate = offset_rate
1        self._fetch_schema(data_schema_list)
1        self.cache_enable = _cache_enable()
1        self.cache_key_prefix = model_id + "_"
1
1    def _fetch_schema(self, data_schema_list: list):
1        series_to_length = {}
1        for schema in data_schema_list:
1            path_pattern = schema.schemaName
1            series_list = []
1            time_condition = (
1                self.TIME_CONDITION % (schema.timeRange[0], schema.timeRange[1])
1                if schema.timeRange
1                else ""
1            )
1            with self.session.execute_query_statement(
1                self.SHOW_TIMESERIES % (path_pattern, time_condition)
1            ) as show_timeseries_result:
1                while show_timeseries_result.has_next():
1                    # parse the name of time series according to data_schema_list
1                    series_list.append(
1                        get_field_value(show_timeseries_result.next().get_fields()[0])
1                    )
1
1            for series in series_list:
1                split_series = series.split(".")
1                with self.session.execute_query_statement(
1                    self.COUNT_SERIES_SQL
1                    % (split_series[-1], ".".join(split_series[:-1]), time_condition)
1                ) as count_series_result:
1                    while count_series_result.has_next():
1                        length = get_field_value(
1                            count_series_result.next().get_fields()[0]
1                        )
1                        # count the number (length) of time series data points
1                        # structure: {series_name: (split_series_name, length, time_condition)}
1                        series_to_length[series] = (
1                            split_series,
1                            length,
1                            time_condition,
1                        )
1
1        sorted_series = sorted(series_to_length.items(), key=lambda x: x[1][1])
1        # structure: [(split_series_name, the number of windows of this series, prefix sum of window number, window start offset, time_condition of this series), ...]
1        sorted_series_with_prefix_sum = []
1        window_sum = 0
1        for seq_name, seq_value in sorted_series:
1            # calculate and sum the number of training data windows for each time series
1            window_count = (
1                seq_value[1] - self.seq_len - self.output_token_len + 1
1            ) // self.window_step
1            if window_count <= 1:
1                continue
1            use_window_count = int(window_count * self.use_rate)
1            window_sum += use_window_count
1            sorted_series_with_prefix_sum.append(
1                (
1                    seq_value[0],
1                    use_window_count,
1                    window_sum,
1                    int(window_count * self.offset_rate),
1                    seq_value[2],
1                )
1            )
1
1        self.total_window_count = window_sum
1        self.sorted_series = sorted_series_with_prefix_sum
1
1    def __getitem__(self, index):
1        window_index = index
1        # locate the series to be queried
1        series_index = 0
1        while self.sorted_series[series_index][2] < window_index:
1            series_index += 1
1        # locate the window of this series to be queried
1        if series_index != 0:
1            window_index -= self.sorted_series[series_index - 1][2]
1        series = self.sorted_series[series_index][0]
1        window_index += self.sorted_series[series_index][3]
1        time_condition = self.sorted_series[series_index][4]
1        if self.cache_enable:
1            cache_key = self.cache_key_prefix + ".".join(series) + time_condition
1            series_data = self.cache.get(cache_key)
1            if series_data is not None:
1                # try to get the training data window from cache first
1                series_data = torch.tensor(series_data)
1                result = series_data[
1                    window_index * self.window_step : window_index * self.window_step
1                    + self.seq_len
1                    + self.output_token_len
1                ]
1                return (
1                    result[0 : self.seq_len],
1                    result[self.input_token_len : self.seq_len + self.output_token_len],
1                    np.ones(self.token_num, dtype=np.int32),
1                )
1        series_data = []
1        sql = ""
1        try:
1            if self.cache_enable:
1                # fetch all data points of the specific series when cache is enabled
1                sql = self.FETCH_SERIES_SQL % (
1                    series[-1],
1                    ".".join(series[0:-1]),
1                    time_condition,
1                )
1            else:
1                # TODO: fetch only the specific data window, current logic is wrong
1                sql = self.FETCH_SERIES_RANGE_SQL % (
1                    series[-1],
1                    ".".join(series[0:-1]),
1                    window_index,
1                    self.seq_len,
1                    time_condition,
1                )
1            with self.session.execute_query_statement(sql) as query_result:
1                while query_result.has_next():
1                    series_data.append(
1                        get_field_value(query_result.next().get_fields()[0])
1                    )
1        except Exception as e:
1            logger.error("Executing sql: {} with exception: {}".format(sql, e))
1        if self.cache_enable:
1            self.cache.put(cache_key, series_data)
1        result = series_data[
1            window_index * self.window_step : window_index * self.window_step
1            + self.seq_len
1            + self.output_token_len
1        ]
1        result = torch.tensor(result)
1        return (
1            result[0 : self.seq_len],
1            result[self.input_token_len : self.seq_len + self.output_token_len],
1            np.ones(self.token_num, dtype=np.int32),
1        )
1
1    def __len__(self):
1        return self.total_window_count
1
1
1class IoTDBTableModelDataset(BasicDatabaseForecastDataset):
1
1    DEFAULT_TAG = "__DEFAULT_TAG__"
1
1    def __init__(
1        self,
1        model_id: str,
1        seq_len: int,
1        input_token_len: int,
1        output_token_len: int,
1        window_step: int,
1        data_schema_list: list,
1        ip: str = AINodeDescriptor().get_config().get_ain_cluster_ingress_address(),
1        port: int = AINodeDescriptor().get_config().get_ain_cluster_ingress_port(),
1        username: str = AINodeDescriptor()
1        .get_config()
1        .get_ain_cluster_ingress_username(),
1        password: str = AINodeDescriptor()
1        .get_config()
1        .get_ain_cluster_ingress_password(),
1        time_zone: str = AINodeDescriptor()
1        .get_config()
1        .get_ain_cluster_ingress_time_zone(),
1        use_rate: float = 1.0,
1        offset_rate: float = 0.0,
1    ):
1        super().__init__(
1            ip, port, seq_len, input_token_len, output_token_len, window_step
1        )
1
1        table_session_config = TableSessionConfig(
1            node_urls=[f"{ip}:{port}"],
1            username=username,
1            password=password,
1            time_zone=time_zone,
1            use_ssl=AINodeDescriptor()
1            .get_config()
1            .get_ain_cluster_ingress_ssl_enabled(),
1            ca_certs=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file(),
1        )
1        self.session = TableSession(table_session_config)
1        self.use_rate = use_rate
1        self.offset_rate = offset_rate
1
1        # used for caching data
1        self._fetch_schema(data_schema_list)
1
1    def _fetch_schema(self, data_schema_list: list):
1        series_map = {}
1        for target_sql in data_schema_list:
1            target_sql = target_sql.schemaName
1            with self.session.execute_query_statement(target_sql) as target_data:
1                while target_data.has_next():
1                    cur_data = target_data.next()
1                    # TODO: currently, we only support the following simple table form
1                    time_col, value_col, tag_col = -1, -1, -1
1                    for i, field in enumerate(cur_data.get_fields()):
1                        if field.get_data_type() == TSDataType.TIMESTAMP:
1                            time_col = i
1                        elif field.get_data_type() in (
1                            TSDataType.INT32,
1                            TSDataType.INT64,
1                            TSDataType.FLOAT,
1                            TSDataType.DOUBLE,
1                        ):
1                            value_col = i
1                        elif field.get_data_type() == TSDataType.TEXT:
1                            tag_col = i
1                    if time_col == -1 or value_col == -1:
1                        raise ValueError(
1                            "The training cannot start due to invalid data schema"
1                        )
1                    if tag_col == -1:
1                        tag = self.DEFAULT_TAG
1                    else:
1                        tag = cur_data.get_fields()[tag_col].get_string_value()
1                    if tag not in series_map:
1                        series_map[tag] = []
1                    series_list = series_map[tag]
1                    series_list.append(
1                        get_field_value(cur_data.get_fields()[value_col])
1                    )
1
1        # TODO: Unify the following implementation
1        # structure: [(series_name, the number of windows of this series, prefix sum of window number, window start offset, series_data), ...]
1        series_with_prefix_sum = []
1        window_sum = 0
1        for seq_name, seq_values in series_map.items():
1            # calculate and sum the number of training data windows for each time series
1            window_count = (
1                len(seq_values) - self.seq_len - self.output_token_len + 1
1            ) // self.window_step
1            if window_count <= 1:
1                continue
1            use_window_count = int(window_count * self.use_rate)
1            window_sum += use_window_count
1            series_with_prefix_sum.append(
1                (
1                    seq_name,
1                    use_window_count,
1                    window_sum,
1                    int(window_count * self.offset_rate),
1                    seq_values,
1                )
1            )
1
1        self.total_window_count = window_sum
1        self.series_with_prefix_sum = series_with_prefix_sum
1
1    def __getitem__(self, index):
1        window_index = index
1        # locate the series to be queried
1        series_index = 0
1        while self.series_with_prefix_sum[series_index][1] < window_index:
1            series_index += 1
1        # locate the window of this series to be queried
1        if series_index != 0:
1            window_index -= self.series_with_prefix_sum[series_index - 1][2]
1        window_index += self.series_with_prefix_sum[series_index][3]
1        result = self.series_with_prefix_sum[series_index][4][
1            window_index * self.window_step : window_index * self.window_step
1            + self.seq_len
1            + self.output_token_len
1        ]
1        result = torch.tensor(result)
1        return (
1            result[0 : self.seq_len],
1            result[self.input_token_len : self.seq_len + self.output_token_len],
1            np.ones(self.token_num, dtype=np.int32),
1        )
1
1    def __len__(self):
1        return self.total_window_count
1
1
1def register_dataset(key: str, dataset: Dataset):
1    DatasetFactory().register(key, dataset)
1
1
1@singleton
1class DatasetFactory(object):
1
1    def __init__(self):
1        self.dataset_list = {
1            "iotdb.table": IoTDBTableModelDataset,
1            "iotdb.tree": IoTDBTreeModelDataset,
1        }
1
1    def register(self, key: str, dataset: Dataset):
1        if key not in self.dataset_list:
1            self.dataset_list[key] = dataset
1        else:
1            raise KeyError(f"Dataset {key} already exists")
1
1    def deregister(self, key: str):
1        del self.dataset_list[key]
1
1    def get_dataset(self, key: str):
1        if key not in self.dataset_list.keys():
1            raise KeyError(f"Dataset {key} does not exist")
1        return self.dataset_list[key]
1