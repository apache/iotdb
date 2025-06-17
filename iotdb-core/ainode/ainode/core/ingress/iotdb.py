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
from iotdb.Session import Session
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from torch.utils.data import Dataset

from ainode.core.config import AINodeDescriptor
from ainode.core.ingress.dataset import BasicDatabaseForecastDataset
from ainode.core.log import Logger
from ainode.core.util.cache import MemoryLRUCache
from ainode.core.util.decorator import singleton

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
        input_len: int,
        out_len: int,
        data_schema_list: list,
        ip: str = "127.0.0.1",
        port: int = 6667,
        username: str = "root",
        password: str = "root",
        time_zone: str = "UTC+8",
        start_split: float = 0,
        end_split: float = 1,
    ):
        super().__init__(ip, port, input_len, out_len)

        self.SHOW_TIMESERIES = "show timeseries %s%s"
        self.COUNT_SERIES_SQL = "select count(%s) from %s%s"
        self.FETCH_SERIES_SQL = "select %s from %s%s"
        self.FETCH_SERIES_RANGE_SQL = "select %s from %s offset %s limit %s%s"

        self.TIME_CONDITION = " where time>%s and time<%s"

        self.session = Session.init_from_node_urls(
            node_urls=[f"{ip}:{port}"],
            user=username,
            password=password,
            zone_id=time_zone,
        )
        self.session.open(False)
        self.context_length = self.input_len + self.output_len
        self.token_num = self.context_length // self.input_len
        self._fetch_schema(data_schema_list)
        self.start_idx = int(self.total_count * start_split)
        self.end_idx = int(self.total_count * end_split)
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
                        series_to_length[series] = (
                            split_series,
                            length,
                            time_condition,
                        )

        sorted_series = sorted(series_to_length.items(), key=lambda x: x[1][1])
        sorted_series_with_prefix_sum = []
        window_sum = 0
        for seq_name, seq_value in sorted_series:
            window_count = seq_value[1] - self.context_length + 1
            if window_count <= 0:
                continue
            window_sum += window_count
            sorted_series_with_prefix_sum.append(
                (seq_value[0], window_count, window_sum, seq_value[2])
            )

        self.total_count = window_sum
        self.sorted_series = sorted_series_with_prefix_sum

    def __getitem__(self, index):
        window_index = index
        series_index = 0
        while self.sorted_series[series_index][2] < window_index:
            series_index += 1

        if series_index != 0:
            window_index -= self.sorted_series[series_index - 1][2]

        if window_index != 0:
            window_index -= 1
        series = self.sorted_series[series_index][0]
        time_condition = self.sorted_series[series_index][3]
        if self.cache_enable:
            cache_key = self.cache_key_prefix + ".".join(series) + time_condition
            series_data = self.cache.get(cache_key)
            if series_data is not None:
                series_data = torch.tensor(series_data)
                result = series_data[window_index : window_index + self.context_length]
                return (
                    result[0 : self.input_len],
                    result[-self.output_len :],
                    np.ones(self.token_num, dtype=np.int32),
                )
        result = []
        sql = ""
        try:
            if self.cache_enable:
                sql = self.FETCH_SERIES_SQL % (
                    series[-1],
                    ".".join(series[0:-1]),
                    time_condition,
                )
            else:
                sql = self.FETCH_SERIES_RANGE_SQL % (
                    series[-1],
                    ".".join(series[0:-1]),
                    window_index,
                    self.context_length,
                    time_condition,
                )
            with self.session.execute_query_statement(sql) as query_result:
                while query_result.has_next():
                    result.append(get_field_value(query_result.next().get_fields()[0]))
        except Exception as e:
            logger.error("Executing sql: {} with exception: {}".format(sql, e))
        if self.cache_enable:
            self.cache.put(cache_key, result)
        result = torch.tensor(result)
        return (
            result[0 : self.input_len],
            result[-self.output_len :],
            np.ones(self.token_num, dtype=np.int32),
        )

    def __len__(self):
        return self.end_idx - self.start_idx


class IoTDBTableModelDataset(BasicDatabaseForecastDataset):

    def __init__(
        self,
        input_len: int,
        out_len: int,
        data_schema_list: list,
        ip: str = "127.0.0.1",
        port: int = 6667,
        username: str = "root",
        password: str = "root",
        time_zone: str = "UTC+8",
        start_split: float = 0,
        end_split: float = 1,
    ):
        super().__init__(ip, port, input_len, out_len)
        if end_split < start_split:
            raise ValueError("end_split must be greater than start_split")

        # database , table
        self.SELECT_SERIES_FORMAT_SQL = "select distinct item_id from %s"
        self.COUNT_SERIES_LENGTH_SQL = (
            "select count(value) from %s where item_id = '%s'"
        )
        self.FETCH_SERIES_SQL = (
            "select value from %s where item_id = '%s' offset %s limit %s"
        )
        self.SERIES_NAME = "%s.%s"

        table_session_config = TableSessionConfig(
            node_urls=[f"{ip}:{port}"],
            username=username,
            password=password,
            time_zone=time_zone,
        )

        self.session = TableSession(table_session_config)
        self.context_length = self.input_len + self.output_len
        self.token_num = self.context_length // self.input_len
        self._fetch_schema(data_schema_list)

        self.start_index = int(self.total_count * start_split)
        self.end_index = self.total_count * end_split

    def _fetch_schema(self, data_schema_list: list):
        series_to_length = {}
        for data_schema in data_schema_list:
            series_list = []
            with self.session.execute_query_statement(
                self.SELECT_SERIES_FORMAT_SQL % data_schema
            ) as show_devices_result:
                while show_devices_result.has_next():
                    series_list.append(
                        get_field_value(show_devices_result.next().get_fields()[0])
                    )

            for series in series_list:
                with self.session.execute_query_statement(
                    self.COUNT_SERIES_LENGTH_SQL % (data_schema.schemaName, series)
                ) as count_series_result:
                    length = get_field_value(count_series_result.next().get_fields()[0])
                    series_to_length[
                        self.SERIES_NAME % (data_schema.schemaName, series)
                    ] = length

        sorted_series = sorted(series_to_length.items(), key=lambda x: x[1])
        sorted_series_with_prefix_sum = []
        window_sum = 0
        for seq_name, seq_length in sorted_series:
            window_count = seq_length - self.context_length + 1
            if window_count < 0:
                continue
            window_sum += window_count
            sorted_series_with_prefix_sum.append((seq_name, window_count, window_sum))

        self.total_count = window_sum
        self.sorted_series = sorted_series_with_prefix_sum

    def __getitem__(self, index):
        window_index = index

        series_index = 0

        while self.sorted_series[series_index][2] < window_index:
            series_index += 1

        if series_index != 0:
            window_index -= self.sorted_series[series_index - 1][2]

        if window_index != 0:
            window_index -= 1
        series = self.sorted_series[series_index][0]
        schema = series.split(".")

        result = []
        sql = self.FETCH_SERIES_SQL % (
            schema[0:1],
            schema[2],
            window_index,
            self.context_length,
        )
        try:
            with self.session.execute_query_statement(sql) as query_result:
                while query_result.has_next():
                    result.append(get_field_value(query_result.next().get_fields()[0]))
        except Exception as e:
            logger.error("Executing sql: {} with exception: {}".format(sql, e))
        result = torch.tensor(result)
        return (
            result[0 : self.input_len],
            result[-self.output_len :],
            np.ones(self.token_num, dtype=np.int32),
        )

    def __len__(self):
        return self.end_index - self.start_index


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
