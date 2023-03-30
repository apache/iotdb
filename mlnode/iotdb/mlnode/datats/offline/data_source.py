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
import pandas as pd

from iotdb.mlnode import serde
from iotdb.mlnode.client import client_manager


class DataSource(object):
    """
    Pre-fetched in multi-variate time series in memory

    Methods:
        get_data: returns self.data, the time series value (Numpy.2DArray)
        get_timestamp: returns self.timestamp, the aligned timestamp value
    """

    def __init__(self):
        self.data = None
        self.timestamp = None

    def _read_data(self):
        raise NotImplementedError

    def get_data(self):
        return self.data

    def get_timestamp(self):
        return self.timestamp


class FileDataSource(DataSource):
    def __init__(self, filename: str = None):
        super(FileDataSource, self).__init__()
        self.filename = filename
        self._read_data()

    def _read_data(self):
        try:
            raw_data = pd.read_csv(self.filename)
        except Exception:
            raise RuntimeError(f'Fail to load data with filename: {self.filename}')
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.timestamp = pd.to_datetime(raw_data[raw_data.columns[0]].values)


class ThriftDataSource(DataSource):
    def __init__(self, query_expressions: list = None, query_filter: str = None):
        super(DataSource, self).__init__()
        self.query_expressions = query_expressions
        self.query_filter = query_filter
        self._read_data()

    def _read_data(self):
        try:
            data_client = client_manager.borrow_data_node_client()
        except Exception:  # is this exception catch needed???
            raise RuntimeError('Fail to establish connection with DataNode')

        try:
            res = data_client.fetch_timeseries(
                queryExpressions=self.query_expressions,
                queryFilter=self.query_filter,
            )
        except Exception:
            raise RuntimeError(f'Fail to fetch data with query expressions: {self.query_expressions}'
                               f' and query filter: {self.query_filter}')

        if len(res.tsDataset) == 0:
            raise RuntimeError(f'No data fetched with query filter: {self.query_filter}')

        raw_data = serde.convert_to_df(res.columnNameList,
                                       res.columnTypeList,
                                       res.columnNameIndexMap,
                                       res.tsDataset)
        if raw_data.empty:
            raise RuntimeError(f'Fetched empty data with query expressions: '
                               f'{self.query_expressions} and query filter: {self.query_filter}')
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.timestamp = pd.to_datetime(raw_data[raw_data.columns[0]].values, unit='ms', utc=True) \
            .tz_convert('Asia/Shanghai')  # for iotdb
