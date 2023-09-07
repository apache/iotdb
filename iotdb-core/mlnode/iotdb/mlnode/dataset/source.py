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
import pandas as pd

from iotdb.mlnode.client import client_manager


class DataSource(object):
    """
    Pre-fetched in multi-variate time series in memory

    Methods:
        get_data: returns time series value (Numpy.2DArray)
        get_timestamp: returns aligned timestamp value
    """

    def __init__(self):
        self.data = None
        self.timestamp = None
        self._read_data()

    def _read_data(self):
        raise NotImplementedError

    def get_data(self) -> np.ndarray:
        return self.data

    def get_timestamp(self) -> np.ndarray:
        return self.timestamp


class FileDataSource(DataSource):
    def __init__(self, filename: str = None):
        self.filename = filename
        super(FileDataSource, self).__init__()

    def _read_data(self) -> None:
        try:
            raw_data = pd.read_csv(self.filename)
        except Exception:
            raise RuntimeError(f'Fail to load data with filename: {self.filename}')
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.timestamp = pd.to_datetime(raw_data[raw_data.columns[0]].values)


class IoTDBDataSource(DataSource):
    def __init__(self, query_body: str = None):
        self.query_body = query_body
        super(IoTDBDataSource, self).__init__()

    def _read_data(self) -> None:
        try:
            data_client = client_manager.borrow_data_node_client()
        except Exception:
            raise RuntimeError('Fail to establish connection with DataNode')

        raw_data = data_client.fetch_timeseries(self.query_body)

        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.timestamp = pd.to_datetime(raw_data[raw_data.columns[0]].values, unit='ms', utc=True) \
            .tz_convert('Asia/Shanghai')
