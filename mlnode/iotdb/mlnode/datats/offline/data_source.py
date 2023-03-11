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
from iotdb.mlnode.client import dataClient

class DataSource(object):
    """
    Pre-fetched in multi-variate time series in memory

    Args:
        source_type: available choice in ['file', 'sql', 'thrift']
        filename: for file type, the file location in `csv` format
        session and sql: for sql type

    Methods:
        get_data: returns self.data, the time series value (Numpy.2DArray)
        get_timestamp: returns self.timestamp, the aligned timestamp value
    """

    def __init__(self, source_type='file', filename=None, session=None, sql=None, query_expressions=None, query_filter=None, **kwargs):
        self.source_type = source_type
        self.data = None
        self.timestamp = None

        # TODO: add exception
        if self.source_type == 'file':
            self._read_file_data(filename)
        elif self.source_type == 'sql':
            self._read_sql_data(session, sql)
        elif self.source_type == 'thrift':
            self._read_thrift_data(query_expressions, query_filter)
        else:
            raise NotImplementedError('Unknown data source type (%s)' % type)

    def _read_file_data(self, filename):
        raw_data = pd.read_csv(filename)
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.timestamp = pd.to_datetime(raw_data[raw_data.columns[0]].values)

    def _read_sql_data(self, session, sql):
        result = session.execute_query_statement(sql)
        assert result, "Failed to fetch data from database (%s)" % sql
        raw_data = result.todf()
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.timestamp = pd.to_datetime(raw_data[raw_data.columns[0]].values, unit='ms', utc=True).tz_convert(
            'Asia/Shanghai')  # for iotdb

    def _read_thrift_data(self, query_expressions, query_filter):  # TODO: fetch until all
        res = dataClient.fetch_timeseries(
            queryExpressions=query_expressions,
            queryFilter=query_filter,
        )
        raw_data = serde.convert_to_df(res.columnNameList,
                                   res.columnTypeList,
                                   res.columnNameIndexMap,
                                   res.tsDataset)
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        #TODO: check the timestamp
        self.timestamp = pd.to_datetime(raw_data[raw_data.columns[0]].values, unit='ms', utc=True).tz_convert(
            'Asia/Shanghai')  # for iotdb

    def get_data(self):
        return self.data

    def get_timestamp(self):
        return self.timestamp