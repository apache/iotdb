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


class DataSource(object):
    def __init__(self, type='file', filename=None, session=None, sql=None, **kwargs):
        self.type = type
        self.data = None
        self.data_stamp = None
        if self.type == 'file':
            self._read_file_data__(filename)
        elif self.type == 'sql':
            self._read_sql_data__(session, sql)
        elif self.type == 'thrift':
            raise NotImplementedError('Unknow data source type (%s)'% type)
        else:
            raise NotImplementedError('Unknow data source type (%s)'% type)

    def _read_file_data__(self, filename):
        raw_data = pd.read_csv(filename)
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.data_stamp = pd.to_datetime(raw_data[raw_data.columns[0]].values)

    def _read_sql_data__(self, session, sql):
        result = session.execute_query_statement(sql)
        assert result, "Failed to fetch data from database (%s)" % sql  
        raw_data = result.todf()
        cols_data = raw_data.columns[1:]
        self.data = raw_data[cols_data].values
        self.data_stamp = pd.to_datetime(raw_data[raw_data.columns[0]].values, unit='ms', utc=True).tz_convert('Asia/Shanghai') # for iotdb

    def _read_thrift_data__(self): #TODO
        pass