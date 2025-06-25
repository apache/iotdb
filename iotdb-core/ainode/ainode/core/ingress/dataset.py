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

from torch.utils.data import Dataset


class BasicDatabaseDataset(Dataset):
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port


class BasicDatabaseForecastDataset(BasicDatabaseDataset):
    def __init__(
        self,
        ip: str,
        port: int,
        seq_len: int,
        input_token_len: int,
        output_token_len: int,
    ):
        super().__init__(ip, port)
        # The number of the time series data points of the model input
        self.seq_len = seq_len
        # The number of the time series data points of each model token
        self.input_token_len = input_token_len
        # The number of the time series data points of the model output
        self.output_token_len = output_token_len
        self.token_num = self.seq_len // self.input_token_len
