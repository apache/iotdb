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
1
1from torch.utils.data import Dataset
1
1
1class BasicDatabaseDataset(Dataset):
1    def __init__(self, ip: str, port: int):
1        self.ip = ip
1        self.port = port
1
1
1class BasicDatabaseForecastDataset(BasicDatabaseDataset):
1    def __init__(
1        self,
1        ip: str,
1        port: int,
1        seq_len: int,
1        input_token_len: int,
1        output_token_len: int,
1        window_step: int,
1    ):
1        super().__init__(ip, port)
1        # The number of the time series data points of the model input
1        self.seq_len = seq_len
1        # The number of the time series data points of each model token
1        self.input_token_len = input_token_len
1        # The number of the time series data points of the model output
1        self.output_token_len = output_token_len
1        self.token_num = self.seq_len // self.input_token_len
1        self.window_step = window_step
1