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
from typing import cast

from torch.utils.data import Dataset

from iotdb.mlnode.constant import TaskType
from iotdb.mlnode.das.dataset import TsDataset
from iotdb.mlnode.das.source import IoTDBDataSource
from iotdb.mlnode.exception import UnsupportedError
from iotdb.mlnode.parser import ForecastTaskOptions, TaskOptions


def create_dataset(query_body: str, task_options: TaskOptions) -> Dataset:
    """
    Create a dataset for training according to the task type.
    Currently, only ForcastTask is supported, so only create_forecast_dataset is called.
    """
    task_type = task_options.get_task_type()
    if task_type == TaskType.FORECAST:
        task_options = cast(ForecastTaskOptions, task_options)
        return create_forecast_dataset(query_body, task_options)
    else:
        raise UnsupportedError(f"task type {task_type}")


def create_forecast_dataset(query_body: str, task_options: ForecastTaskOptions) -> Dataset:
    """
    Create a dataset for training for forecasting.
    In forecasting, dataset is created from a ThriftDataSource, and then wrapped by a WindowDataset,
    where the input length and predict length are required.
    """
    datasource = IoTDBDataSource(query_body)
    dataset = TsDataset(datasource, task_options.input_length, task_options.predict_length)
    return dataset
