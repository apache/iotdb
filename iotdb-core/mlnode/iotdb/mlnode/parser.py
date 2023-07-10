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


from abc import abstractmethod
from typing import Dict

from iotdb.mlnode.algorithm.enums import ForecastModelType
from iotdb.mlnode.constant import TaskType, OptionsKey
from iotdb.mlnode.exception import MissingOptionError, UnsupportedError, RedundantOptionError
from iotdb.mlnode.serde import convert_to_df
from iotdb.thrift.mlnode.ttypes import TForecastReq


class TaskOptions(object):
    def __init__(self, options: Dict):
        self._raw_options = options

        if OptionsKey.MODEL_TYPE not in self._raw_options:
            raise MissingOptionError(OptionsKey.MODEL_TYPE.value)

        self.model_type = getattr(ForecastModelType, self._raw_options.pop(OptionsKey.MODEL_TYPE).upper(), None)
        if not self.model_type:
            raise UnsupportedError(f"model_type {self.model_type}")

        # training with auto-tuning as default
        self.auto_tuning = self._raw_options.pop(OptionsKey.AUTO_TUNING, True)

    @abstractmethod
    def get_task_type(self) -> TaskType:
        raise NotImplementedError("Subclasses must implement the validate() method.")

    def _check_redundant_options(self) -> None:
        if len(self._raw_options):
            raise RedundantOptionError(str(self._raw_options))


class ForecastTaskOptions(TaskOptions):
    def __init__(self, options: Dict):
        super().__init__(options)
        self.input_length = self._raw_options.pop(OptionsKey.INPUT_LENGTH, 96)
        self.predict_length = self._raw_options.pop(OptionsKey.PREDICT_LENGTH, 96)
        super()._check_redundant_options()

    def get_task_type(self) -> TaskType:
        return TaskType.FORECAST


def parse_task_type(options: Dict) -> TaskType:
    if OptionsKey.TASK_TYPE not in options:
        raise MissingOptionError(OptionsKey.TASK_TYPE.name())
    task_type = getattr(TaskType, options.pop(OptionsKey.TASK_TYPE).upper(), None)
    if not task_type:
        raise UnsupportedError(f"task_type {task_type}")
    return task_type


def parse_task_options(options) -> TaskOptions:
    task_type = parse_task_type(options)
    if task_type == TaskType.FORECAST:
        return ForecastTaskOptions(options)
    else:
        raise UnsupportedError(f"task type {task_type}")


def parse_forecast_request(req: TForecastReq):
    column_name_list = req.inputColumnNameList
    column_type_list = req.inputTypeList
    ts_dataset = req.inputData
    data = convert_to_df(column_name_list, column_type_list, None, [ts_dataset])
    time_stamp, data = data[data.columns[0:1]], data[data.columns[1:]]
    full_data = (data, time_stamp)
    return req.modelPath, full_data, req.predictLength
