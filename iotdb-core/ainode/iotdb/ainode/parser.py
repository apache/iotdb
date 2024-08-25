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
import argparse
import re
from abc import abstractmethod
from typing import Dict

from iotdb.ainode.constant import OptionsKey, TaskType, ForecastModelType
from iotdb.ainode.exception import (MissingOptionError, RedundantOptionError,
                                    UnsupportedError, BadConfigValueError)
from iotdb.ainode.serde import convert_to_df, get_data_type_byte_from_str
from iotdb.thrift.ainode.ttypes import TInferenceReq, TConfigs


class TaskOptions(object):
    def __init__(self, options: Dict):
        self._raw_options = options

        if OptionsKey.MODEL_TYPE.name() not in self._raw_options:
            raise MissingOptionError(OptionsKey.MODEL_TYPE.name())
        model_name = self._raw_options.pop(OptionsKey.MODEL_TYPE.name())
        self.model_type = getattr(ForecastModelType, model_name.upper(), None)
        if not self.model_type:
            raise UnsupportedError(f"model_type {model_name}")

        # training with auto-tuning as default
        self.auto_tuning = str2bool(self._raw_options.pop(OptionsKey.AUTO_TUNING.name(), "false"))

    @abstractmethod
    def get_task_type(self) -> TaskType:
        raise NotImplementedError("Subclasses must implement the validate() method.")

    def _check_redundant_options(self) -> None:
        if len(self._raw_options):
            raise RedundantOptionError(str(self._raw_options))


class ForecastTaskOptions(TaskOptions):
    def __init__(self, options: Dict):
        super().__init__(options)
        self.input_length = self._raw_options.pop(OptionsKey.INPUT_LENGTH.name(), 96)
        self.predict_length = self._raw_options.pop(OptionsKey.PREDICT_LENGTH.name(), 96)
        self.predict_index_list = self._raw_options.pop(OptionsKey.PREDICT_INDEX_LIST.name(), None)
        self.input_type_list = self._raw_options.pop(OptionsKey.INPUT_TYPE_LIST.name(), None)
        super()._check_redundant_options()

    def get_task_type(self) -> TaskType:
        return TaskType.FORECAST


def parse_task_type(options: Dict) -> TaskType:
    if OptionsKey.TASK_TYPE.name() not in options:
        raise MissingOptionError(OptionsKey.TASK_TYPE.name())
    task_name = options.pop(OptionsKey.TASK_TYPE.name())
    task_type = getattr(TaskType, task_name.upper(), None)
    if not task_type:
        raise UnsupportedError(f"task_type {task_name}")
    return task_type


def parse_task_options(options) -> TaskOptions:
    task_type = parse_task_type(options)
    if task_type == TaskType.FORECAST:
        return ForecastTaskOptions(options)
    else:
        raise UnsupportedError(f"task type {task_type}")


def parse_inference_config(config_dict):
    """
    Args:
        config_dict: dict
            - configs: dict
                - input_shape (list<i32>): input shape of the model and needs to be two-dimensional array like [96, 2]
                - output_shape (list<i32>): output shape of the model and needs to be two-dimensional array like [96, 2]
                - input_type (list<str>): input type of the model and each element needs to be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text'], default float64
                - output_type (list<str>): output type of the model and each element needs to be in ['bool', 'int32', 'int64', 'float32', 'float64', 'text'], default float64
            - attributes: dict
    Returns:
        configs: TConfigs
        attributes: str
    """
    configs = config_dict['configs']

    # check if input_shape and output_shape are two-dimensional array
    if not (isinstance(configs['input_shape'], list) and len(configs['input_shape']) == 2):
        raise BadConfigValueError('input_shape', configs['input_shape'],
                                  'input_shape should be a two-dimensional array.')
    if not (isinstance(configs['output_shape'], list) and len(configs['output_shape']) == 2):
        raise BadConfigValueError('output_shape', configs['output_shape'],
                                  'output_shape should be a two-dimensional array.')

    # check if input_shape and output_shape are positive integer
    input_shape_is_positive_number = isinstance(configs['input_shape'][0], int) and isinstance(
        configs['input_shape'][1], int) and configs['input_shape'][0] > 0 and configs['input_shape'][1] > 0
    if not input_shape_is_positive_number:
        raise BadConfigValueError('input_shape', configs['input_shape'],
                                  'element in input_shape should be positive integer.')

    output_shape_is_positive_number = isinstance(configs['output_shape'][0], int) and isinstance(
        configs['output_shape'][1], int) and configs['output_shape'][0] > 0 and configs['output_shape'][1] > 0
    if not output_shape_is_positive_number:
        raise BadConfigValueError('output_shape', configs['output_shape'],
                                  'element in output_shape should be positive integer.')

    # check if input_type and output_type are one-dimensional array with right length
    if 'input_type' in configs and not (
            isinstance(configs['input_type'], list) and len(configs['input_type']) == configs['input_shape'][1]):
        raise BadConfigValueError('input_type', configs['input_type'],
                                  'input_type should be a one-dimensional array and length of it should be equal to input_shape[1].')

    if 'output_type' in configs and not (
            isinstance(configs['output_type'], list) and len(configs['output_type']) == configs['output_shape'][1]):
        raise BadConfigValueError('output_type', configs['output_type'],
                                  'output_type should be a one-dimensional array and length of it should be equal to output_shape[1].')

    # parse input_type and output_type to byte
    if 'input_type' in configs:
        input_type = [get_data_type_byte_from_str(x) for x in configs['input_type']]
    else:
        input_type = [get_data_type_byte_from_str('float32')] * configs['input_shape'][1]

    if 'output_type' in configs:
        output_type = [get_data_type_byte_from_str(x) for x in configs['output_type']]
    else:
        output_type = [get_data_type_byte_from_str('float32')] * configs['output_shape'][1]

    # parse attributes
    attributes = ""
    if 'attributes' in config_dict:
        attributes = str(config_dict['attributes'])

    return TConfigs(configs['input_shape'], configs['output_shape'], input_type, output_type), attributes


def parse_inference_request(req: TInferenceReq):
    binary_dataset = req.dataset
    type_list = req.typeList
    column_name_list = req.columnNameList
    column_name_index = req.columnNameIndexMap
    data = convert_to_df(column_name_list, type_list, column_name_index, [binary_dataset])
    time_stamp, data = data[data.columns[0:1]], data[data.columns[1:]]
    full_data = (data, time_stamp, type_list, column_name_list)
    inference_attributes = req.inferenceAttributes
    if inference_attributes is None:
        inference_attributes = {}

    window_params = req.windowParams
    if window_params is None:
        # set default window_step to infinity and window_interval to dataset length
        window_step = float('inf')
        window_interval = data.shape[0]
    else:
        window_step = window_params.windowStep
        window_interval = window_params.windowInterval
    return req.modelId, full_data, window_interval, window_step, inference_attributes


def str2bool(value):
    if value.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif value.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


# This is used to extract the key message in RuntimeError instead of the traceback message
def runtime_error_extractor(error_message):
    pattern = re.compile(r"RuntimeError: (.+)")
    match = pattern.search(error_message)

    if match:
        return match.group(1)
    else:
        return ""
