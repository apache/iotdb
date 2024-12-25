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
from torch import tensor

from iotdb.ainode.constant import TSStatusCode
from iotdb.ainode.exception import InvalidWindowArgumentError, InferenceModelInternalError, runtime_error_extractor
from iotdb.ainode.log import Logger
from iotdb.ainode.manager.model_manager import ModelManager
from iotdb.ainode.util.serde import convert_to_binary, convert_to_df
from iotdb.ainode.util.status import get_status
from iotdb.thrift.ainode.ttypes import TInferenceReq, TInferenceResp

logger = Logger()


class InferenceManager:
    @staticmethod
    def inference(req: TInferenceReq, model_manager: ModelManager):
        logger.info(f"start inference registered model {req.modelId}")
        try:
            model_id, full_data, window_interval, window_step, inference_attributes = _parse_inference_request(req)

            if model_id.startswith('_'):
                # built-in models
                logger.info(f"start inference built-in model {model_id}")
                # parse the inference attributes and create the built-in model
                model = _get_built_in_model(model_id, model_manager, inference_attributes)
                inference_results = _inference_with_built_in_model(
                    model, full_data)
            else:
                # user-registered models
                model = _get_model(model_id, model_manager, inference_attributes)
                inference_results = _inference_with_registered_model(
                    model, full_data, window_interval, window_step)
            for i in range(len(inference_results)):
                inference_results[i] = convert_to_binary(inference_results[i])
            return TInferenceResp(
                get_status(
                    TSStatusCode.SUCCESS_STATUS),
                inference_results)
        except Exception as e:
            logger.warning(e)
            inference_results = []
            return TInferenceResp(get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e)), inference_results)


def _process_data(full_data):
    """
    Args:
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
    Returns:
        data: a tensor with shape (1, L, C)
        data_length: the number of data points
    Description:
        the process_data module will convert the input data into a tensor with shape (1, L, C), where L is the number of
        data points, C is the number of variables, the data and time_stamp are aligned by index. The module will also
        convert the data type of each column to the corresponding type.
    """
    data, time_stamp, type_list, _ = full_data
    data_length = time_stamp.shape[0]
    data = data.fillna(0)
    for i in range(len(type_list)):
        if type_list[i] == "TEXT":
            data[data.columns[i]] = 0
        elif type_list[i] == "BOOLEAN":
            data[data.columns[i]] = data[data.columns[i]].astype("int")
    data = tensor(data.values).unsqueeze(0)
    return data, data_length


def _inference_with_registered_model(model, full_data, window_interval, window_step):
    """
    Args:
        model: the user-defined model
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
        window_interval: the length of each sliding window
        window_step: the step between two adjacent sliding windows
    Returns:
        outputs: a list of output DataFrames, where each DataFrame has shape (H', C'), where H' is the output window
            interval, C' is the number of variables in the output DataFrame
    Description:
        the inference_with_registered_model function will inference with deep learning model, which is registered in
        user register process. This module will split the input data into several sliding windows which has the same
        shape (1, H, C), where H is the window interval, and then feed each sliding window into the model to get the
        output, the output is a DataFrame with shape (H', C'), where H' is the output window interval, C' is the number
        of variables in the output DataFrame. Then the inference module will concatenate all the output DataFrames into
        a list.
    """

    dataset, dataset_length = _process_data(full_data)

    # check the validity of window_interval and window_step, the two arguments must be positive integers, and the
    # window_interval should not be larger than the dataset length
    if window_interval is None or window_step is None \
            or window_interval > dataset_length \
            or window_interval <= 0 or \
            window_step <= 0:
        raise InvalidWindowArgumentError(window_interval, window_step, dataset_length)

    sliding_times = int((dataset_length - window_interval) // window_step + 1)
    outputs = []
    try:
        # split the input data into several sliding windows
        for sliding_time in range(sliding_times):
            if window_step == float('inf'):
                start_index = 0
            else:
                start_index = sliding_time * window_step
            end_index = start_index + window_interval
            # input_data: tensor, shape: (1, H, C), where H is input window interval
            input_data = dataset[:, start_index:end_index, :]
            # output: tensor, shape: (1, H', C'), where H' is the output window interval
            output = model(input_data)
            # output: DataFrame, shape: (H', C')
            output = pd.DataFrame(output.squeeze(0).detach().numpy())
            outputs.append(output)
    except Exception as e:
        error_msg = runtime_error_extractor(str(e))
        if error_msg != "":
            raise InferenceModelInternalError(error_msg)
        raise InferenceModelInternalError(str(e))

    return outputs


def _inference_with_built_in_model(model, full_data):
    """
    Args:
        model: the built-in model
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
    Returns:
        outputs: a list of output DataFrames, where each DataFrame has shape (H', C'), where H' is the output window
            interval, C' is the number of variables in the output DataFrame
    Description:
        the inference_with_built_in_model function will inference with built-in model, which does not
        require user registration. This module will parse the inference attributes and create the built-in model, then
        feed the input data into the model to get the output, the output is a DataFrame with shape (H', C'), where H'
        is the output window interval, C' is the number of variables in the output DataFrame. Then the inference module
        will concatenate all the output DataFrames into a list.
    """

    data, _, _, _ = full_data
    output = model.inference(data)
    # output: DataFrame, shape: (H', C')
    output = pd.DataFrame(output)
    outputs = [output]
    return outputs


def _get_model(model_id: str, model_manager: ModelManager, inference_attributes: {}):
    if inference_attributes is None or 'acceleration' not in inference_attributes:
        # if the acceleration is not specified, then the acceleration will be set to default value False
        acceleration = False
    else:
        # if the acceleration is specified, then the acceleration will be set to the specified value
        acceleration = (inference_attributes['acceleration'].lower() == 'true')
    return model_manager.load_model(model_id, acceleration)


def _get_built_in_model(model_id: str, model_manager: ModelManager, inference_attributes: {}):
    return model_manager.load_built_in_model(model_id, inference_attributes)


def _parse_inference_request(req: TInferenceReq):
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
