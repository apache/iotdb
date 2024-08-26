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

from iotdb.ainode.constant import BuiltInModelType
from iotdb.ainode.exception import InvalidWindowArgumentError, InferenceModelInternalError, \
    BuiltInModelNotSupportError
from iotdb.ainode.factory import create_built_in_model
from iotdb.ainode.log import logger
from iotdb.ainode.parser import runtime_error_extractor
from iotdb.ainode.storage import model_storage


def inference_with_registered_model(model_id, full_data, window_interval, window_step, inference_attributes):
    """
    Args:
        model_id: the unique id of the model
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
        window_interval: the length of each sliding window
        window_step: the step between two adjacent sliding windows
        inference_attributes: a list of attributes to be inferred. In this function, the attributes will include the
            acceleration, which indicates whether the model is accelerated by the torch. Compile
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
    logger.info(f"start inference registered model {model_id}")

    # parse the inference attributes
    acceleration = False
    if inference_attributes is None or 'acceleration' not in inference_attributes:
        # if the acceleration is not specified, then the acceleration will be set to default value False
        acceleration = False
    else:
        # if the acceleration is specified, then the acceleration will be set to the specified value
        acceleration = (inference_attributes['acceleration'].lower() == 'true')

    model = model_storage.load_model_from_id(model_id, acceleration)
    dataset, dataset_length = process_data(full_data)

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


def process_data(full_data):
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


def inference_with_built_in_model(model_id, full_data, inference_attributes):
    """
    Args:
        model_id: the unique id of the model
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
        inference_attributes: a list of attributes to be inferred, in this function, the attributes will include some
            parameters of the built-in model. Some parameters are optional, and if the parameters are not
            specified, the default value will be used.
    Returns:
        outputs: a list of output DataFrames, where each DataFrame has shape (H', C'), where H' is the output window
            interval, C' is the number of variables in the output DataFrame
    Description:
        the inference_with_built_in_model function will inference with built-in model from sktime, which does not
        require user registration. This module will parse the inference attributes and create the built-in model, then
        feed the input data into the model to get the output, the output is a DataFrame with shape (H', C'), where H'
        is the output window interval, C' is the number of variables in the output DataFrame. Then the inference module
        will concatenate all the output DataFrames into a list.
    """
    model_id = model_id.lower()
    if model_id not in BuiltInModelType.values():
        raise BuiltInModelNotSupportError(model_id)

    logger.info(f"start inference built-in model {model_id}")

    # parse the inference attributes and create the built-in model
    model, attributes = create_built_in_model(model_id, inference_attributes)

    data, _, _, _ = full_data

    output = model.inference(data)

    # output: DataFrame, shape: (H', C')
    output = pd.DataFrame(output)
    outputs = [output]
    return outputs
