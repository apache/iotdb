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
1import os
1from abc import abstractmethod
1from typing import Callable, Dict, List
1
1import numpy as np
1from huggingface_hub import hf_hub_download
1from sklearn.preprocessing import MinMaxScaler
1from sktime.detection.hmm_learn import GMMHMM, GaussianHMM
1from sktime.detection.stray import STRAY
1from sktime.forecasting.arima import ARIMA
1from sktime.forecasting.exp_smoothing import ExponentialSmoothing
1from sktime.forecasting.naive import NaiveForecaster
1from sktime.forecasting.trend import STLForecaster
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import (
1    MODEL_CONFIG_FILE_IN_JSON,
1    MODEL_WEIGHTS_FILE_IN_SAFETENSORS,
1    AttributeName,
1)
1from iotdb.ainode.core.exception import (
1    BuiltInModelNotSupportError,
1    InferenceModelInternalError,
1    ListRangeException,
1    NumericalRangeException,
1    StringRangeException,
1    WrongAttributeTypeError,
1)
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.model.model_enums import BuiltInModelType
1from iotdb.ainode.core.model.model_info import TIMER_REPO_ID
1from iotdb.ainode.core.model.sundial import modeling_sundial
1from iotdb.ainode.core.model.timerxl import modeling_timer
1
1logger = Logger()
1
1
1def _download_file_from_hf_if_necessary(local_dir: str, repo_id: str) -> bool:
1    weights_path = os.path.join(local_dir, MODEL_WEIGHTS_FILE_IN_SAFETENSORS)
1    config_path = os.path.join(local_dir, MODEL_CONFIG_FILE_IN_JSON)
1    if not os.path.exists(weights_path):
1        logger.info(
1            f"Model weights file not found at {weights_path}, downloading from HuggingFace..."
1        )
1        try:
1            hf_hub_download(
1                repo_id=repo_id,
1                filename=MODEL_WEIGHTS_FILE_IN_SAFETENSORS,
1                local_dir=local_dir,
1            )
1            logger.info(f"Got file to {weights_path}")
1        except Exception as e:
1            logger.error(
1                f"Failed to download model weights file to {local_dir} due to {e}"
1            )
1            return False
1    if not os.path.exists(config_path):
1        logger.info(
1            f"Model config file not found at {config_path}, downloading from HuggingFace..."
1        )
1        try:
1            hf_hub_download(
1                repo_id=repo_id,
1                filename=MODEL_CONFIG_FILE_IN_JSON,
1                local_dir=local_dir,
1            )
1            logger.info(f"Got file to {config_path}")
1        except Exception as e:
1            logger.error(
1                f"Failed to download model config file to {local_dir} due to {e}"
1            )
1            return False
1    return True
1
1
1def download_built_in_ltsm_from_hf_if_necessary(
1    model_type: BuiltInModelType, local_dir: str
1) -> bool:
1    """
1    Download the built-in ltsm from HuggingFace repository when necessary.
1
1    Return:
1        bool: True if the model is existed or downloaded successfully, False otherwise.
1    """
1    repo_id = TIMER_REPO_ID[model_type]
1    if not _download_file_from_hf_if_necessary(local_dir, repo_id):
1        return False
1    return True
1
1
1def get_model_attributes(model_type: BuiltInModelType):
1    if model_type == BuiltInModelType.ARIMA:
1        attribute_map = arima_attribute_map
1    elif model_type == BuiltInModelType.NAIVE_FORECASTER:
1        attribute_map = naive_forecaster_attribute_map
1    elif (
1        model_type == BuiltInModelType.EXPONENTIAL_SMOOTHING
1        or model_type == BuiltInModelType.HOLTWINTERS
1    ):
1        attribute_map = exponential_smoothing_attribute_map
1    elif model_type == BuiltInModelType.STL_FORECASTER:
1        attribute_map = stl_forecaster_attribute_map
1    elif model_type == BuiltInModelType.GMM_HMM:
1        attribute_map = gmmhmm_attribute_map
1    elif model_type == BuiltInModelType.GAUSSIAN_HMM:
1        attribute_map = gaussian_hmm_attribute_map
1    elif model_type == BuiltInModelType.STRAY:
1        attribute_map = stray_attribute_map
1    elif model_type == BuiltInModelType.TIMER_XL:
1        attribute_map = timerxl_attribute_map
1    elif model_type == BuiltInModelType.SUNDIAL:
1        attribute_map = sundial_attribute_map
1    else:
1        raise BuiltInModelNotSupportError(model_type.value)
1    return attribute_map
1
1
1def fetch_built_in_model(
1    model_type: BuiltInModelType, model_dir, inference_attrs: Dict[str, str]
1) -> Callable:
1    """
1    Fetch the built-in model according to its id and directory, not that this directory only contains model weights and config.
1    Args:
1        model_type: the type of the built-in model
1        model_dir: for huggingface models only, the directory where the model is stored
1    Returns:
1        model: the built-in model
1    """
1    default_attributes = get_model_attributes(model_type)
1    # parse the attributes from inference_attrs
1    attributes = parse_attribute(inference_attrs, default_attributes)
1
1    # build the built-in model
1    if model_type == BuiltInModelType.ARIMA:
1        model = ArimaModel(attributes)
1    elif (
1        model_type == BuiltInModelType.EXPONENTIAL_SMOOTHING
1        or model_type == BuiltInModelType.HOLTWINTERS
1    ):
1        model = ExponentialSmoothingModel(attributes)
1    elif model_type == BuiltInModelType.NAIVE_FORECASTER:
1        model = NaiveForecasterModel(attributes)
1    elif model_type == BuiltInModelType.STL_FORECASTER:
1        model = STLForecasterModel(attributes)
1    elif model_type == BuiltInModelType.GMM_HMM:
1        model = GMMHMMModel(attributes)
1    elif model_type == BuiltInModelType.GAUSSIAN_HMM:
1        model = GaussianHmmModel(attributes)
1    elif model_type == BuiltInModelType.STRAY:
1        model = STRAYModel(attributes)
1    elif model_type == BuiltInModelType.TIMER_XL:
1        model = modeling_timer.TimerForPrediction.from_pretrained(model_dir)
1    elif model_type == BuiltInModelType.SUNDIAL:
1        model = modeling_sundial.SundialForPrediction.from_pretrained(model_dir)
1    else:
1        raise BuiltInModelNotSupportError(model_type.value)
1
1    return model
1
1
1class Attribute(object):
1    def __init__(self, name: str):
1        """
1        Args:
1            name: the name of the attribute
1        """
1        self._name = name
1
1    @abstractmethod
1    def get_default_value(self):
1        raise NotImplementedError
1
1    @abstractmethod
1    def validate_value(self, value):
1        raise NotImplementedError
1
1    @abstractmethod
1    def parse(self, string_value: str):
1        raise NotImplementedError
1
1
1class IntAttribute(Attribute):
1    def __init__(
1        self,
1        name: str,
1        default_value: int,
1        default_low: int,
1        default_high: int,
1    ):
1        super(IntAttribute, self).__init__(name)
1        self.__default_value = default_value
1        self.__default_low = default_low
1        self.__default_high = default_high
1
1    def get_default_value(self):
1        return self.__default_value
1
1    def validate_value(self, value):
1        if self.__default_low <= value <= self.__default_high:
1            return True
1        raise NumericalRangeException(
1            self._name, value, self.__default_low, self.__default_high
1        )
1
1    def parse(self, string_value: str):
1        try:
1            int_value = int(string_value)
1        except:
1            raise WrongAttributeTypeError(self._name, "int")
1        return int_value
1
1
1class FloatAttribute(Attribute):
1    def __init__(
1        self,
1        name: str,
1        default_value: float,
1        default_low: float,
1        default_high: float,
1    ):
1        super(FloatAttribute, self).__init__(name)
1        self.__default_value = default_value
1        self.__default_low = default_low
1        self.__default_high = default_high
1
1    def get_default_value(self):
1        return self.__default_value
1
1    def validate_value(self, value):
1        if self.__default_low <= value <= self.__default_high:
1            return True
1        raise NumericalRangeException(
1            self._name, value, self.__default_low, self.__default_high
1        )
1
1    def parse(self, string_value: str):
1        try:
1            float_value = float(string_value)
1        except:
1            raise WrongAttributeTypeError(self._name, "float")
1        return float_value
1
1
1class StringAttribute(Attribute):
1    def __init__(self, name: str, default_value: str, value_choices: List[str]):
1        super(StringAttribute, self).__init__(name)
1        self.__default_value = default_value
1        self.__value_choices = value_choices
1
1    def get_default_value(self):
1        return self.__default_value
1
1    def validate_value(self, value):
1        if value in self.__value_choices:
1            return True
1        raise StringRangeException(self._name, value, self.__value_choices)
1
1    def parse(self, string_value: str):
1        return string_value
1
1
1class BooleanAttribute(Attribute):
1    def __init__(self, name: str, default_value: bool):
1        super(BooleanAttribute, self).__init__(name)
1        self.__default_value = default_value
1
1    def get_default_value(self):
1        return self.__default_value
1
1    def validate_value(self, value):
1        if isinstance(value, bool):
1            return True
1        raise WrongAttributeTypeError(self._name, "bool")
1
1    def parse(self, string_value: str):
1        if string_value.lower() == "true":
1            return True
1        elif string_value.lower() == "false":
1            return False
1        else:
1            raise WrongAttributeTypeError(self._name, "bool")
1
1
1class ListAttribute(Attribute):
1    def __init__(self, name: str, default_value: List, value_type):
1        """
1        value_type is the type of the elements in the list, e.g. int, float, str
1        """
1        super(ListAttribute, self).__init__(name)
1        self.__default_value = default_value
1        self.__value_type = value_type
1        self.__type_to_str = {str: "str", int: "int", float: "float"}
1
1    def get_default_value(self):
1        return self.__default_value
1
1    def validate_value(self, value):
1        if not isinstance(value, list):
1            raise WrongAttributeTypeError(self._name, "list")
1        for value_item in value:
1            if not isinstance(value_item, self.__value_type):
1                raise WrongAttributeTypeError(self._name, self.__value_type)
1        return True
1
1    def parse(self, string_value: str):
1        try:
1            list_value = eval(string_value)
1        except:
1            raise WrongAttributeTypeError(self._name, "list")
1        if not isinstance(list_value, list):
1            raise WrongAttributeTypeError(self._name, "list")
1        for i in range(len(list_value)):
1            try:
1                list_value[i] = self.__value_type(list_value[i])
1            except:
1                raise ListRangeException(
1                    self._name, list_value, self.__type_to_str[self.__value_type]
1                )
1        return list_value
1
1
1class TupleAttribute(Attribute):
1    def __init__(self, name: str, default_value: tuple, value_type):
1        """
1        value_type is the type of the elements in the list, e.g. int, float, str
1        """
1        super(TupleAttribute, self).__init__(name)
1        self.__default_value = default_value
1        self.__value_type = value_type
1        self.__type_to_str = {str: "str", int: "int", float: "float"}
1
1    def get_default_value(self):
1        return self.__default_value
1
1    def validate_value(self, value):
1        if not isinstance(value, tuple):
1            raise WrongAttributeTypeError(self._name, "tuple")
1        for value_item in value:
1            if not isinstance(value_item, self.__value_type):
1                raise WrongAttributeTypeError(self._name, self.__value_type)
1        return True
1
1    def parse(self, string_value: str):
1        try:
1            tuple_value = eval(string_value)
1        except:
1            raise WrongAttributeTypeError(self._name, "tuple")
1        if not isinstance(tuple_value, tuple):
1            raise WrongAttributeTypeError(self._name, "tuple")
1        list_value = list(tuple_value)
1        for i in range(len(list_value)):
1            try:
1                list_value[i] = self.__value_type(list_value[i])
1            except:
1                raise ListRangeException(
1                    self._name, list_value, self.__type_to_str[self.__value_type]
1                )
1        tuple_value = tuple(list_value)
1        return tuple_value
1
1
1def parse_attribute(
1    input_attributes: Dict[str, str], attribute_map: Dict[str, Attribute]
1):
1    """
1    Args:
1        input_attributes: a dict of attributes, where the key is the attribute name, the value is the string value of
1            the attribute
1        attribute_map: a dict of hyperparameters, where the key is the attribute name, the value is the Attribute
1            object
1    Returns:
1        a dict of attributes, where the key is the attribute name, the value is the parsed value of the attribute
1    """
1    attributes = {}
1    for attribute_name in attribute_map:
1        # user specified the attribute
1        if attribute_name in input_attributes:
1            attribute = attribute_map[attribute_name]
1            value = attribute.parse(input_attributes[attribute_name])
1            attribute.validate_value(value)
1            attributes[attribute_name] = value
1        # user did not specify the attribute, use the default value
1        else:
1            try:
1                attributes[attribute_name] = attribute_map[
1                    attribute_name
1                ].get_default_value()
1            except NotImplementedError as e:
1                logger.error(f"attribute {attribute_name} is not implemented.")
1                raise e
1    return attributes
1
1
1sundial_attribute_map = {
1    AttributeName.INPUT_TOKEN_LEN.value: IntAttribute(
1        name=AttributeName.INPUT_TOKEN_LEN.value,
1        default_value=16,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.HIDDEN_SIZE.value: IntAttribute(
1        name=AttributeName.HIDDEN_SIZE.value,
1        default_value=768,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.INTERMEDIATE_SIZE.value: IntAttribute(
1        name=AttributeName.INTERMEDIATE_SIZE.value,
1        default_value=3072,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.OUTPUT_TOKEN_LENS.value: ListAttribute(
1        name=AttributeName.OUTPUT_TOKEN_LENS.value, default_value=[720], value_type=int
1    ),
1    AttributeName.NUM_HIDDEN_LAYERS.value: IntAttribute(
1        name=AttributeName.NUM_HIDDEN_LAYERS.value,
1        default_value=12,
1        default_low=1,
1        default_high=16,
1    ),
1    AttributeName.NUM_ATTENTION_HEADS.value: IntAttribute(
1        name=AttributeName.NUM_ATTENTION_HEADS.value,
1        default_value=12,
1        default_low=1,
1        default_high=192,
1    ),
1    AttributeName.HIDDEN_ACT.value: StringAttribute(
1        name=AttributeName.HIDDEN_ACT.value,
1        default_value="silu",
1        value_choices=["relu", "gelu", "silu", "tanh"],
1    ),
1    AttributeName.USE_CACHE.value: BooleanAttribute(
1        name=AttributeName.USE_CACHE.value,
1        default_value=True,
1    ),
1    AttributeName.ROPE_THETA.value: IntAttribute(
1        name=AttributeName.ROPE_THETA.value,
1        default_value=10000,
1        default_low=1000,
1        default_high=50000,
1    ),
1    AttributeName.DROPOUT_RATE.value: FloatAttribute(
1        name=AttributeName.DROPOUT_RATE.value,
1        default_value=0.1,
1        default_low=0.0,
1        default_high=1.0,
1    ),
1    AttributeName.INITIALIZER_RANGE.value: FloatAttribute(
1        name=AttributeName.INITIALIZER_RANGE.value,
1        default_value=0.02,
1        default_low=0.0,
1        default_high=1.0,
1    ),
1    AttributeName.MAX_POSITION_EMBEDDINGS.value: IntAttribute(
1        name=AttributeName.MAX_POSITION_EMBEDDINGS.value,
1        default_value=10000,
1        default_low=1,
1        default_high=50000,
1    ),
1    AttributeName.FLOW_LOSS_DEPTH.value: IntAttribute(
1        name=AttributeName.FLOW_LOSS_DEPTH.value,
1        default_value=3,
1        default_low=1,
1        default_high=50,
1    ),
1    AttributeName.NUM_SAMPLING_STEPS.value: IntAttribute(
1        name=AttributeName.NUM_SAMPLING_STEPS.value,
1        default_value=50,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.DIFFUSION_BATCH_MUL.value: IntAttribute(
1        name=AttributeName.DIFFUSION_BATCH_MUL.value,
1        default_value=4,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.CKPT_PATH.value: StringAttribute(
1        name=AttributeName.CKPT_PATH.value,
1        default_value=os.path.join(
1            os.getcwd(),
1            AINodeDescriptor().get_config().get_ain_models_dir(),
1            "weights",
1            "sundial",
1        ),
1        value_choices=[""],
1    ),
1}
1
1timerxl_attribute_map = {
1    AttributeName.INPUT_TOKEN_LEN.value: IntAttribute(
1        name=AttributeName.INPUT_TOKEN_LEN.value,
1        default_value=96,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.HIDDEN_SIZE.value: IntAttribute(
1        name=AttributeName.HIDDEN_SIZE.value,
1        default_value=1024,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.INTERMEDIATE_SIZE.value: IntAttribute(
1        name=AttributeName.INTERMEDIATE_SIZE.value,
1        default_value=2048,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.OUTPUT_TOKEN_LENS.value: ListAttribute(
1        name=AttributeName.OUTPUT_TOKEN_LENS.value, default_value=[96], value_type=int
1    ),
1    AttributeName.NUM_HIDDEN_LAYERS.value: IntAttribute(
1        name=AttributeName.NUM_HIDDEN_LAYERS.value,
1        default_value=8,
1        default_low=1,
1        default_high=16,
1    ),
1    AttributeName.NUM_ATTENTION_HEADS.value: IntAttribute(
1        name=AttributeName.NUM_ATTENTION_HEADS.value,
1        default_value=8,
1        default_low=1,
1        default_high=192,
1    ),
1    AttributeName.HIDDEN_ACT.value: StringAttribute(
1        name=AttributeName.HIDDEN_ACT.value,
1        default_value="silu",
1        value_choices=["relu", "gelu", "silu", "tanh"],
1    ),
1    AttributeName.USE_CACHE.value: BooleanAttribute(
1        name=AttributeName.USE_CACHE.value,
1        default_value=True,
1    ),
1    AttributeName.ROPE_THETA.value: IntAttribute(
1        name=AttributeName.ROPE_THETA.value,
1        default_value=10000,
1        default_low=1000,
1        default_high=50000,
1    ),
1    AttributeName.ATTENTION_DROPOUT.value: FloatAttribute(
1        name=AttributeName.ATTENTION_DROPOUT.value,
1        default_value=0.0,
1        default_low=0.0,
1        default_high=1.0,
1    ),
1    AttributeName.INITIALIZER_RANGE.value: FloatAttribute(
1        name=AttributeName.INITIALIZER_RANGE.value,
1        default_value=0.02,
1        default_low=0.0,
1        default_high=1.0,
1    ),
1    AttributeName.MAX_POSITION_EMBEDDINGS.value: IntAttribute(
1        name=AttributeName.MAX_POSITION_EMBEDDINGS.value,
1        default_value=10000,
1        default_low=1,
1        default_high=50000,
1    ),
1    AttributeName.CKPT_PATH.value: StringAttribute(
1        name=AttributeName.CKPT_PATH.value,
1        default_value=os.path.join(
1            os.getcwd(),
1            AINodeDescriptor().get_config().get_ain_models_dir(),
1            "weights",
1            "timerxl",
1            "model.safetensors",
1        ),
1        value_choices=[""],
1    ),
1}
1
1# built-in sktime model attributes
1# NaiveForecaster
1naive_forecaster_attribute_map = {
1    AttributeName.PREDICT_LENGTH.value: IntAttribute(
1        name=AttributeName.PREDICT_LENGTH.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.STRATEGY.value: StringAttribute(
1        name=AttributeName.STRATEGY.value,
1        default_value="last",
1        value_choices=["last", "mean"],
1    ),
1    AttributeName.SP.value: IntAttribute(
1        name=AttributeName.SP.value, default_value=1, default_low=1, default_high=5000
1    ),
1}
1# ExponentialSmoothing
1exponential_smoothing_attribute_map = {
1    AttributeName.PREDICT_LENGTH.value: IntAttribute(
1        name=AttributeName.PREDICT_LENGTH.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.DAMPED_TREND.value: BooleanAttribute(
1        name=AttributeName.DAMPED_TREND.value,
1        default_value=False,
1    ),
1    AttributeName.INITIALIZATION_METHOD.value: StringAttribute(
1        name=AttributeName.INITIALIZATION_METHOD.value,
1        default_value="estimated",
1        value_choices=["estimated", "heuristic", "legacy-heuristic", "known"],
1    ),
1    AttributeName.OPTIMIZED.value: BooleanAttribute(
1        name=AttributeName.OPTIMIZED.value,
1        default_value=True,
1    ),
1    AttributeName.REMOVE_BIAS.value: BooleanAttribute(
1        name=AttributeName.REMOVE_BIAS.value,
1        default_value=False,
1    ),
1    AttributeName.USE_BRUTE.value: BooleanAttribute(
1        name=AttributeName.USE_BRUTE.value,
1        default_value=False,
1    ),
1}
1# Arima
1arima_attribute_map = {
1    AttributeName.PREDICT_LENGTH.value: IntAttribute(
1        name=AttributeName.PREDICT_LENGTH.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.ORDER.value: TupleAttribute(
1        name=AttributeName.ORDER.value, default_value=(1, 0, 0), value_type=int
1    ),
1    AttributeName.SEASONAL_ORDER.value: TupleAttribute(
1        name=AttributeName.SEASONAL_ORDER.value,
1        default_value=(0, 0, 0, 0),
1        value_type=int,
1    ),
1    AttributeName.METHOD.value: StringAttribute(
1        name=AttributeName.METHOD.value,
1        default_value="lbfgs",
1        value_choices=["lbfgs", "bfgs", "newton", "nm", "cg", "ncg", "powell"],
1    ),
1    AttributeName.MAXITER.value: IntAttribute(
1        name=AttributeName.MAXITER.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.SUPPRESS_WARNINGS.value: BooleanAttribute(
1        name=AttributeName.SUPPRESS_WARNINGS.value,
1        default_value=True,
1    ),
1    AttributeName.OUT_OF_SAMPLE_SIZE.value: IntAttribute(
1        name=AttributeName.OUT_OF_SAMPLE_SIZE.value,
1        default_value=0,
1        default_low=0,
1        default_high=5000,
1    ),
1    AttributeName.SCORING.value: StringAttribute(
1        name=AttributeName.SCORING.value,
1        default_value="mse",
1        value_choices=["mse", "mae", "rmse", "mape", "smape", "rmsle", "r2"],
1    ),
1    AttributeName.WITH_INTERCEPT.value: BooleanAttribute(
1        name=AttributeName.WITH_INTERCEPT.value,
1        default_value=True,
1    ),
1    AttributeName.TIME_VARYING_REGRESSION.value: BooleanAttribute(
1        name=AttributeName.TIME_VARYING_REGRESSION.value,
1        default_value=False,
1    ),
1    AttributeName.ENFORCE_STATIONARITY.value: BooleanAttribute(
1        name=AttributeName.ENFORCE_STATIONARITY.value,
1        default_value=True,
1    ),
1    AttributeName.ENFORCE_INVERTIBILITY.value: BooleanAttribute(
1        name=AttributeName.ENFORCE_INVERTIBILITY.value,
1        default_value=True,
1    ),
1    AttributeName.SIMPLE_DIFFERENCING.value: BooleanAttribute(
1        name=AttributeName.SIMPLE_DIFFERENCING.value,
1        default_value=False,
1    ),
1    AttributeName.MEASUREMENT_ERROR.value: BooleanAttribute(
1        name=AttributeName.MEASUREMENT_ERROR.value,
1        default_value=False,
1    ),
1    AttributeName.MLE_REGRESSION.value: BooleanAttribute(
1        name=AttributeName.MLE_REGRESSION.value,
1        default_value=True,
1    ),
1    AttributeName.HAMILTON_REPRESENTATION.value: BooleanAttribute(
1        name=AttributeName.HAMILTON_REPRESENTATION.value,
1        default_value=False,
1    ),
1    AttributeName.CONCENTRATE_SCALE.value: BooleanAttribute(
1        name=AttributeName.CONCENTRATE_SCALE.value,
1        default_value=False,
1    ),
1}
1# STLForecaster
1stl_forecaster_attribute_map = {
1    AttributeName.PREDICT_LENGTH.value: IntAttribute(
1        name=AttributeName.PREDICT_LENGTH.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.SP.value: IntAttribute(
1        name=AttributeName.SP.value, default_value=2, default_low=1, default_high=5000
1    ),
1    AttributeName.SEASONAL.value: IntAttribute(
1        name=AttributeName.SEASONAL.value,
1        default_value=7,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.SEASONAL_DEG.value: IntAttribute(
1        name=AttributeName.SEASONAL_DEG.value,
1        default_value=1,
1        default_low=0,
1        default_high=5000,
1    ),
1    AttributeName.TREND_DEG.value: IntAttribute(
1        name=AttributeName.TREND_DEG.value,
1        default_value=1,
1        default_low=0,
1        default_high=5000,
1    ),
1    AttributeName.LOW_PASS_DEG.value: IntAttribute(
1        name=AttributeName.LOW_PASS_DEG.value,
1        default_value=1,
1        default_low=0,
1        default_high=5000,
1    ),
1    AttributeName.SEASONAL_JUMP.value: IntAttribute(
1        name=AttributeName.SEASONAL_JUMP.value,
1        default_value=1,
1        default_low=0,
1        default_high=5000,
1    ),
1    AttributeName.TREND_JUMP.value: IntAttribute(
1        name=AttributeName.TREND_JUMP.value,
1        default_value=1,
1        default_low=0,
1        default_high=5000,
1    ),
1    AttributeName.LOSS_PASS_JUMP.value: IntAttribute(
1        name=AttributeName.LOSS_PASS_JUMP.value,
1        default_value=1,
1        default_low=0,
1        default_high=5000,
1    ),
1}
1
1# GAUSSIAN_HMM
1gaussian_hmm_attribute_map = {
1    AttributeName.N_COMPONENTS.value: IntAttribute(
1        name=AttributeName.N_COMPONENTS.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.COVARIANCE_TYPE.value: StringAttribute(
1        name=AttributeName.COVARIANCE_TYPE.value,
1        default_value="diag",
1        value_choices=["spherical", "diag", "full", "tied"],
1    ),
1    AttributeName.MIN_COVAR.value: FloatAttribute(
1        name=AttributeName.MIN_COVAR.value,
1        default_value=1e-3,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.STARTPROB_PRIOR.value: FloatAttribute(
1        name=AttributeName.STARTPROB_PRIOR.value,
1        default_value=1.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.TRANSMAT_PRIOR.value: FloatAttribute(
1        name=AttributeName.TRANSMAT_PRIOR.value,
1        default_value=1.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.MEANS_PRIOR.value: FloatAttribute(
1        name=AttributeName.MEANS_PRIOR.value,
1        default_value=0.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.MEANS_WEIGHT.value: FloatAttribute(
1        name=AttributeName.MEANS_WEIGHT.value,
1        default_value=0.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.COVARS_PRIOR.value: FloatAttribute(
1        name=AttributeName.COVARS_PRIOR.value,
1        default_value=1e-2,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.COVARS_WEIGHT.value: FloatAttribute(
1        name=AttributeName.COVARS_WEIGHT.value,
1        default_value=1.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.ALGORITHM.value: StringAttribute(
1        name=AttributeName.ALGORITHM.value,
1        default_value="viterbi",
1        value_choices=["viterbi", "map"],
1    ),
1    AttributeName.N_ITER.value: IntAttribute(
1        name=AttributeName.N_ITER.value,
1        default_value=10,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.TOL.value: FloatAttribute(
1        name=AttributeName.TOL.value,
1        default_value=1e-2,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.PARAMS.value: StringAttribute(
1        name=AttributeName.PARAMS.value,
1        default_value="stmc",
1        value_choices=["stmc", "stm"],
1    ),
1    AttributeName.INIT_PARAMS.value: StringAttribute(
1        name=AttributeName.INIT_PARAMS.value,
1        default_value="stmc",
1        value_choices=["stmc", "stm"],
1    ),
1    AttributeName.IMPLEMENTATION.value: StringAttribute(
1        name=AttributeName.IMPLEMENTATION.value,
1        default_value="log",
1        value_choices=["log", "scaling"],
1    ),
1}
1
1# GMMHMM
1gmmhmm_attribute_map = {
1    AttributeName.N_COMPONENTS.value: IntAttribute(
1        name=AttributeName.N_COMPONENTS.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.N_MIX.value: IntAttribute(
1        name=AttributeName.N_MIX.value,
1        default_value=1,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.MIN_COVAR.value: FloatAttribute(
1        name=AttributeName.MIN_COVAR.value,
1        default_value=1e-3,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.STARTPROB_PRIOR.value: FloatAttribute(
1        name=AttributeName.STARTPROB_PRIOR.value,
1        default_value=1.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.TRANSMAT_PRIOR.value: FloatAttribute(
1        name=AttributeName.TRANSMAT_PRIOR.value,
1        default_value=1.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.WEIGHTS_PRIOR.value: FloatAttribute(
1        name=AttributeName.WEIGHTS_PRIOR.value,
1        default_value=1.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.MEANS_PRIOR.value: FloatAttribute(
1        name=AttributeName.MEANS_PRIOR.value,
1        default_value=0.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.MEANS_WEIGHT.value: FloatAttribute(
1        name=AttributeName.MEANS_WEIGHT.value,
1        default_value=0.0,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.ALGORITHM.value: StringAttribute(
1        name=AttributeName.ALGORITHM.value,
1        default_value="viterbi",
1        value_choices=["viterbi", "map"],
1    ),
1    AttributeName.COVARIANCE_TYPE.value: StringAttribute(
1        name=AttributeName.COVARIANCE_TYPE.value,
1        default_value="diag",
1        value_choices=["sperical", "diag", "full", "tied"],
1    ),
1    AttributeName.N_ITER.value: IntAttribute(
1        name=AttributeName.N_ITER.value,
1        default_value=10,
1        default_low=1,
1        default_high=5000,
1    ),
1    AttributeName.TOL.value: FloatAttribute(
1        name=AttributeName.TOL.value,
1        default_value=1e-2,
1        default_low=-1e10,
1        default_high=1e10,
1    ),
1    AttributeName.INIT_PARAMS.value: StringAttribute(
1        name=AttributeName.INIT_PARAMS.value,
1        default_value="stmcw",
1        value_choices=[
1            "s",
1            "t",
1            "m",
1            "c",
1            "w",
1            "st",
1            "sm",
1            "sc",
1            "sw",
1            "tm",
1            "tc",
1            "tw",
1            "mc",
1            "mw",
1            "cw",
1            "stm",
1            "stc",
1            "stw",
1            "smc",
1            "smw",
1            "scw",
1            "tmc",
1            "tmw",
1            "tcw",
1            "mcw",
1            "stmc",
1            "stmw",
1            "stcw",
1            "smcw",
1            "tmcw",
1            "stmcw",
1        ],
1    ),
1    AttributeName.PARAMS.value: StringAttribute(
1        name=AttributeName.PARAMS.value,
1        default_value="stmcw",
1        value_choices=[
1            "s",
1            "t",
1            "m",
1            "c",
1            "w",
1            "st",
1            "sm",
1            "sc",
1            "sw",
1            "tm",
1            "tc",
1            "tw",
1            "mc",
1            "mw",
1            "cw",
1            "stm",
1            "stc",
1            "stw",
1            "smc",
1            "smw",
1            "scw",
1            "tmc",
1            "tmw",
1            "tcw",
1            "mcw",
1            "stmc",
1            "stmw",
1            "stcw",
1            "smcw",
            "tmcw",
            "stmcw",
        ],
    ),
    AttributeName.IMPLEMENTATION.value: StringAttribute(
        name=AttributeName.IMPLEMENTATION.value,
        default_value="log",
        value_choices=["log", "scaling"],
    ),
}

# STRAY
stray_attribute_map = {
    AttributeName.ALPHA.value: FloatAttribute(
        name=AttributeName.ALPHA.value,
        default_value=0.01,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.K.value: IntAttribute(
        name=AttributeName.K.value, default_value=10, default_low=1, default_high=5000
    ),
    AttributeName.KNN_ALGORITHM.value: StringAttribute(
        name=AttributeName.KNN_ALGORITHM.value,
        default_value="brute",
        value_choices=["brute", "kd_tree", "ball_tree", "auto"],
    ),
    AttributeName.P.value: FloatAttribute(
        name=AttributeName.P.value,
        default_value=0.5,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.SIZE_THRESHOLD.value: IntAttribute(
        name=AttributeName.SIZE_THRESHOLD.value,
        default_value=50,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.OUTLIER_TAIL.value: StringAttribute(
        name=AttributeName.OUTLIER_TAIL.value,
        default_value="max",
        value_choices=["min", "max"],
    ),
}


class BuiltInModel(object):
    def __init__(self, attributes):
        self._attributes = attributes
        self._model = None

    @abstractmethod
    def inference(self, data):
        raise NotImplementedError


class ArimaModel(BuiltInModel):
    def __init__(self, attributes):
        super(ArimaModel, self).__init__(attributes)
        self._model = ARIMA(
            order=attributes["order"],
            seasonal_order=attributes["seasonal_order"],
            method=attributes["method"],
            suppress_warnings=attributes["suppress_warnings"],
            maxiter=attributes["maxiter"],
            out_of_sample_size=attributes["out_of_sample_size"],
            scoring=attributes["scoring"],
            with_intercept=attributes["with_intercept"],
            time_varying_regression=attributes["time_varying_regression"],
            enforce_stationarity=attributes["enforce_stationarity"],
            enforce_invertibility=attributes["enforce_invertibility"],
            simple_differencing=attributes["simple_differencing"],
            measurement_error=attributes["measurement_error"],
            mle_regression=attributes["mle_regression"],
            hamilton_representation=attributes["hamilton_representation"],
            concentrate_scale=attributes["concentrate_scale"],
        )

    def inference(self, data):
        try:
            predict_length = self._attributes["predict_length"]
            self._model.fit(data)
            output = self._model.predict(fh=range(predict_length))
            output = np.array(output, dtype=np.float64)
            return output
        except Exception as e:
            raise InferenceModelInternalError(str(e))


class ExponentialSmoothingModel(BuiltInModel):
    def __init__(self, attributes):
        super(ExponentialSmoothingModel, self).__init__(attributes)
        self._model = ExponentialSmoothing(
            damped_trend=attributes["damped_trend"],
            initialization_method=attributes["initialization_method"],
            optimized=attributes["optimized"],
            remove_bias=attributes["remove_bias"],
            use_brute=attributes["use_brute"],
        )

    def inference(self, data):
        try:
            predict_length = self._attributes["predict_length"]
            self._model.fit(data)
            output = self._model.predict(fh=range(predict_length))
            output = np.array(output, dtype=np.float64)
            return output
        except Exception as e:
            raise InferenceModelInternalError(str(e))


class NaiveForecasterModel(BuiltInModel):
    def __init__(self, attributes):
        super(NaiveForecasterModel, self).__init__(attributes)
        self._model = NaiveForecaster(
            strategy=attributes["strategy"], sp=attributes["sp"]
        )

    def inference(self, data):
        try:
            predict_length = self._attributes["predict_length"]
            self._model.fit(data)
            output = self._model.predict(fh=range(predict_length))
            output = np.array(output, dtype=np.float64)
            return output
        except Exception as e:
            raise InferenceModelInternalError(str(e))


class STLForecasterModel(BuiltInModel):
    def __init__(self, attributes):
        super(STLForecasterModel, self).__init__(attributes)
        self._model = STLForecaster(
            sp=attributes["sp"],
            seasonal=attributes["seasonal"],
            seasonal_deg=attributes["seasonal_deg"],
            trend_deg=attributes["trend_deg"],
            low_pass_deg=attributes["low_pass_deg"],
            seasonal_jump=attributes["seasonal_jump"],
            trend_jump=attributes["trend_jump"],
            low_pass_jump=attributes["low_pass_jump"],
        )

    def inference(self, data):
        try:
            predict_length = self._attributes["predict_length"]
            self._model.fit(data)
            output = self._model.predict(fh=range(predict_length))
            output = np.array(output, dtype=np.float64)
            return output
        except Exception as e:
            raise InferenceModelInternalError(str(e))


class GMMHMMModel(BuiltInModel):
    def __init__(self, attributes):
        super(GMMHMMModel, self).__init__(attributes)
        self._model = GMMHMM(
            n_components=attributes["n_components"],
            n_mix=attributes["n_mix"],
            min_covar=attributes["min_covar"],
            startprob_prior=attributes["startprob_prior"],
            transmat_prior=attributes["transmat_prior"],
            means_prior=attributes["means_prior"],
            means_weight=attributes["means_weight"],
            weights_prior=attributes["weights_prior"],
            algorithm=attributes["algorithm"],
            covariance_type=attributes["covariance_type"],
            n_iter=attributes["n_iter"],
            tol=attributes["tol"],
            params=attributes["params"],
            init_params=attributes["init_params"],
            implementation=attributes["implementation"],
        )

    def inference(self, data):
        try:
            self._model.fit(data)
            output = self._model.predict(data)
            output = np.array(output, dtype=np.int32)
            return output
        except Exception as e:
            raise InferenceModelInternalError(str(e))


class GaussianHmmModel(BuiltInModel):
    def __init__(self, attributes):
        super(GaussianHmmModel, self).__init__(attributes)
        self._model = GaussianHMM(
            n_components=attributes["n_components"],
            covariance_type=attributes["covariance_type"],
            min_covar=attributes["min_covar"],
            startprob_prior=attributes["startprob_prior"],
            transmat_prior=attributes["transmat_prior"],
            means_prior=attributes["means_prior"],
            means_weight=attributes["means_weight"],
            covars_prior=attributes["covars_prior"],
            covars_weight=attributes["covars_weight"],
            algorithm=attributes["algorithm"],
            n_iter=attributes["n_iter"],
            tol=attributes["tol"],
            params=attributes["params"],
            init_params=attributes["init_params"],
            implementation=attributes["implementation"],
        )

    def inference(self, data):
        try:
            self._model.fit(data)
            output = self._model.predict(data)
            output = np.array(output, dtype=np.int32)
            return output
        except Exception as e:
            raise InferenceModelInternalError(str(e))


class STRAYModel(BuiltInModel):
    def __init__(self, attributes):
        super(STRAYModel, self).__init__(attributes)
        self._model = STRAY(
            alpha=attributes["alpha"],
            k=attributes["k"],
            knn_algorithm=attributes["knn_algorithm"],
            p=attributes["p"],
            size_threshold=attributes["size_threshold"],
            outlier_tail=attributes["outlier_tail"],
        )

    def inference(self, data):
        try:
            data = MinMaxScaler().fit_transform(data)
            output = self._model.fit_transform(data)
            # change the output to int
            output = np.array(output, dtype=np.int32)
            return output
        except Exception as e:
            raise InferenceModelInternalError(str(e))
