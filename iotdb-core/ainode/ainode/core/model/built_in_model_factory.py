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
import os
from abc import abstractmethod
from typing import Callable, Dict, List

import numpy as np
from huggingface_hub import hf_hub_download
from sklearn.preprocessing import MinMaxScaler
from sktime.annotation.hmm_learn import GMMHMM, GaussianHMM
from sktime.annotation.stray import STRAY
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.exp_smoothing import ExponentialSmoothing
from sktime.forecasting.naive import NaiveForecaster
from sktime.forecasting.trend import STLForecaster

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import (
    MODEL_CONFIG_FILE_IN_JSON,
    MODEL_WEIGHTS_FILE_IN_SAFETENSORS,
    AttributeName,
)
from ainode.core.exception import (
    BuiltInModelNotSupportError,
    InferenceModelInternalError,
    ListRangeException,
    NumericalRangeException,
    StringRangeException,
    WrongAttributeTypeError,
)
from ainode.core.log import Logger
from ainode.core.model.model_info import TIMER_REPO_ID, BuiltInModelType
from ainode.core.model.sundial import modeling_sundial
from ainode.core.model.timerxl import modeling_timer

logger = Logger()


def download_ltsm_if_necessary(model_type: BuiltInModelType, local_dir) -> bool:
    """
    Download the built-in ltsm from HuggingFace repository when necessary.

    Return:
        bool: True if the model is existed or downloaded successfully, False otherwise.
    """
    repo_id = TIMER_REPO_ID[model_type]
    weights_path = os.path.join(local_dir, MODEL_WEIGHTS_FILE_IN_SAFETENSORS)
    if not os.path.exists(weights_path):
        logger.info(
            f"Weight not found at {weights_path}, downloading from HuggingFace..."
        )
        try:
            hf_hub_download(
                repo_id=repo_id,
                filename=MODEL_WEIGHTS_FILE_IN_SAFETENSORS,
                local_dir=local_dir,
            )
            logger.info(f"Got weight to {weights_path}")
        except Exception as e:
            logger.error(
                f"Failed to download huggingface model weights to {local_dir} due to {e}"
            )
            return False
    config_path = os.path.join(local_dir, MODEL_CONFIG_FILE_IN_JSON)
    if not os.path.exists(config_path):
        logger.info(
            f"Config not found at {config_path}, downloading from HuggingFace..."
        )
        try:
            hf_hub_download(
                repo_id=repo_id,
                filename=MODEL_CONFIG_FILE_IN_JSON,
                local_dir=local_dir,
            )
            logger.info(f"Got config to {config_path}")
        except Exception as e:
            logger.error(
                f"Failed to download huggingface model config to {local_dir} due to {e}"
            )
            return False
    return True


def get_model_attributes(model_type: BuiltInModelType):
    if model_type == BuiltInModelType.ARIMA:
        attribute_map = arima_attribute_map
    elif model_type == BuiltInModelType.NAIVE_FORECASTER:
        attribute_map = naive_forecaster_attribute_map
    elif (
        model_type == BuiltInModelType.EXPONENTIAL_SMOOTHING
        or model_type == BuiltInModelType.HOLTWINTERS.value
    ):
        attribute_map = exponential_smoothing_attribute_map
    elif model_type == BuiltInModelType.STL_FORECASTER:
        attribute_map = stl_forecaster_attribute_map
    elif model_type == BuiltInModelType.GMM_HMM:
        attribute_map = gmmhmm_attribute_map
    elif model_type == BuiltInModelType.GAUSSIAN_HMM:
        attribute_map = gaussian_hmm_attribute_map
    elif model_type == BuiltInModelType.STRAY:
        attribute_map = stray_attribute_map
    elif model_type == BuiltInModelType.TIMER_XL:
        attribute_map = timerxl_attribute_map
    elif model_type == BuiltInModelType.SUNDIAL:
        attribute_map = sundial_attribute_map
    else:
        raise BuiltInModelNotSupportError(model_type.value)
    return attribute_map


def fetch_built_in_model(model_type: BuiltInModelType, model_dir) -> Callable:
    """
    Fetch the built-in model according to its id and directory, not that this directory only contains model weights and config.
    Args:
        model_type: the type of the built-in model
        model_dir: for huggingface models only, the directory where the model is stored
    Returns:
        model: the built-in model
    """
    attributes = get_model_attributes(model_type)

    # build the built-in model
    if model_type == BuiltInModelType.ARIMA:
        model = ArimaModel(attributes)
    elif (
        model_type == BuiltInModelType.EXPONENTIAL_SMOOTHING
        or model_type == BuiltInModelType.HOLTWINTERS
    ):
        model = ExponentialSmoothingModel(attributes)
    elif model_type == BuiltInModelType.NAIVE_FORECASTER:
        model = NaiveForecasterModel(attributes)
    elif model_type == BuiltInModelType.STL_FORECASTER:
        model = STLForecasterModel(attributes)
    elif model_type == BuiltInModelType.GMM_HMM:
        model = GMMHMMModel(attributes)
    elif model_type == BuiltInModelType.GAUSSIAN_HMM:
        model = GaussianHmmModel(attributes)
    elif model_type == BuiltInModelType.STRAY:
        model = STRAYModel(attributes)
    elif model_type == BuiltInModelType.TIMER_XL:
        model = modeling_timer.TimerForPrediction.from_pretrained(model_dir)
    elif model_type == BuiltInModelType.SUNDIAL:
        model = modeling_sundial.SundialForPrediction.from_pretrained(model_dir)
    else:
        raise BuiltInModelNotSupportError(model_type.value)

    return model


class Attribute(object):
    def __init__(self, name: str):
        """
        Args:
            name: the name of the attribute
        """
        self._name = name

    @abstractmethod
    def get_default_value(self):
        raise NotImplementedError

    @abstractmethod
    def validate_value(self, value):
        raise NotImplementedError

    @abstractmethod
    def parse(self, string_value: str):
        raise NotImplementedError


class IntAttribute(Attribute):
    def __init__(
        self,
        name: str,
        default_value: int,
        default_low: int,
        default_high: int,
    ):
        super(IntAttribute, self).__init__(name)
        self.__default_value = default_value
        self.__default_low = default_low
        self.__default_high = default_high

    def get_default_value(self):
        return self.__default_value

    def validate_value(self, value):
        if self.__default_low <= value <= self.__default_high:
            return True
        raise NumericalRangeException(
            self._name, value, self.__default_low, self.__default_high
        )

    def parse(self, string_value: str):
        try:
            int_value = int(string_value)
        except:
            raise WrongAttributeTypeError(self._name, "int")
        return int_value


class FloatAttribute(Attribute):
    def __init__(
        self,
        name: str,
        default_value: float,
        default_low: float,
        default_high: float,
    ):
        super(FloatAttribute, self).__init__(name)
        self.__default_value = default_value
        self.__default_low = default_low
        self.__default_high = default_high

    def get_default_value(self):
        return self.__default_value

    def validate_value(self, value):
        if self.__default_low <= value <= self.__default_high:
            return True
        raise NumericalRangeException(
            self._name, value, self.__default_low, self.__default_high
        )

    def parse(self, string_value: str):
        try:
            float_value = float(string_value)
        except:
            raise WrongAttributeTypeError(self._name, "float")
        return float_value


class StringAttribute(Attribute):
    def __init__(self, name: str, default_value: str, value_choices: List[str]):
        super(StringAttribute, self).__init__(name)
        self.__default_value = default_value
        self.__value_choices = value_choices

    def get_default_value(self):
        return self.__default_value

    def validate_value(self, value):
        if value in self.__value_choices:
            return True
        raise StringRangeException(self._name, value, self.__value_choices)

    def parse(self, string_value: str):
        return string_value


class BooleanAttribute(Attribute):
    def __init__(self, name: str, default_value: bool):
        super(BooleanAttribute, self).__init__(name)
        self.__default_value = default_value

    def get_default_value(self):
        return self.__default_value

    def validate_value(self, value):
        if isinstance(value, bool):
            return True
        raise WrongAttributeTypeError(self._name, "bool")

    def parse(self, string_value: str):
        if string_value.lower() == "true":
            return True
        elif string_value.lower() == "false":
            return False
        else:
            raise WrongAttributeTypeError(self._name, "bool")


class ListAttribute(Attribute):
    def __init__(self, name: str, default_value: List, value_type):
        """
        value_type is the type of the elements in the list, e.g. int, float, str
        """
        super(ListAttribute, self).__init__(name)
        self.__default_value = default_value
        self.__value_type = value_type
        self.__type_to_str = {str: "str", int: "int", float: "float"}

    def get_default_value(self):
        return self.__default_value

    def validate_value(self, value):
        if not isinstance(value, list):
            raise WrongAttributeTypeError(self._name, "list")
        for value_item in value:
            if not isinstance(value_item, self.__value_type):
                raise WrongAttributeTypeError(self._name, self.__value_type)
        return True

    def parse(self, string_value: str):
        try:
            list_value = eval(string_value)
        except:
            raise WrongAttributeTypeError(self._name, "list")
        if not isinstance(list_value, list):
            raise WrongAttributeTypeError(self._name, "list")
        for i in range(len(list_value)):
            try:
                list_value[i] = self.__value_type(list_value[i])
            except:
                raise ListRangeException(
                    self._name, list_value, self.__type_to_str[self.__value_type]
                )
        return list_value


class TupleAttribute(Attribute):
    def __init__(self, name: str, default_value: tuple, value_type):
        """
        value_type is the type of the elements in the list, e.g. int, float, str
        """
        super(TupleAttribute, self).__init__(name)
        self.__default_value = default_value
        self.__value_type = value_type
        self.__type_to_str = {str: "str", int: "int", float: "float"}

    def get_default_value(self):
        return self.__default_value

    def validate_value(self, value):
        if not isinstance(value, tuple):
            raise WrongAttributeTypeError(self._name, "tuple")
        for value_item in value:
            if not isinstance(value_item, self.__value_type):
                raise WrongAttributeTypeError(self._name, self.__value_type)
        return True

    def parse(self, string_value: str):
        try:
            tuple_value = eval(string_value)
        except:
            raise WrongAttributeTypeError(self._name, "tuple")
        if not isinstance(tuple_value, tuple):
            raise WrongAttributeTypeError(self._name, "tuple")
        list_value = list(tuple_value)
        for i in range(len(list_value)):
            try:
                list_value[i] = self.__value_type(list_value[i])
            except:
                raise ListRangeException(
                    self._name, list_value, self.__type_to_str[self.__value_type]
                )
        tuple_value = tuple(list_value)
        return tuple_value


def parse_attribute(
    input_attributes: Dict[str, str], attribute_map: Dict[str, Attribute]
):
    """
    Args:
        input_attributes: a dict of attributes, where the key is the attribute name, the value is the string value of
            the attribute
        attribute_map: a dict of hyperparameters, where the key is the attribute name, the value is the Attribute
            object
    Returns:
        a dict of attributes, where the key is the attribute name, the value is the parsed value of the attribute
    """
    attributes = {}
    for attribute_name in attribute_map:
        # user specified the attribute
        if attribute_name in input_attributes:
            attribute = attribute_map[attribute_name]
            value = attribute.parse(input_attributes[attribute_name])
            attribute.validate_value(value)
            attributes[attribute_name] = value
        # user did not specify the attribute, use the default value
        else:
            try:
                attributes[attribute_name] = attribute_map[
                    attribute_name
                ].get_default_value()
            except NotImplementedError as e:
                logger.error(f"attribute {attribute_name} is not implemented.")
                raise e
    return attributes


sundial_attribute_map = {
    AttributeName.INPUT_TOKEN_LEN.value: IntAttribute(
        name=AttributeName.INPUT_TOKEN_LEN.value,
        default_value=16,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.HIDDEN_SIZE.value: IntAttribute(
        name=AttributeName.HIDDEN_SIZE.value,
        default_value=768,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.INTERMEDIATE_SIZE.value: IntAttribute(
        name=AttributeName.INTERMEDIATE_SIZE.value,
        default_value=3072,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.OUTPUT_TOKEN_LENS.value: ListAttribute(
        name=AttributeName.OUTPUT_TOKEN_LENS.value, default_value=[720], value_type=int
    ),
    AttributeName.NUM_HIDDEN_LAYERS.value: IntAttribute(
        name=AttributeName.NUM_HIDDEN_LAYERS.value,
        default_value=12,
        default_low=1,
        default_high=16,
    ),
    AttributeName.NUM_ATTENTION_HEADS.value: IntAttribute(
        name=AttributeName.NUM_ATTENTION_HEADS.value,
        default_value=12,
        default_low=1,
        default_high=192,
    ),
    AttributeName.HIDDEN_ACT.value: StringAttribute(
        name=AttributeName.HIDDEN_ACT.value,
        default_value="silu",
        value_choices=["relu", "gelu", "silu", "tanh"],
    ),
    AttributeName.USE_CACHE.value: BooleanAttribute(
        name=AttributeName.USE_CACHE.value,
        default_value=True,
    ),
    AttributeName.ROPE_THETA.value: IntAttribute(
        name=AttributeName.ROPE_THETA.value,
        default_value=10000,
        default_low=1000,
        default_high=50000,
    ),
    AttributeName.DROPOUT_RATE.value: FloatAttribute(
        name=AttributeName.DROPOUT_RATE.value,
        default_value=0.1,
        default_low=0.0,
        default_high=1.0,
    ),
    AttributeName.INITIALIZER_RANGE.value: FloatAttribute(
        name=AttributeName.INITIALIZER_RANGE.value,
        default_value=0.02,
        default_low=0.0,
        default_high=1.0,
    ),
    AttributeName.MAX_POSITION_EMBEDDINGS.value: IntAttribute(
        name=AttributeName.MAX_POSITION_EMBEDDINGS.value,
        default_value=10000,
        default_low=1,
        default_high=50000,
    ),
    AttributeName.FLOW_LOSS_DEPTH.value: IntAttribute(
        name=AttributeName.FLOW_LOSS_DEPTH.value,
        default_value=3,
        default_low=1,
        default_high=50,
    ),
    AttributeName.NUM_SAMPLING_STEPS.value: IntAttribute(
        name=AttributeName.NUM_SAMPLING_STEPS.value,
        default_value=50,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.DIFFUSION_BATCH_MUL.value: IntAttribute(
        name=AttributeName.DIFFUSION_BATCH_MUL.value,
        default_value=4,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.CKPT_PATH.value: StringAttribute(
        name=AttributeName.CKPT_PATH.value,
        default_value=os.path.join(
            os.getcwd(),
            AINodeDescriptor().get_config().get_ain_models_dir(),
            "weights",
            "sundial",
        ),
        value_choices=[""],
    ),
}

timerxl_attribute_map = {
    AttributeName.INPUT_TOKEN_LEN.value: IntAttribute(
        name=AttributeName.INPUT_TOKEN_LEN.value,
        default_value=96,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.HIDDEN_SIZE.value: IntAttribute(
        name=AttributeName.HIDDEN_SIZE.value,
        default_value=1024,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.INTERMEDIATE_SIZE.value: IntAttribute(
        name=AttributeName.INTERMEDIATE_SIZE.value,
        default_value=2048,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.OUTPUT_TOKEN_LENS.value: ListAttribute(
        name=AttributeName.OUTPUT_TOKEN_LENS.value, default_value=[96], value_type=int
    ),
    AttributeName.NUM_HIDDEN_LAYERS.value: IntAttribute(
        name=AttributeName.NUM_HIDDEN_LAYERS.value,
        default_value=8,
        default_low=1,
        default_high=16,
    ),
    AttributeName.NUM_ATTENTION_HEADS.value: IntAttribute(
        name=AttributeName.NUM_ATTENTION_HEADS.value,
        default_value=8,
        default_low=1,
        default_high=192,
    ),
    AttributeName.HIDDEN_ACT.value: StringAttribute(
        name=AttributeName.HIDDEN_ACT.value,
        default_value="silu",
        value_choices=["relu", "gelu", "silu", "tanh"],
    ),
    AttributeName.USE_CACHE.value: BooleanAttribute(
        name=AttributeName.USE_CACHE.value,
        default_value=True,
    ),
    AttributeName.ROPE_THETA.value: IntAttribute(
        name=AttributeName.ROPE_THETA.value,
        default_value=10000,
        default_low=1000,
        default_high=50000,
    ),
    AttributeName.ATTENTION_DROPOUT.value: FloatAttribute(
        name=AttributeName.ATTENTION_DROPOUT.value,
        default_value=0.0,
        default_low=0.0,
        default_high=1.0,
    ),
    AttributeName.INITIALIZER_RANGE.value: FloatAttribute(
        name=AttributeName.INITIALIZER_RANGE.value,
        default_value=0.02,
        default_low=0.0,
        default_high=1.0,
    ),
    AttributeName.MAX_POSITION_EMBEDDINGS.value: IntAttribute(
        name=AttributeName.MAX_POSITION_EMBEDDINGS.value,
        default_value=10000,
        default_low=1,
        default_high=50000,
    ),
    AttributeName.CKPT_PATH.value: StringAttribute(
        name=AttributeName.CKPT_PATH.value,
        default_value=os.path.join(
            os.getcwd(),
            AINodeDescriptor().get_config().get_ain_models_dir(),
            "weights",
            "timerxl",
            "model.safetensors",
        ),
        value_choices=[""],
    ),
}

# built-in sktime model attributes
# NaiveForecaster
naive_forecaster_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.STRATEGY.value: StringAttribute(
        name=AttributeName.STRATEGY.value,
        default_value="last",
        value_choices=["last", "mean"],
    ),
    AttributeName.SP.value: IntAttribute(
        name=AttributeName.SP.value, default_value=1, default_low=1, default_high=5000
    ),
}
# ExponentialSmoothing
exponential_smoothing_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.DAMPED_TREND.value: BooleanAttribute(
        name=AttributeName.DAMPED_TREND.value,
        default_value=False,
    ),
    AttributeName.INITIALIZATION_METHOD.value: StringAttribute(
        name=AttributeName.INITIALIZATION_METHOD.value,
        default_value="estimated",
        value_choices=["estimated", "heuristic", "legacy-heuristic", "known"],
    ),
    AttributeName.OPTIMIZED.value: BooleanAttribute(
        name=AttributeName.OPTIMIZED.value,
        default_value=True,
    ),
    AttributeName.REMOVE_BIAS.value: BooleanAttribute(
        name=AttributeName.REMOVE_BIAS.value,
        default_value=False,
    ),
    AttributeName.USE_BRUTE.value: BooleanAttribute(
        name=AttributeName.USE_BRUTE.value,
        default_value=False,
    ),
}
# Arima
arima_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.ORDER.value: TupleAttribute(
        name=AttributeName.ORDER.value, default_value=(1, 0, 0), value_type=int
    ),
    AttributeName.SEASONAL_ORDER.value: TupleAttribute(
        name=AttributeName.SEASONAL_ORDER.value,
        default_value=(0, 0, 0, 0),
        value_type=int,
    ),
    AttributeName.METHOD.value: StringAttribute(
        name=AttributeName.METHOD.value,
        default_value="lbfgs",
        value_choices=["lbfgs", "bfgs", "newton", "nm", "cg", "ncg", "powell"],
    ),
    AttributeName.MAXITER.value: IntAttribute(
        name=AttributeName.MAXITER.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.SUPPRESS_WARNINGS.value: BooleanAttribute(
        name=AttributeName.SUPPRESS_WARNINGS.value,
        default_value=True,
    ),
    AttributeName.OUT_OF_SAMPLE_SIZE.value: IntAttribute(
        name=AttributeName.OUT_OF_SAMPLE_SIZE.value,
        default_value=0,
        default_low=0,
        default_high=5000,
    ),
    AttributeName.SCORING.value: StringAttribute(
        name=AttributeName.SCORING.value,
        default_value="mse",
        value_choices=["mse", "mae", "rmse", "mape", "smape", "rmsle", "r2"],
    ),
    AttributeName.WITH_INTERCEPT.value: BooleanAttribute(
        name=AttributeName.WITH_INTERCEPT.value,
        default_value=True,
    ),
    AttributeName.TIME_VARYING_REGRESSION.value: BooleanAttribute(
        name=AttributeName.TIME_VARYING_REGRESSION.value,
        default_value=False,
    ),
    AttributeName.ENFORCE_STATIONARITY.value: BooleanAttribute(
        name=AttributeName.ENFORCE_STATIONARITY.value,
        default_value=True,
    ),
    AttributeName.ENFORCE_INVERTIBILITY.value: BooleanAttribute(
        name=AttributeName.ENFORCE_INVERTIBILITY.value,
        default_value=True,
    ),
    AttributeName.SIMPLE_DIFFERENCING.value: BooleanAttribute(
        name=AttributeName.SIMPLE_DIFFERENCING.value,
        default_value=False,
    ),
    AttributeName.MEASUREMENT_ERROR.value: BooleanAttribute(
        name=AttributeName.MEASUREMENT_ERROR.value,
        default_value=False,
    ),
    AttributeName.MLE_REGRESSION.value: BooleanAttribute(
        name=AttributeName.MLE_REGRESSION.value,
        default_value=True,
    ),
    AttributeName.HAMILTON_REPRESENTATION.value: BooleanAttribute(
        name=AttributeName.HAMILTON_REPRESENTATION.value,
        default_value=False,
    ),
    AttributeName.CONCENTRATE_SCALE.value: BooleanAttribute(
        name=AttributeName.CONCENTRATE_SCALE.value,
        default_value=False,
    ),
}
# STLForecaster
stl_forecaster_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.SP.value: IntAttribute(
        name=AttributeName.SP.value, default_value=2, default_low=1, default_high=5000
    ),
    AttributeName.SEASONAL.value: IntAttribute(
        name=AttributeName.SEASONAL.value,
        default_value=7,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.SEASONAL_DEG.value: IntAttribute(
        name=AttributeName.SEASONAL_DEG.value,
        default_value=1,
        default_low=0,
        default_high=5000,
    ),
    AttributeName.TREND_DEG.value: IntAttribute(
        name=AttributeName.TREND_DEG.value,
        default_value=1,
        default_low=0,
        default_high=5000,
    ),
    AttributeName.LOW_PASS_DEG.value: IntAttribute(
        name=AttributeName.LOW_PASS_DEG.value,
        default_value=1,
        default_low=0,
        default_high=5000,
    ),
    AttributeName.SEASONAL_JUMP.value: IntAttribute(
        name=AttributeName.SEASONAL_JUMP.value,
        default_value=1,
        default_low=0,
        default_high=5000,
    ),
    AttributeName.TREND_JUMP.value: IntAttribute(
        name=AttributeName.TREND_JUMP.value,
        default_value=1,
        default_low=0,
        default_high=5000,
    ),
    AttributeName.LOSS_PASS_JUMP.value: IntAttribute(
        name=AttributeName.LOSS_PASS_JUMP.value,
        default_value=1,
        default_low=0,
        default_high=5000,
    ),
}

# GAUSSIAN_HMM
gaussian_hmm_attribute_map = {
    AttributeName.N_COMPONENTS.value: IntAttribute(
        name=AttributeName.N_COMPONENTS.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.COVARIANCE_TYPE.value: StringAttribute(
        name=AttributeName.COVARIANCE_TYPE.value,
        default_value="diag",
        value_choices=["spherical", "diag", "full", "tied"],
    ),
    AttributeName.MIN_COVAR.value: FloatAttribute(
        name=AttributeName.MIN_COVAR.value,
        default_value=1e-3,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.STARTPROB_PRIOR.value: FloatAttribute(
        name=AttributeName.STARTPROB_PRIOR.value,
        default_value=1.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.TRANSMAT_PRIOR.value: FloatAttribute(
        name=AttributeName.TRANSMAT_PRIOR.value,
        default_value=1.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.MEANS_PRIOR.value: FloatAttribute(
        name=AttributeName.MEANS_PRIOR.value,
        default_value=0.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.MEANS_WEIGHT.value: FloatAttribute(
        name=AttributeName.MEANS_WEIGHT.value,
        default_value=0.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.COVARS_PRIOR.value: FloatAttribute(
        name=AttributeName.COVARS_PRIOR.value,
        default_value=1e-2,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.COVARS_WEIGHT.value: FloatAttribute(
        name=AttributeName.COVARS_WEIGHT.value,
        default_value=1.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.ALGORITHM.value: StringAttribute(
        name=AttributeName.ALGORITHM.value,
        default_value="viterbi",
        value_choices=["viterbi", "map"],
    ),
    AttributeName.N_ITER.value: IntAttribute(
        name=AttributeName.N_ITER.value,
        default_value=10,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.TOL.value: FloatAttribute(
        name=AttributeName.TOL.value,
        default_value=1e-2,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.PARAMS.value: StringAttribute(
        name=AttributeName.PARAMS.value,
        default_value="stmc",
        value_choices=["stmc", "stm"],
    ),
    AttributeName.INIT_PARAMS.value: StringAttribute(
        name=AttributeName.INIT_PARAMS.value,
        default_value="stmc",
        value_choices=["stmc", "stm"],
    ),
    AttributeName.IMPLEMENTATION.value: StringAttribute(
        name=AttributeName.IMPLEMENTATION.value,
        default_value="log",
        value_choices=["log", "scaling"],
    ),
}

# GMMHMM
gmmhmm_attribute_map = {
    AttributeName.N_COMPONENTS.value: IntAttribute(
        name=AttributeName.N_COMPONENTS.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.N_MIX.value: IntAttribute(
        name=AttributeName.N_MIX.value,
        default_value=1,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.MIN_COVAR.value: FloatAttribute(
        name=AttributeName.MIN_COVAR.value,
        default_value=1e-3,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.STARTPROB_PRIOR.value: FloatAttribute(
        name=AttributeName.STARTPROB_PRIOR.value,
        default_value=1.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.TRANSMAT_PRIOR.value: FloatAttribute(
        name=AttributeName.TRANSMAT_PRIOR.value,
        default_value=1.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.WEIGHTS_PRIOR.value: FloatAttribute(
        name=AttributeName.WEIGHTS_PRIOR.value,
        default_value=1.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.MEANS_PRIOR.value: FloatAttribute(
        name=AttributeName.MEANS_PRIOR.value,
        default_value=0.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.MEANS_WEIGHT.value: FloatAttribute(
        name=AttributeName.MEANS_WEIGHT.value,
        default_value=0.0,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.ALGORITHM.value: StringAttribute(
        name=AttributeName.ALGORITHM.value,
        default_value="viterbi",
        value_choices=["viterbi", "map"],
    ),
    AttributeName.COVARIANCE_TYPE.value: StringAttribute(
        name=AttributeName.COVARIANCE_TYPE.value,
        default_value="diag",
        value_choices=["sperical", "diag", "full", "tied"],
    ),
    AttributeName.N_ITER.value: IntAttribute(
        name=AttributeName.N_ITER.value,
        default_value=10,
        default_low=1,
        default_high=5000,
    ),
    AttributeName.TOL.value: FloatAttribute(
        name=AttributeName.TOL.value,
        default_value=1e-2,
        default_low=-1e10,
        default_high=1e10,
    ),
    AttributeName.INIT_PARAMS.value: StringAttribute(
        name=AttributeName.INIT_PARAMS.value,
        default_value="stmcw",
        value_choices=[
            "s",
            "t",
            "m",
            "c",
            "w",
            "st",
            "sm",
            "sc",
            "sw",
            "tm",
            "tc",
            "tw",
            "mc",
            "mw",
            "cw",
            "stm",
            "stc",
            "stw",
            "smc",
            "smw",
            "scw",
            "tmc",
            "tmw",
            "tcw",
            "mcw",
            "stmc",
            "stmw",
            "stcw",
            "smcw",
            "tmcw",
            "stmcw",
        ],
    ),
    AttributeName.PARAMS.value: StringAttribute(
        name=AttributeName.PARAMS.value,
        default_value="stmcw",
        value_choices=[
            "s",
            "t",
            "m",
            "c",
            "w",
            "st",
            "sm",
            "sc",
            "sw",
            "tm",
            "tc",
            "tw",
            "mc",
            "mw",
            "cw",
            "stm",
            "stc",
            "stw",
            "smc",
            "smw",
            "scw",
            "tmc",
            "tmw",
            "tcw",
            "mcw",
            "stmc",
            "stmw",
            "stcw",
            "smcw",
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
