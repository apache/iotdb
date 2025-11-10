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
from typing import Callable, Dict, List
from enum import Enum
from iotdb.ainode.core.exception import (
    BuiltInModelNotSupportError,
    ListRangeException,
    NumericalRangeException,
    StringRangeException,
    WrongAttributeTypeError,
)
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_enums import BuiltInModelType

logger = Logger()


class AttributeName(Enum):
    # forecast Attribute
    PREDICT_LENGTH = "predict_length"

    # NaiveForecaster
    STRATEGY = "strategy"
    SP = "sp"

    # STLForecaster
    # SP = 'sp'
    SEASONAL = "seasonal"
    SEASONAL_DEG = "seasonal_deg"
    TREND_DEG = "trend_deg"
    LOW_PASS_DEG = "low_pass_deg"
    SEASONAL_JUMP = "seasonal_jump"
    TREND_JUMP = "trend_jump"
    LOSS_PASS_JUMP = "low_pass_jump"

    # ExponentialSmoothing
    DAMPED_TREND = "damped_trend"
    INITIALIZATION_METHOD = "initialization_method"
    OPTIMIZED = "optimized"
    REMOVE_BIAS = "remove_bias"
    USE_BRUTE = "use_brute"

    # Arima
    ORDER = "order"
    SEASONAL_ORDER = "seasonal_order"
    METHOD = "method"
    MAXITER = "maxiter"
    SUPPRESS_WARNINGS = "suppress_warnings"
    OUT_OF_SAMPLE_SIZE = "out_of_sample_size"
    SCORING = "scoring"
    WITH_INTERCEPT = "with_intercept"
    TIME_VARYING_REGRESSION = "time_varying_regression"
    ENFORCE_STATIONARITY = "enforce_stationarity"
    ENFORCE_INVERTIBILITY = "enforce_invertibility"
    SIMPLE_DIFFERENCING = "simple_differencing"
    MEASUREMENT_ERROR = "measurement_error"
    MLE_REGRESSION = "mle_regression"
    HAMILTON_REPRESENTATION = "hamilton_representation"
    CONCENTRATE_SCALE = "concentrate_scale"

    # GAUSSIAN_HMM
    N_COMPONENTS = "n_components"
    COVARIANCE_TYPE = "covariance_type"
    MIN_COVAR = "min_covar"
    STARTPROB_PRIOR = "startprob_prior"
    TRANSMAT_PRIOR = "transmat_prior"
    MEANS_PRIOR = "means_prior"
    MEANS_WEIGHT = "means_weight"
    COVARS_PRIOR = "covars_prior"
    COVARS_WEIGHT = "covars_weight"
    ALGORITHM = "algorithm"
    N_ITER = "n_iter"
    TOL = "tol"
    PARAMS = "params"
    INIT_PARAMS = "init_params"
    IMPLEMENTATION = "implementation"

    # GMMHMM
    # N_COMPONENTS = "n_components"
    N_MIX = "n_mix"
    # MIN_COVAR = "min_covar"
    # STARTPROB_PRIOR = "startprob_prior"
    # TRANSMAT_PRIOR = "transmat_prior"
    WEIGHTS_PRIOR = "weights_prior"

    # MEANS_PRIOR = "means_prior"
    # MEANS_WEIGHT = "means_weight"
    # ALGORITHM = "algorithm"
    # COVARIANCE_TYPE = "covariance_type"
    # N_ITER = "n_iter"
    # TOL = "tol"
    # INIT_PARAMS = "init_params"
    # PARAMS = "params"
    # IMPLEMENTATION = "implementation"

    # STRAY
    ALPHA = "alpha"
    K = "k"
    KNN_ALGORITHM = "knn_algorithm"
    P = "p"
    SIZE_THRESHOLD = "size_threshold"
    OUTLIER_TAIL = "outlier_tail"

    # timerxl
    INPUT_TOKEN_LEN = "input_token_len"
    HIDDEN_SIZE = "hidden_size"
    INTERMEDIATE_SIZE = "intermediate_size"
    OUTPUT_TOKEN_LENS = "output_token_lens"
    NUM_HIDDEN_LAYERS = "num_hidden_layers"
    NUM_ATTENTION_HEADS = "num_attention_heads"
    HIDDEN_ACT = "hidden_act"
    USE_CACHE = "use_cache"
    ROPE_THETA = "rope_theta"
    ATTENTION_DROPOUT = "attention_dropout"
    INITIALIZER_RANGE = "initializer_range"
    MAX_POSITION_EMBEDDINGS = "max_position_embeddings"
    CKPT_PATH = "ckpt_path"

    # sundial
    DROPOUT_RATE = "dropout_rate"
    FLOW_LOSS_DEPTH = "flow_loss_depth"
    NUM_SAMPLING_STEPS = "num_sampling_steps"
    DIFFUSION_BATCH_MUL = "diffusion_batch_mul"

    def name(self) -> str:
        return self.value


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


def get_attributes(model_type: BuiltInModelType):
    """
    Get the attribute map of the built-in model.

    Args:
        model_type: the type of the built-in model

    Returns:
        the attribute map of the built-in model

    """
    if model_type == BuiltInModelType.ARIMA:
        attribute_map = arima_attribute_map
    elif model_type == BuiltInModelType.NAIVE_FORECASTER:
        attribute_map = naive_forecaster_attribute_map
    elif (
            model_type == BuiltInModelType.EXPONENTIAL_SMOOTHING
            or model_type == BuiltInModelType.HOLTWINTERS
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
    else:
        raise BuiltInModelNotSupportError(model_type.value)
    return attribute_map


def update_attribute(
        input_attributes: Dict[str, str], attribute_map: Dict[str, Attribute]
):
    """
    Update the attribute of the built-in model using the input attributes.
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
