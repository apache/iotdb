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

from dataclasses import dataclass, field
from typing import Any, Dict, List, Union

from iotdb.ainode.core.exception import (
    BuiltInModelNotSupportException,
    ListRangeException,
    NumericalRangeException,
    StringRangeException,
    WrongAttributeTypeException,
)
from iotdb.ainode.core.log import Logger

logger = Logger()


@dataclass
class AttributeConfig:
    """Base class for attribute configuration"""

    name: str
    default: Any
    type: str  # 'int', 'float', 'str', 'bool', 'list', 'tuple'
    low: Union[int, float, None] = None
    high: Union[int, float, None] = None
    choices: List[str] = field(default_factory=list)
    value_type: type = None  # Element type for list and tuple

    def validate_value(self, value):
        """Validate if the value meets the requirements"""
        if self.type == "int":
            if value is None:
                return True  # Allow None for optional int parameters
            if not isinstance(value, int):
                raise WrongAttributeTypeException(self.name, "int")
            if self.low is not None and self.high is not None:
                if not (self.low <= value <= self.high):
                    raise NumericalRangeException(self.name, value, self.low, self.high)
        elif self.type == "float":
            if value is None:
                return True  # Allow None for optional float parameters
            if not isinstance(value, (int, float)):
                raise WrongAttributeTypeException(self.name, "float")
            value = float(value)
            if self.low is not None and self.high is not None:
                if not (self.low <= value <= self.high):
                    raise NumericalRangeException(self.name, value, self.low, self.high)
        elif self.type == "str":
            if value is None:
                return True  # Allow None for optional str parameters
            if not isinstance(value, str):
                raise WrongAttributeTypeException(self.name, "str")
            if self.choices and value not in self.choices:
                raise StringRangeException(self.name, value, self.choices)
        elif self.type == "bool":
            if value is None:
                return True  # Allow None for optional bool parameters
            if not isinstance(value, bool):
                raise WrongAttributeTypeException(self.name, "bool")
        elif self.type == "list":
            if not isinstance(value, list):
                raise WrongAttributeTypeException(self.name, "list")
            for item in value:
                if not isinstance(item, self.value_type):
                    raise WrongAttributeTypeException(self.name, self.value_type)
        elif self.type == "tuple":
            if not isinstance(value, tuple):
                raise WrongAttributeTypeException(self.name, "tuple")
            for item in value:
                if not isinstance(item, self.value_type):
                    raise WrongAttributeTypeException(self.name, self.value_type)
        return True

    def parse(self, string_value: str):
        """Parse string value to corresponding type"""
        if self.type == "int":
            if string_value.lower() == "none" or string_value.strip() == "":
                return None
            try:
                return int(string_value)
            except:
                raise WrongAttributeTypeException(self.name, "int")
        elif self.type == "float":
            if string_value.lower() == "none" or string_value.strip() == "":
                return None
            try:
                return float(string_value)
            except:
                raise WrongAttributeTypeException(self.name, "float")
        elif self.type == "str":
            if string_value.lower() == "none" or string_value.strip() == "":
                return None
            return string_value
        elif self.type == "bool":
            if string_value.lower() == "true":
                return True
            elif string_value.lower() == "false":
                return False
            elif string_value.lower() == "none" or string_value.strip() == "":
                return None
            else:
                raise WrongAttributeTypeException(self.name, "bool")
        elif self.type == "list":
            try:
                list_value = eval(string_value)
            except:
                raise WrongAttributeTypeException(self.name, "list")
            if not isinstance(list_value, list):
                raise WrongAttributeTypeException(self.name, "list")
            for i in range(len(list_value)):
                try:
                    list_value[i] = self.value_type(list_value[i])
                except:
                    raise ListRangeException(
                        self.name, list_value, str(self.value_type)
                    )
            return list_value
        elif self.type == "tuple":
            try:
                tuple_value = eval(string_value)
            except:
                raise WrongAttributeTypeException(self.name, "tuple")
            if not isinstance(tuple_value, tuple):
                raise WrongAttributeTypeException(self.name, "tuple")
            list_value = list(tuple_value)
            for i in range(len(list_value)):
                try:
                    list_value[i] = self.value_type(list_value[i])
                except:
                    raise ListRangeException(
                        self.name, list_value, str(self.value_type)
                    )
            return tuple(list_value)


# Model configuration definitions - using concise dictionary format
MODEL_CONFIGS = {
    "NAIVE_FORECASTER": {
        "output_length": AttributeConfig("output_length", 1, "int", 1, 5000),
        "strategy": AttributeConfig(
            "strategy", "last", "str", choices=["last", "mean", "drift"]
        ),
        "window_length": AttributeConfig("window_length", None, "int"),
        "sp": AttributeConfig("sp", 1, "int", 1, 5000),
    },
    "EXPONENTIAL_SMOOTHING": {
        "output_length": AttributeConfig("output_length", 1, "int", 1, 5000),
        "damped_trend": AttributeConfig("damped_trend", False, "bool"),
        "initialization_method": AttributeConfig(
            "initialization_method",
            "estimated",
            "str",
            choices=["estimated", "heuristic", "legacy-heuristic", "known"],
        ),
        "optimized": AttributeConfig("optimized", True, "bool"),
        "remove_bias": AttributeConfig("remove_bias", False, "bool"),
        "use_brute": AttributeConfig("use_brute", False, "bool"),
    },
    "ARIMA": {
        "output_length": AttributeConfig("output_length", 1, "int", 1, 5000),
        "order": AttributeConfig("order", (1, 0, 0), "tuple", value_type=int),
        "seasonal_order": AttributeConfig(
            "seasonal_order", (0, 0, 0, 0), "tuple", value_type=int
        ),
        "start_params": AttributeConfig("start_params", None, "str"),
        "method": AttributeConfig(
            "method",
            "lbfgs",
            "str",
            choices=["lbfgs", "bfgs", "newton", "nm", "cg", "ncg", "powell"],
        ),
        "maxiter": AttributeConfig("maxiter", 50, "int", 1, 5000),
        "suppress_warnings": AttributeConfig("suppress_warnings", False, "bool"),
        "out_of_sample_size": AttributeConfig("out_of_sample_size", 0, "int", 0, 5000),
        "scoring": AttributeConfig(
            "scoring",
            "mse",
            "str",
            choices=["mse", "mae", "rmse", "mape", "smape", "rmsle", "r2"],
        ),
        "scoring_args": AttributeConfig("scoring_args", None, "str"),
        "trend": AttributeConfig("trend", None, "str"),
        "with_intercept": AttributeConfig("with_intercept", True, "bool"),
        "time_varying_regression": AttributeConfig(
            "time_varying_regression", False, "bool"
        ),
        "enforce_stationarity": AttributeConfig("enforce_stationarity", True, "bool"),
        "enforce_invertibility": AttributeConfig("enforce_invertibility", True, "bool"),
        "simple_differencing": AttributeConfig("simple_differencing", False, "bool"),
        "measurement_error": AttributeConfig("measurement_error", False, "bool"),
        "mle_regression": AttributeConfig("mle_regression", True, "bool"),
        "hamilton_representation": AttributeConfig(
            "hamilton_representation", False, "bool"
        ),
        "concentrate_scale": AttributeConfig("concentrate_scale", False, "bool"),
    },
    "STL_FORECASTER": {
        "output_length": AttributeConfig("output_length", 1, "int", 1, 5000),
        "sp": AttributeConfig("sp", 2, "int", 1, 5000),
        "seasonal": AttributeConfig("seasonal", 7, "int", 1, 5000),
        "trend": AttributeConfig("trend", None, "int"),
        "low_pass": AttributeConfig("low_pass", None, "int"),
        "seasonal_deg": AttributeConfig("seasonal_deg", 1, "int", 0, 5000),
        "trend_deg": AttributeConfig("trend_deg", 1, "int", 0, 5000),
        "low_pass_deg": AttributeConfig("low_pass_deg", 1, "int", 0, 5000),
        "robust": AttributeConfig("robust", False, "bool"),
        "seasonal_jump": AttributeConfig("seasonal_jump", 1, "int", 0, 5000),
        "trend_jump": AttributeConfig("trend_jump", 1, "int", 0, 5000),
        "low_pass_jump": AttributeConfig("low_pass_jump", 1, "int", 0, 5000),
        "inner_iter": AttributeConfig("inner_iter", None, "int"),
        "outer_iter": AttributeConfig("outer_iter", None, "int"),
        "forecaster_trend": AttributeConfig("forecaster_trend", None, "str"),
        "forecaster_seasonal": AttributeConfig("forecaster_seasonal", None, "str"),
        "forecaster_resid": AttributeConfig("forecaster_resid", None, "str"),
    },
    "GAUSSIAN_HMM": {
        "n_components": AttributeConfig("n_components", 1, "int", 1, 5000),
        "covariance_type": AttributeConfig(
            "covariance_type",
            "diag",
            "str",
            choices=["spherical", "diag", "full", "tied"],
        ),
        "min_covar": AttributeConfig("min_covar", 1e-3, "float", -1e10, 1e10),
        "startprob_prior": AttributeConfig(
            "startprob_prior", 1.0, "float", -1e10, 1e10
        ),
        "transmat_prior": AttributeConfig("transmat_prior", 1.0, "float", -1e10, 1e10),
        "means_prior": AttributeConfig("means_prior", 0, "float", -1e10, 1e10),
        "means_weight": AttributeConfig("means_weight", 0, "float", -1e10, 1e10),
        "covars_prior": AttributeConfig("covars_prior", 0.01, "float", -1e10, 1e10),
        "covars_weight": AttributeConfig("covars_weight", 1, "float", -1e10, 1e10),
        "algorithm": AttributeConfig(
            "algorithm", "viterbi", "str", choices=["viterbi", "map"]
        ),
        "random_state": AttributeConfig("random_state", None, "float"),
        "n_iter": AttributeConfig("n_iter", 10, "int", 1, 5000),
        "tol": AttributeConfig("tol", 1e-2, "float", -1e10, 1e10),
        "verbose": AttributeConfig("verbose", False, "bool"),
        "params": AttributeConfig("params", "stmc", "str", choices=["stmc", "stm"]),
        "init_params": AttributeConfig(
            "init_params", "stmc", "str", choices=["stmc", "stm"]
        ),
        "implementation": AttributeConfig(
            "implementation", "log", "str", choices=["log", "scaling"]
        ),
    },
    "GMM_HMM": {
        "n_components": AttributeConfig("n_components", 1, "int", 1, 5000),
        "n_mix": AttributeConfig("n_mix", 1, "int", 1, 5000),
        "min_covar": AttributeConfig("min_covar", 1e-3, "float", -1e10, 1e10),
        "startprob_prior": AttributeConfig(
            "startprob_prior", 1.0, "float", -1e10, 1e10
        ),
        "transmat_prior": AttributeConfig("transmat_prior", 1.0, "float", -1e10, 1e10),
        "weights_prior": AttributeConfig("weights_prior", 1.0, "float", -1e10, 1e10),
        "means_prior": AttributeConfig("means_prior", 0.0, "float", -1e10, 1e10),
        "means_weight": AttributeConfig("means_weight", 0.0, "float", -1e10, 1e10),
        "covars_prior": AttributeConfig("covars_prior", None, "float"),
        "covars_weight": AttributeConfig("covars_weight", None, "float"),
        "algorithm": AttributeConfig(
            "algorithm", "viterbi", "str", choices=["viterbi", "map"]
        ),
        "covariance_type": AttributeConfig(
            "covariance_type",
            "diag",
            "str",
            choices=["spherical", "diag", "full", "tied"],
        ),
        "random_state": AttributeConfig("random_state", None, "int"),
        "n_iter": AttributeConfig("n_iter", 10, "int", 1, 5000),
        "tol": AttributeConfig("tol", 1e-2, "float", -1e10, 1e10),
        "verbose": AttributeConfig("verbose", False, "bool"),
        "init_params": AttributeConfig(
            "init_params",
            "stmcw",
            "str",
            choices=[
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
        "params": AttributeConfig(
            "params",
            "stmcw",
            "str",
            choices=[
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
        "implementation": AttributeConfig(
            "implementation", "log", "str", choices=["log", "scaling"]
        ),
    },
    "STRAY": {
        "alpha": AttributeConfig("alpha", 0.01, "float", -1e10, 1e10),
        "k": AttributeConfig("k", 10, "int", 1, 5000),
        "knn_algorithm": AttributeConfig(
            "knn_algorithm",
            "brute",
            "str",
            choices=["brute", "kd_tree", "ball_tree", "auto"],
        ),
        "p": AttributeConfig("p", 0.5, "float", -1e10, 1e10),
        "size_threshold": AttributeConfig("size_threshold", 50, "int", 1, 5000),
        "outlier_tail": AttributeConfig(
            "outlier_tail", "max", "str", choices=["min", "max"]
        ),
    },
}


def get_attributes(model_id: str) -> Dict[str, AttributeConfig]:
    """Get attribute configuration for Sktime model"""
    model_id = "EXPONENTIAL_SMOOTHING" if model_id == "HOLTWINTERS" else model_id
    if model_id not in MODEL_CONFIGS:
        raise BuiltInModelNotSupportException(model_id)
    return MODEL_CONFIGS[model_id]


def update_attribute(
    input_attributes: Dict[str, str], attribute_map: Dict[str, AttributeConfig]
) -> Dict[str, Any]:
    """Update Sktime model attributes using input attributes"""
    attributes = {}
    for name, config in attribute_map.items():
        if name in input_attributes:
            value = config.parse(input_attributes[name])
            config.validate_value(value)
            attributes[name] = value
        else:
            attributes[name] = config.default
    return attributes
