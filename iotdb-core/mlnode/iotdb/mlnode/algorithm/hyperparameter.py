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
from enum import Enum
from typing import Optional, List, Dict, Tuple

import optuna

from iotdb.mlnode.algorithm.enums import ForecastModelType
from iotdb.mlnode.algorithm.models.forecast.dlinear import dlinear_hyperparameter_map
from iotdb.mlnode.algorithm.validator import Validator
from iotdb.mlnode.parser import TaskOptions


class Hyperparameter(object):

    def __init__(self, name: str, log: bool):
        """
        Args:
            name: name of the hyperparameter
        """
        self.__name = name
        self.__log = log


class FloatHyperparameter(Hyperparameter):
    def __init__(self, name: str,
                 log: bool,
                 default_value: float,
                 value_validators: List[Validator],
                 default_low: float,
                 low_validators: List[Validator],
                 default_high: float,
                 high_validators: List[Validator],
                 step: Optional[float] = None):
        super(FloatHyperparameter, self).__init__(name, log)
        self.__default_value = default_value
        self.__value_validators = value_validators
        self.__default_low = default_low
        self.__low_validators = low_validators
        self.__default_high = default_high
        self.__high_validators = high_validators
        self.__step = step


class HyperparameterName(Enum):
    LEARNING_RATE = "learning_rate"

    def name(self):
        return self.value


def get_model_hyperparameter_map(model_type: ForecastModelType):
    if model_type == ForecastModelType.DLINEAR:
        return dlinear_hyperparameter_map
    else:
        raise NotImplementedError(f"Model type {model_type} is not supported yet.")


def parse_fixed_hyperparameters(
        task_options: TaskOptions,
        hyperparameters: Dict[str, str]
) -> Tuple[Dict, Dict]:
    hyperparameter_map = get_model_hyperparameter_map(task_options.model_type)
    return None, None


def generate_hyperparameters(
        optuna_suggest: optuna.Trial,
        task_options: TaskOptions,
        hyperparameters: Dict[str, str]
) -> Tuple[Dict, Dict]:
    hyperparameter_map = get_model_hyperparameter_map(task_options.model_type)
    return None, None
