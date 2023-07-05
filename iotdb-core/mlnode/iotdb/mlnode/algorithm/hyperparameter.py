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
from abc import abstractmethod
from typing import Optional, List, Dict, Tuple

import optuna

from iotdb.mlnode.algorithm.models.forecast.dlinear import dlinear_structure_hyperparameter_map
from iotdb.mlnode.exception import BadConfigValueError

from iotdb.mlnode.algorithm.enums import ForecastModelType
from iotdb.mlnode.algorithm.validator import Validator, NumberRangeValidator
from iotdb.mlnode.parser import TaskOptions


class Hyperparameter(object):

    def __init__(self, name: str):
        """
        Args:
            name: name of the hyperparameter
        """
        self._name = name

    @abstractmethod
    def get_default_value(self):
        raise NotImplementedError

    @abstractmethod
    def validate(self, value):
        pass

    @abstractmethod
    def validate_range(self, min_value, max_value):
        pass

    @abstractmethod
    def suggest_parameter(self, optuna_suggest: optuna.Trial):
        raise NotImplementedError

    @abstractmethod
    def parse(self, string_value: str):
        pass


class IntHyperparameter(Hyperparameter):
    def __init__(self, name: str,
                 log: bool,
                 default_value: int,
                 value_validators: List[Validator],
                 default_low: int,
                 low_validators: List[Validator],
                 default_high: int,
                 high_validators: List[Validator],
                 step: Optional[int] = None,
                 tuning: bool = True
                 ):
        super(IntHyperparameter, self).__init__(name)
        self.__log = log
        self.__default_value = default_value
        self.__value_validators = value_validators
        self.__default_low = default_low
        self.__low_validators = low_validators
        self.__default_high = default_high
        self.__high_validators = high_validators
        self.__step = step

    def get_default_value(self):
        return self.__default_value

    def validate(self, value):
        try:
            for validator in self.__value_validators:
                validator.validate(int(value))
        except Exception as e:
            raise BadConfigValueError(self._name, value, str(e))

    def validate_range(self, min_value: int, max_value: int):
        try:
            for validator in self.__low_validators:
                validator.validate(min_value)
        except Exception as e:
            raise BadConfigValueError(self._name, min_value, str(e))

        try:
            for validator in self.__low_validators:
                validator.validate(max_value)
        except Exception as e:
            raise BadConfigValueError(self._name, max_value, str(e))

    def suggest_parameter(self, optuna_suggest: optuna.Trial):
        return optuna_suggest.suggest_int(
            name=self._name,
            low=self.__default_low,
            high=self.__default_high,
            step=self.__step,
            log=self.__log
        )

    def parse(self, string_value: str):
        return int(string_value)


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
        super(FloatHyperparameter, self).__init__(name)
        self.__log = log
        self.__default_value = default_value
        self.__value_validators = value_validators
        self.__default_low = default_low
        self.__low_validators = low_validators
        self.__default_high = default_high
        self.__high_validators = high_validators
        self.__step = step

    def get_default_value(self):
        return self.__default_value

    def suggest_parameter(self, optuna_suggest: optuna.Trial):
        return optuna_suggest.suggest_float(
            name=self._name,
            low=self.__default_low,
            high=self.__default_high,
            step=self.__step,
            log=self.__log
        )

    def validate(self, value):
        try:
            for validator in self.__value_validators:
                validator.validate(float(value))
        except Exception as e:
            raise BadConfigValueError(self._name, value, str(e))

    def validate_range(self, min_value: float, max_value: float):
        try:
            for validator in self.__low_validators:
                validator.validate(min_value)
        except Exception as e:
            raise BadConfigValueError(self._name, min_value, str(e))

        try:
            for validator in self.__low_validators:
                validator.validate(max_value)
        except Exception as e:
            raise BadConfigValueError(self._name, max_value, str(e))

    def parse(self, string_value: str):
        return float(string_value)


class HyperparameterName(Enum):
    LEARNING_RATE = "learning_rate"
    EPOCHS = "epochs"
    BATCH_SIZE = "batch_size"

    # Structure hyperparameter
    KERNEL_SIZE = "kernel_size"

    def name(self):
        return self.value


training_hyperparameter_map = {
    HyperparameterName.LEARNING_RATE.name(): FloatHyperparameter(name=HyperparameterName.LEARNING_RATE.name(),
                                                                 log=True,
                                                                 default_value=1e-3,
                                                                 value_validators=[NumberRangeValidator(1e-8, 10)],
                                                                 default_low=1e-5,
                                                                 low_validators=[],
                                                                 default_high=1e-1,
                                                                 high_validators=[])
}


def get_structure_hyperparameter_map(model_type: ForecastModelType) -> Dict[str, Hyperparameter]:
    if model_type == ForecastModelType.DLINEAR:
        return dlinear_structure_hyperparameter_map
    else:
        raise NotImplementedError(f"Model type {model_type} is not supported yet")


def parse_fixed_hyperparameters(
        task_options: TaskOptions,
        input_hyperparameters: Dict[str, str]
) -> Tuple[Dict, Dict]:
    hyperparameter_map = get_model_hyperparameter_map(task_options.model_type)
    return None, None


def parse_dict(input_hyperparameters: Dict[str, str], hyperparameter_template_map: Dict[str, Hyperparameter]) -> Dict:
    hyperparameter = {}
    for hyperparameter_name in hyperparameter_template_map.keys():
        hyperparameter_template = hyperparameter_template_map[hyperparameter_name]

        # if user define current hyperparameter
        if hyperparameter_name in input_hyperparameters.keys():
            value = hyperparameter_template.parse(input_hyperparameters[hyperparameter_name])
            hyperparameter_template.validate(value)
            hyperparameter[hyperparameter_name] = value
        # use default value
        else:
            hyperparameter[hyperparameter_name] = hyperparameter_template.get_default_value()
    return hyperparameter


def generate_hyperparameters(
        optuna_suggest: optuna.Trial,
        task_options: TaskOptions,
        input_hyperparameters: Dict[str, str]
) -> Tuple[Dict, Dict]:
    hyperparameter_map = get_model_hyperparameter_map(task_options.model_type)
    return None, None
