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
    def validate_value(self, value):
        raise NotImplementedError

    @abstractmethod
    def validate_range(self, min_value, max_value):
        raise NotImplementedError

    @abstractmethod
    def suggest_parameter(self, optuna_suggest: optuna.Trial):
        raise NotImplementedError

    @abstractmethod
    def parse(self, string_value: str):
        raise NotImplementedError


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
                 tuning: bool = False
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
        self.__tuning = tuning

    def get_default_value(self):
        return self.__default_value

    def __validate(self, value, validators: List[Validator] = None):
        try:
            for validator in validators:
                validator.validate(int(value))
        except Exception as e:
            raise BadConfigValueError(self._name, value, str(e))

    def validate_value(self, value):
        self.__validate(value, self.__value_validators)

    def validate_range(self, min_value: int, max_value: int):
        self.__validate(min_value, self.__low_validators)
        self.__validate(max_value, self.__high_validators)

    def suggest_parameter(self, optuna_suggest: optuna.Trial):
        if not self.__tuning:
            return self.__default_value
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
                 step: Optional[float] = None,
                 tuning: bool = False):
        super(FloatHyperparameter, self).__init__(name)
        self.__log = log
        self.__default_value = default_value
        self.__value_validators = value_validators
        self.__default_low = default_low
        self.__low_validators = low_validators
        self.__default_high = default_high
        self.__high_validators = high_validators
        self.__step = step
        self.__tuning = tuning

    def get_default_value(self):
        return self.__default_value

    def __validate(self, value, validators: List[Validator] = None):
        try:
            for validator in validators:
                validator.validate(float(value))
        except Exception as e:
            raise BadConfigValueError(self._name, value, str(e))

    def validate_value(self, value):
        self.__validate(value, self.__value_validators)

    def validate_range(self, min_value: float, max_value: float):
        self.__validate(min_value, self.__low_validators)
        self.__validate(max_value, self.__high_validators)

    def suggest_parameter(self, optuna_suggest: optuna.Trial):
        if not self.__tuning:
            return self.__default_value
        return optuna_suggest.suggest_float(
            name=self._name,
            low=self.__default_low,
            high=self.__default_high,
            step=self.__step,
            log=self.__log
        )

    def parse(self, string_value: str):
        return float(string_value)


class HyperparameterName(Enum):
    # Training hyperparameter
    LEARNING_RATE = "learning_rate"
    EPOCHS = "epochs"
    BATCH_SIZE = "batch_size"
    USE_GPU = "use_gpu"
    NUM_WORKERS = "num_workers"

    # Structure hyperparameter
    KERNEL_SIZE = "kernel_size"
    INPUT_VARS = "input_vars"
    BLOCK_TYPE = "block_type"
    D_MODEL = "d_model"
    INNER_LAYERS = "inner_layer"
    OUTER_LAYERS = "outer_layer"

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
                                                                 high_validators=[],
                                                                 tuning=True),
}

dlinear_structure_hyperparameter_map = {
    HyperparameterName.KERNEL_SIZE.value: IntHyperparameter(name=HyperparameterName.KERNEL_SIZE.name(),
                                                            log=True,
                                                            default_value=25,
                                                            value_validators=[NumberRangeValidator(1, 1e10)],
                                                            default_low=5,
                                                            low_validators=[],
                                                            default_high=50,
                                                            high_validators=[])
}

nbeats_structure_hyperparameter_map = {
    HyperparameterName.D_MODEL.value: IntHyperparameter(name=HyperparameterName.D_MODEL.name(),
                                                        log=True,
                                                        default_value=512,
                                                        value_validators=[NumberRangeValidator(4, 8192)],
                                                        default_low=2,
                                                        low_validators=[],
                                                        default_high=2048,
                                                        high_validators=[]),
    HyperparameterName.INNER_LAYERS.value: IntHyperparameter(name=HyperparameterName.INNER_LAYERS.name(),
                                                             log=False,
                                                             default_value=4,
                                                             value_validators=[NumberRangeValidator(1, 128)],
                                                             default_low=1,
                                                             low_validators=[],
                                                             default_high=128,
                                                             high_validators=[]),
    HyperparameterName.OUTER_LAYERS.value: IntHyperparameter(name=HyperparameterName.OUTER_LAYERS.name(),
                                                             log=False,
                                                             default_value=4,
                                                             value_validators=[NumberRangeValidator(1, 128)],
                                                             default_low=1,
                                                             low_validators=[],
                                                             default_high=128,
                                                             high_validators=[])
}


def get_structure_hyperparameter_map(model_type: ForecastModelType) -> Dict[str, Hyperparameter]:
    """
    Different model may have different structure hyperparameters.
    This method returns the structure hyperparameter map for a given model type.
    """
    if model_type == ForecastModelType.DLINEAR:
        return dlinear_structure_hyperparameter_map
    elif model_type == ForecastModelType.DLINEAR_INDIVIDUAL:
        return dlinear_structure_hyperparameter_map
    elif model_type == ForecastModelType.NBEATS:
        return nbeats_structure_hyperparameter_map
    else:
        raise NotImplementedError(f"Model type {model_type} is not supported yet")


def parse_fixed_hyperparameters(
        task_options: TaskOptions,
        input_hyperparameters: Dict[str, str]
) -> Tuple[Dict, Dict]:
    """
    Parse the input hyperparameters into model hyperparameters and task hyperparameters.
    If the input hyperparameters contains hyperparameters that are not defined in the model or task,
    use default value.
    """
    structure_hyperparameter_map = get_structure_hyperparameter_map(task_options.model_type)
    model_hyperparameters = parse_dict(input_hyperparameters, structure_hyperparameter_map)
    task_hyperparameters = parse_dict(input_hyperparameters, training_hyperparameter_map)
    return model_hyperparameters, task_hyperparameters


def parse_dict(input_hyperparameters: Dict[str, str], hyperparameter_template_map: Dict[str, Hyperparameter]) -> Dict:
    hyperparameter = {}
    for hyperparameter_name in hyperparameter_template_map.keys():
        hyperparameter_template = hyperparameter_template_map[hyperparameter_name]

        # if user define current hyperparameter
        if hyperparameter_name in input_hyperparameters.keys():
            value = hyperparameter_template.parse(input_hyperparameters[hyperparameter_name])
            hyperparameter_template.validate_value(value)
            hyperparameter[hyperparameter_name] = value
        # use default value
        else:
            hyperparameter[hyperparameter_name] = hyperparameter_template.get_default_value()
    return hyperparameter


def generate_hyperparameters(
        optuna_suggest: optuna.Trial,
        task_options: TaskOptions
) -> Tuple[Dict, Dict]:
    """
    Generate hyperparameters for model and task by optuna in auto-tuning training.
    """
    # TODO : support user to define hyperparameters in auto_tuning training
    structure_hyperparameter_map = get_structure_hyperparameter_map(task_options.model_type)
    model_hyperparameters = {}
    for hyperparameter_name, hyperparameter_template in structure_hyperparameter_map.items():
        model_hyperparameters[hyperparameter_name] = hyperparameter_template.suggest_parameter(optuna_suggest)

    task_hyperparameters = {}
    for hyperparameter_name, hyperparameter_template in training_hyperparameter_map.items():
        task_hyperparameters[hyperparameter_name] = hyperparameter_template.suggest_parameter(optuna_suggest)
    return model_hyperparameters, task_hyperparameters
