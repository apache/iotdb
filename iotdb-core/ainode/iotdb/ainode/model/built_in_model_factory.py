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
from typing import List, Dict

import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sktime.annotation.hmm_learn import GaussianHMM, GMMHMM
from sktime.annotation.stray import STRAY
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.exp_smoothing import ExponentialSmoothing
from sktime.forecasting.naive import NaiveForecaster
from sktime.forecasting.trend import STLForecaster

from iotdb.ainode.constant import AttributeName, BuiltInModelType
from iotdb.ainode.exception import InferenceModelInternalError, AttributeNotSupportError
from iotdb.ainode.exception import WrongAttributeTypeError, NumericalRangeException, StringRangeException, \
    ListRangeException, BuiltInModelNotSupportError
from iotdb.ainode.log import Logger

logger = Logger()


def get_model_attributes(model_id: str):
    if model_id == BuiltInModelType.ARIMA.value:
        attribute_map = arima_attribute_map
    elif model_id == BuiltInModelType.NAIVE_FORECASTER.value:
        attribute_map = naive_forecaster_attribute_map
    elif model_id == BuiltInModelType.EXPONENTIAL_SMOOTHING.value:
        attribute_map = exponential_smoothing_attribute_map
    elif model_id == BuiltInModelType.STL_FORECASTER.value:
        attribute_map = stl_forecaster_attribute_map
    elif model_id == BuiltInModelType.GMM_HMM.value:
        attribute_map = gmmhmm_attribute_map
    elif model_id == BuiltInModelType.GAUSSIAN_HMM.value:
        attribute_map = gaussian_hmm_attribute_map
    elif model_id == BuiltInModelType.STRAY.value:
        attribute_map = stray_attribute_map
    else:
        raise BuiltInModelNotSupportError(model_id)
    return attribute_map


def fetch_built_in_model(model_id, inference_attributes):
    """
    Args:
        model_id: the unique id of the model
        inference_attributes: a list of attributes to be inferred, in this function, the attributes will include some
            parameters of the built-in model. Some parameters are optional, and if the parameters are not
            specified, the default value will be used.
    Returns:
        model: the built-in model
        attributes: a dict of attributes, where the key is the attribute name, the value is the parsed value of the
            attribute
    Description:
        the create_built_in_model function will create the built-in model, which does not require user
        registration. This module will parse the inference attributes and create the built-in model.
    """
    attribute_map = get_model_attributes(model_id)

    # validate the inference attributes
    for attribute_name in inference_attributes:
        if attribute_name not in attribute_map:
            raise AttributeNotSupportError(model_id, attribute_name)

    # parse the inference attributes, attributes is a Dict[str, Any]
    attributes = parse_attribute(inference_attributes, attribute_map)

    # build the built-in model
    if model_id == BuiltInModelType.ARIMA.value:
        model = ArimaModel(attributes)
    elif model_id == BuiltInModelType.EXPONENTIAL_SMOOTHING.value:
        model = ExponentialSmoothingModel(attributes)
    elif model_id == BuiltInModelType.NAIVE_FORECASTER.value:
        model = NaiveForecasterModel(attributes)
    elif model_id == BuiltInModelType.STL_FORECASTER.value:
        model = STLForecasterModel(attributes)
    elif model_id == BuiltInModelType.GMM_HMM.value:
        model = GMMHMMModel(attributes)
    elif model_id == BuiltInModelType.GAUSSIAN_HMM.value:
        model = GaussianHmmModel(attributes)
    elif model_id == BuiltInModelType.STRAY.value:
        model = STRAYModel(attributes)
    else:
        raise BuiltInModelNotSupportError(model_id)

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
    def __init__(self, name: str,
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
        raise NumericalRangeException(self._name, value, self.__default_low, self.__default_high)

    def parse(self, string_value: str):
        try:
            int_value = int(string_value)
        except:
            raise WrongAttributeTypeError(self._name, "int")
        return int_value


class FloatAttribute(Attribute):
    def __init__(self, name: str,
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
        raise NumericalRangeException(self._name, value, self.__default_low, self.__default_high)

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
                raise ListRangeException(self._name, list_value, self.__type_to_str[self.__value_type])
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
                raise ListRangeException(self._name, list_value, self.__type_to_str[self.__value_type])
        tuple_value = tuple(list_value)
        return tuple_value


def parse_attribute(input_attributes: Dict[str, str], attribute_map: Dict[str, Attribute]):
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
                attributes[attribute_name] = attribute_map[attribute_name].get_default_value()
            except NotImplementedError as e:
                logger.error(f"attribute {attribute_name} is not implemented.")
                raise e
    return attributes


# built-in sktime model attributes
# NaiveForecaster
naive_forecaster_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000
    ),
    AttributeName.STRATEGY.value: StringAttribute(
        name=AttributeName.STRATEGY.value,
        default_value="last",
        value_choices=["last", "mean"],
    ),
    AttributeName.SP.value: IntAttribute(
        name=AttributeName.SP.value,
        default_value=1,
        default_low=1,
        default_high=5000
    ),
}
# ExponentialSmoothing
exponential_smoothing_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000
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
    )
}
# Arima
arima_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000
    ),
    AttributeName.ORDER.value: TupleAttribute(
        name=AttributeName.ORDER.value,
        default_value=(1, 0, 0),
        value_type=int
    ),
    AttributeName.SEASONAL_ORDER.value: TupleAttribute(
        name=AttributeName.SEASONAL_ORDER.value,
        default_value=(0, 0, 0, 0),
        value_type=int
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
        default_high=5000
    ),
    AttributeName.SUPPRESS_WARNINGS.value: BooleanAttribute(
        name=AttributeName.SUPPRESS_WARNINGS.value,
        default_value=True,
    ),
    AttributeName.OUT_OF_SAMPLE_SIZE.value: IntAttribute(
        name=AttributeName.OUT_OF_SAMPLE_SIZE.value,
        default_value=0,
        default_low=0,
        default_high=5000
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
    )
}
# STLForecaster
stl_forecaster_attribute_map = {
    AttributeName.PREDICT_LENGTH.value: IntAttribute(
        name=AttributeName.PREDICT_LENGTH.value,
        default_value=1,
        default_low=1,
        default_high=5000
    ),
    AttributeName.SP.value: IntAttribute(
        name=AttributeName.SP.value,
        default_value=2,
        default_low=1,
        default_high=5000
    ),
    AttributeName.SEASONAL.value: IntAttribute(
        name=AttributeName.SEASONAL.value,
        default_value=7,
        default_low=1,
        default_high=5000
    ),
    AttributeName.SEASONAL_DEG.value: IntAttribute(
        name=AttributeName.SEASONAL_DEG.value,
        default_value=1,
        default_low=0,
        default_high=5000
    ),
    AttributeName.TREND_DEG.value: IntAttribute(
        name=AttributeName.TREND_DEG.value,
        default_value=1,
        default_low=0,
        default_high=5000
    ),
    AttributeName.LOW_PASS_DEG.value: IntAttribute(
        name=AttributeName.LOW_PASS_DEG.value,
        default_value=1,
        default_low=0,
        default_high=5000
    ),
    AttributeName.SEASONAL_JUMP.value: IntAttribute(
        name=AttributeName.SEASONAL_JUMP.value,
        default_value=1,
        default_low=0,
        default_high=5000
    ),
    AttributeName.TREND_JUMP.value: IntAttribute(
        name=AttributeName.TREND_JUMP.value,
        default_value=1,
        default_low=0,
        default_high=5000
    ),
    AttributeName.LOSS_PASS_JUMP.value: IntAttribute(
        name=AttributeName.LOSS_PASS_JUMP.value,
        default_value=1,
        default_low=0,
        default_high=5000
    ),
}

# GAUSSIAN_HMM
gaussian_hmm_attribute_map = {
    AttributeName.N_COMPONENTS.value: IntAttribute(
        name=AttributeName.N_COMPONENTS.value,
        default_value=1,
        default_low=1,
        default_high=5000
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
        default_high=5000
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
    )
}

# GMMHMM
gmmhmm_attribute_map = {
    AttributeName.N_COMPONENTS.value: IntAttribute(
        name=AttributeName.N_COMPONENTS.value,
        default_value=1,
        default_low=1,
        default_high=5000
    ),
    AttributeName.N_MIX.value: IntAttribute(
        name=AttributeName.N_MIX.value,
        default_value=1,
        default_low=1,
        default_high=5000
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
        default_high=5000
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
        value_choices=["s", "t", "m", "c", "w", "st", "sm", "sc", "sw", "tm", "tc", "tw", "mc", "mw", "cw", "stm",
                       "stc", "stw", "smc", "smw", "scw", "tmc", "tmw", "tcw", "mcw", "stmc", "stmw", "stcw", "smcw",
                       "tmcw", "stmcw"]
    ),
    AttributeName.PARAMS.value: StringAttribute(
        name=AttributeName.PARAMS.value,
        default_value="stmcw",
        value_choices=["s", "t", "m", "c", "w", "st", "sm", "sc", "sw", "tm", "tc", "tw", "mc", "mw", "cw", "stm",
                       "stc", "stw", "smc", "smw", "scw", "tmc", "tmw", "tcw", "mcw", "stmc", "stmw", "stcw", "smcw",
                       "tmcw", "stmcw"]
    ),
    AttributeName.IMPLEMENTATION.value: StringAttribute(
        name=AttributeName.IMPLEMENTATION.value,
        default_value="log",
        value_choices=["log", "scaling"],
    )
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
        name=AttributeName.K.value,
        default_value=10,
        default_low=1,
        default_high=5000
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
        default_high=5000
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
            order=attributes['order'],
            seasonal_order=attributes['seasonal_order'],
            method=attributes['method'],
            suppress_warnings=attributes['suppress_warnings'],
            maxiter=attributes['maxiter'],
            out_of_sample_size=attributes['out_of_sample_size'],
            scoring=attributes['scoring'],
            with_intercept=attributes['with_intercept'],
            time_varying_regression=attributes['time_varying_regression'],
            enforce_stationarity=attributes['enforce_stationarity'],
            enforce_invertibility=attributes['enforce_invertibility'],
            simple_differencing=attributes['simple_differencing'],
            measurement_error=attributes['measurement_error'],
            mle_regression=attributes['mle_regression'],
            hamilton_representation=attributes['hamilton_representation'],
            concentrate_scale=attributes['concentrate_scale']
        )

    def inference(self, data):
        try:
            predict_length = self._attributes['predict_length']
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
            damped_trend=attributes['damped_trend'],
            initialization_method=attributes['initialization_method'],
            optimized=attributes['optimized'],
            remove_bias=attributes['remove_bias'],
            use_brute=attributes['use_brute']
        )

    def inference(self, data):
        try:
            predict_length = self._attributes['predict_length']
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
            strategy=attributes['strategy'],
            sp=attributes['sp']
        )

    def inference(self, data):
        try:
            predict_length = self._attributes['predict_length']
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
            sp=attributes['sp'],
            seasonal=attributes['seasonal'],
            seasonal_deg=attributes['seasonal_deg'],
            trend_deg=attributes['trend_deg'],
            low_pass_deg=attributes['low_pass_deg'],
            seasonal_jump=attributes['seasonal_jump'],
            trend_jump=attributes['trend_jump'],
            low_pass_jump=attributes['low_pass_jump']
        )

    def inference(self, data):
        try:
            predict_length = self._attributes['predict_length']
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
            n_components=attributes['n_components'],
            n_mix=attributes['n_mix'],
            min_covar=attributes['min_covar'],
            startprob_prior=attributes['startprob_prior'],
            transmat_prior=attributes['transmat_prior'],
            means_prior=attributes['means_prior'],
            means_weight=attributes['means_weight'],
            weights_prior=attributes['weights_prior'],
            algorithm=attributes['algorithm'],
            covariance_type=attributes['covariance_type'],
            n_iter=attributes['n_iter'],
            tol=attributes['tol'],
            params=attributes['params'],
            init_params=attributes['init_params'],
            implementation=attributes['implementation']
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
            n_components=attributes['n_components'],
            covariance_type=attributes['covariance_type'],
            min_covar=attributes['min_covar'],
            startprob_prior=attributes['startprob_prior'],
            transmat_prior=attributes['transmat_prior'],
            means_prior=attributes['means_prior'],
            means_weight=attributes['means_weight'],
            covars_prior=attributes['covars_prior'],
            covars_weight=attributes['covars_weight'],
            algorithm=attributes['algorithm'],
            n_iter=attributes['n_iter'],
            tol=attributes['tol'],
            params=attributes['params'],
            init_params=attributes['init_params'],
            implementation=attributes['implementation']
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
            alpha=attributes['alpha'],
            k=attributes['k'],
            knn_algorithm=attributes['knn_algorithm'],
            p=attributes['p'],
            size_threshold=attributes['size_threshold'],
            outlier_tail=attributes['outlier_tail']
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
