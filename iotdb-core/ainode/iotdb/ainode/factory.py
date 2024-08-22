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

import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sktime.annotation.hmm_learn import GaussianHMM, GMMHMM
from sktime.annotation.stray import STRAY
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.exp_smoothing import ExponentialSmoothing
from sktime.forecasting.naive import NaiveForecaster
from sktime.forecasting.trend import STLForecaster

from iotdb.ainode.attribute import get_model_attributes, parse_attribute
from iotdb.ainode.constant import BuiltInModelType
from iotdb.ainode.exception import AttributeNotSupportError, BuiltInModelNotSupportError, InferenceModelInternalError


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


def create_built_in_model(model_id, inference_attributes):
    """
    Args:
        model_id: the unique id of the model
        inference_attributes: a list of attributes to be inferred, in this function, the attributes will include some
            parameters of the built-in model. Some parameters are optional, and if the parameters are not
            specified, the default value will be used.
    Returns:
        model: the built-in model from sktime
        attributes: a dict of attributes, where the key is the attribute name, the value is the parsed value of the
            attribute
    Description:
        the create_built_in_model function will create the built-in model from sktime, which does not require user
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
    model = None
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

    return model, attributes
