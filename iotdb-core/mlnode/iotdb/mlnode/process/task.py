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
from multiprocessing.connection import Connection
from typing import Dict, Tuple

import numpy as np
import optuna
import pandas as pd
import torch

from iotdb.mlnode.algorithm.hyperparameter import (generate_hyperparameters,
                                                   parse_fixed_hyperparameters)
from iotdb.mlnode.client import client_manager
from iotdb.mlnode.config import descriptor
from iotdb.mlnode.constant import DEFAULT_TRIAL_ID, TRIAL_ID_PREFIX, OptionsKey, ModelInputName
from iotdb.mlnode.dataset.dataset import TsForecastDataset
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import ForecastTaskOptions
from iotdb.mlnode.process.trial import ForecastingTrainingTrial
from iotdb.mlnode.storage import model_storage
from iotdb.mlnode.util import pack_input_dict
from iotdb.thrift.common.ttypes import TrainingState


class _BasicTask(object):
    def __init__(
            self,
            pid_info: Dict
    ):
        self.pid_info = pid_info

    @abstractmethod
    def __call__(self):
        raise NotImplementedError


# Training Task

class _BasicTrainingTask(_BasicTask):
    def __init__(
            self,
            model_id: str,
            hyperparameters: Dict[str, str],
            pid_info: Dict
    ):
        super().__init__(pid_info)
        self.model_id = model_id
        self.hyperparameters = hyperparameters
        self.configNode_client = client_manager.borrow_config_node_client()

    @abstractmethod
    def __call__(self):
        raise NotImplementedError


class ForecastFixedParamTrainingTask(_BasicTrainingTask):
    def __init__(
            self,
            model_id: str,
            task_options: ForecastTaskOptions,
            hyperparameters: Dict[str, str],
            dataset: TsForecastDataset,
            pid_info: Dict
    ):
        super().__init__(model_id, hyperparameters, pid_info)
        model_hyperparameters, task_hyperparameters = parse_fixed_hyperparameters(task_options, hyperparameters)
        self.dataset = dataset
        self.trial = ForecastingTrainingTrial(
            trial_id=DEFAULT_TRIAL_ID,
            model_id=model_id,
            task_options=task_options,
            model_hyperparameters=model_hyperparameters,
            task_hyperparameters=task_hyperparameters,
            dataset=dataset)

    def __call__(self):
        try:
            self.configNode_client.update_model_state(self.model_id, TrainingState.RUNNING)
            self.pid_info[self.model_id] = os.getpid()
            self.trial.start()
            self.configNode_client.update_model_state(self.model_id, TrainingState.FINISHED, self.trial.trial_id)
        except Exception as e:
            logger.warn(e)
            self.configNode_client.update_model_state(self.model_id, TrainingState.FAILED, self.trial.trial_id)
            raise e


class ForestingTrainingObjective:
    """
    A class which serve as a function, should accept trial as args
    and return the optimization objective.
    Optuna will try to minimize the objective.
    Currently, user cannot define the range of hyperparameters.
    """

    def __init__(
            self,
            model_id: str,
            task_options: ForecastTaskOptions,
            hyperparameters: Dict[str, str],
            dataset: TsForecastDataset,
            pid_info: Dict
    ):
        self.task_options = task_options
        self.hyperparameters = hyperparameters
        self.dataset = dataset
        self.pid_info = pid_info
        self.model_id = model_id

    def __call__(self, optuna_suggest: optuna.Trial):
        model_hyperparameters, task_hyperparameters = generate_hyperparameters(optuna_suggest,
                                                                               self.task_options)
        trial = ForecastingTrainingTrial(
            trial_id=TRIAL_ID_PREFIX + str(optuna_suggest._trial_id),
            model_id=self.model_id,
            task_options=self.task_options,
            model_hyperparameters=model_hyperparameters,
            task_hyperparameters=task_hyperparameters,
            dataset=self.dataset)
        loss = trial.start()
        return loss


class ForecastAutoTuningTrainingTask(_BasicTrainingTask):
    def __init__(
            self,
            model_id: str,
            task_options: ForecastTaskOptions,
            hyperparameters: Dict[str, str],
            dataset: TsForecastDataset,
            pid_info: Dict
    ):
        super().__init__(model_id, hyperparameters, pid_info)
        self.study = optuna.create_study(direction='minimize')
        self.task_options = task_options
        self.dataset = dataset

    def __call__(self):
        self.pid_info[self.model_id] = os.getpid()
        try:
            self.configNode_client.update_model_state(self.model_id, TrainingState.RUNNING)
            self.study.optimize(ForestingTrainingObjective(
                model_id=self.model_id,
                task_options=self.task_options,
                hyperparameters=self.hyperparameters,
                dataset=self.dataset,
                pid_info=self.pid_info),
                n_trials=descriptor.get_config().get_mn_tuning_trial_num(),
                n_jobs=descriptor.get_config().get_mn_tuning_trial_concurrency())
            best_trial_id = TRIAL_ID_PREFIX + str(self.study.best_trial._trial_id)
            self.configNode_client.update_model_state(self.model_id, TrainingState.FINISHED, best_trial_id)
        except Exception as e:
            logger.warn(e)
            self.configNode_client.update_model_state(self.model_id, TrainingState.FAILED)
            raise e


# Inference Task

class _BasicInferenceTask(_BasicTask):
    def __init__(
            self,
            pid_info: Dict,
            data: Tuple,
            model_path: str
    ):
        super().__init__(pid_info)
        self.data = data
        self.model_path = model_path

    @abstractmethod
    def __call__(self, pipe: Connection = None):
        raise NotImplementedError

    @abstractmethod
    def data_align(self, *args):
        raise NotImplementedError


class ForecastingInferenceTask(_BasicInferenceTask):
    def __init__(
            self,
            predict_length: int,
            pid_info: Dict,
            data: Tuple,
            model_path: str
    ):
        super().__init__(pid_info, data, model_path)
        self.pred_len = predict_length

    def __call__(self, pipe: Connection = None):
        self.model, self.model_configs = model_storage.load_model(self.model_path)
        self.model_pred_len = self.model_configs[OptionsKey.PREDICT_LENGTH.name()]
        self.model_input_len = self.model_configs[OptionsKey.INPUT_LENGTH.name()]

        data, time_stamp = self.data
        _, c = data.shape

        time_stamp = pd.to_datetime(time_stamp.values[:, 0], unit='ms', utc=True) \
            .tz_convert('Asia/Shanghai')  # for iotdb
        data, time_stamp = self.data_align(data, time_stamp)
        full_data, full_data_stamp = data, time_stamp
        current_pred_len = 0
        while current_pred_len < self.pred_len:
            current_data = full_data[:, -self.model_input_len:, :]
            current_data = torch.Tensor(current_data)

            input_data = pack_input_dict(batch_x=current_data)
            output_data = self.model(input_data).detach().numpy()
            full_data = np.concatenate([full_data, output_data], axis=1)
            current_pred_len += self.model_pred_len
        full_data_stamp = self.generate_future_mark(full_data_stamp, self.pred_len)
        ret_data = pd.concat(
            [pd.DataFrame(full_data_stamp.astype(np.int64)),
             pd.DataFrame(full_data[0, -self.pred_len:, :]).astype(np.double)], axis=1)
        ret_data.columns = list(np.arange(0, c + 1))
        pipe.send(ret_data)

    def data_align(self, data: pd.DataFrame, data_stamp) -> Tuple[np.ndarray, pd.DataFrame]:
        """
        data: L x C, DataFrame, suppose no batch dim
        time_stamp: L x 1, DataFrame
        """
        data_stamp = pd.DataFrame(data_stamp)
        assert len(data.shape) == 2, 'expect inference data to have two dimensions'
        assert len(data_stamp.shape) == 2, 'expect inference timestamps to be shaped as [L, 1]'
        time_deltas = data_stamp.diff().dropna()
        mean_timedelta = time_deltas.mean()[0]
        data = data.values
        if data.shape[0] < self.model_input_len:
            extra_len = self.model_input_len - data.shape[0]
            data = np.concatenate([np.mean(data, axis=0, keepdims=True).repeat(extra_len, axis=0), data], axis=0)
            extrapolated_timestamp = pd.date_range(data_stamp[0][0] - extra_len * mean_timedelta, periods=extra_len,
                                                   freq=mean_timedelta)
            data_stamp = pd.concat([extrapolated_timestamp.to_frame(), data_stamp])
        else:
            data = data[-self.model_input_len:, :]
            data_stamp = data_stamp[-self.model_input_len:]
        data = data[None, :]  # add batch dim
        return data, data_stamp

    def generate_future_mark(self, time_stamp: pd.DataFrame, future_len: int) -> pd.DatetimeIndex:
        time_deltas = time_stamp.diff().dropna()
        mean_timedelta = time_deltas.mean()[0]
        extrapolated_timestamp = pd.date_range(time_stamp.values[-1][0] + mean_timedelta, periods=future_len,
                                               freq=mean_timedelta)
        return extrapolated_timestamp[:, None]
