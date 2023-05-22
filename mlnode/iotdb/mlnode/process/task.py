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
from torch.utils.data import Dataset

from iotdb.mlnode.algorithm.factory import create_forecast_model
from iotdb.mlnode.client import client_manager
from iotdb.mlnode.config import descriptor
from iotdb.mlnode.log import logger
from iotdb.mlnode.process.trial import ForecastingTrainingTrial
from iotdb.mlnode.storage import model_storage
from iotdb.thrift.common.ttypes import TrainingState


class ForestingTrainingObjective:
    """
    A class which serve as a function, should accept trial as args
    and return the optimization objective.
    Optuna will try to minimize the objective.
    """

    def __init__(
            self,
            trial_configs: Dict,
            model_configs: Dict,
            dataset: Dataset,
            # pid_info: Dict
    ):
        self.trial_configs = trial_configs
        self.model_configs = model_configs
        self.dataset = dataset
        # self.pid_info = pid_info

    def __call__(self, trial: optuna.Trial):
        # TODO: decide which parameters to tune
        trial_configs = self.trial_configs
        trial_configs['learning_rate'] = trial.suggest_float("lr", 1e-7, 1e-1, log=True)
        trial_configs['trial_id'] = 'tid_' + str(trial._trial_id)
        # TODO: check args
        model, model_configs = create_forecast_model(**self.model_configs)
        # self.pid_info[self.trial_configs['model_id']][trial._trial_id] = os.getpid()
        _trial = ForecastingTrainingTrial(trial_configs, model, model_configs, self.dataset)
        loss = _trial.start()
        return loss


class _BasicTask(object):
    """
    This class serve as a function, accepting configs and launch trials
    according to the configs.
    """

    def __init__(
            self,
            task_configs: Dict,
            model_configs: Dict,
            pid_info: Dict
    ):
        """
        Args:
            task_configs:
            model_configs:
            pid_info:
        """
        self.pid_info = pid_info
        self.task_configs = task_configs
        self.model_configs = model_configs

    @abstractmethod
    def __call__(self):
        raise NotImplementedError


class _BasicTrainingTask(_BasicTask):
    def __init__(
            self,
            task_configs: Dict,
            model_configs: Dict,
            pid_info: Dict,
            data_configs: Dict,
            dataset: Dataset,
    ):
        """
        Args:
            task_configs:
            model_configs:
            pid_info:
            data_configs:
            dataset:
        """
        super().__init__(task_configs, model_configs, pid_info)
        self.data_configs = data_configs
        self.dataset = dataset
        self.confignode_client = client_manager.borrow_config_node_client()

    @abstractmethod
    def __call__(self):
        raise NotImplementedError


class _BasicInferenceTask(_BasicTask):
    def __init__(
            self,
            task_configs: Dict,
            model_configs: Dict,
            pid_info: Dict,
            data: Tuple,
            model_path: str
    ):
        """
        Args:
            task_configs:
            model_configs:
            pid_info:
            data:
            model:
        """
        super().__init__(task_configs, model_configs, pid_info)
        self.data = data
        self.input_len = self.model_configs['input_len']
        self.model_path = model_path

    @abstractmethod
    def __call__(self, pipe: Connection = None):
        raise NotImplementedError

    @abstractmethod
    def data_align(self, *args):
        raise NotImplementedError


class ForecastingSingleTrainingTask(_BasicTrainingTask):
    def __init__(
            self,
            task_configs: Dict,
            model_configs: Dict,
            pid_info: Dict,
            data_configs: Dict,
            dataset: Dataset,
            model_id: str,
    ):
        """
        Args:
            task_configs: dict of task configurations
            model_configs: dict of model configurations
            pid_info: a map shared between processes, can be used to find the pid with model_id and trial_id
            data_configs: dict of data configurations
            dataset: training dataset
        """
        super().__init__(task_configs, model_configs, pid_info, data_configs, dataset)
        self.model_id = model_id
        self.default_trial_id = 'tid_0'
        self.task_configs['trial_id'] = self.default_trial_id
        model, model_configs = create_forecast_model(**model_configs)
        self.trial = ForecastingTrainingTrial(task_configs, model, model_configs, dataset)
        self.pid_info[self.model_id][self.default_trial_id] = os.getpid()

    def __call__(self):
        try:
            self.trial.start()
            self.confignode_client.update_model_state(self.model_id, TrainingState.FINISHED, self.default_trial_id)
        except Exception as e:
            logger.warn(e)
            raise e


class ForecastingTuningTrainingTask(_BasicTrainingTask):
    def __init__(
            self,
            task_configs: Dict,
            model_configs: Dict,
            pid_info: Dict,
            data_configs: Dict,
            dataset: Dataset,
            model_id: str,
    ):
        """
        Args:
            task_configs: dict of task configurations
            model_configs: dict of model configurations
            pid_info: a map shared between processes, can be used to find the pid with model_id and trial_id
            data_configs: dict of data configurations
            dataset: training dataset
        """
        super().__init__(task_configs, model_configs, pid_info, data_configs, dataset)
        self.model_id = model_id
        self.study = optuna.create_study(direction='minimize')

    def __call__(self):
        self.study.optimize(ForestingTrainingObjective(
            self.task_configs,
            self.model_configs,
            self.dataset),
            n_trials=descriptor.get_config().get_mn_tuning_trial_num(),
            n_jobs=descriptor.get_config().get_mn_tuning_trial_concurrency())
        best_trial_id = 'tid_' + str(self.study.best_trial._trial_id)
        self.confignode_client.update_model_state(self.model_id, TrainingState.FINISHED, best_trial_id)


class ForecastingInferenceTask(_BasicInferenceTask):
    def __init__(
            self,
            task_configs: Dict,
            model_configs: Dict,
            pid_info: Dict,
            data: Tuple,
            model_path: str
    ):
        super().__init__(task_configs, model_configs, pid_info, data, model_path)
        self.pred_len = self.task_configs['pred_len']
        self.model_pred_len = self.model_configs['pred_len']

    def __call__(self, pipe: Connection = None):
        self.model, _ = model_storage.load_model(self.model_path)
        data, time_stamp = self.data
        L, C = data.shape
        time_stamp = pd.to_datetime(time_stamp.values[:, 0], unit='ms', utc=True) \
            .tz_convert('Asia/Shanghai')  # for iotdb
        data, time_stamp = self.data_align(data, time_stamp)
        full_data, full_data_stamp = data, time_stamp
        current_pred_len = 0
        while current_pred_len < self.pred_len:
            current_data = full_data[:, -self.input_len:, :]
            current_data = torch.Tensor(current_data)
            output_data = self.model(current_data).detach().numpy()
            full_data = np.concatenate([full_data, output_data], axis=1)
            current_pred_len += self.model_pred_len
        full_data_stamp = self.generate_future_mark(full_data_stamp, self.pred_len)
        ret_data = pd.concat(
            [pd.DataFrame(full_data_stamp.astype(np.int64)),
             pd.DataFrame(full_data[0, -self.pred_len:, :]).astype(np.double)], axis=1)
        ret_data.columns = list(np.arange(0, C + 1))
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
        if data.shape[0] < self.input_len:
            extra_len = self.input_len - data.shape[0]
            data = np.concatenate([np.mean(data, axis=0, keepdims=True).repeat(extra_len, axis=0), data], axis=0)
            extrapolated_timestamp = pd.date_range(data_stamp[0][0] - extra_len * mean_timedelta, periods=extra_len,
                                                   freq=mean_timedelta)
            data_stamp = pd.concat([extrapolated_timestamp.to_frame(), data_stamp])
        else:
            data = data[-self.input_len:, :]
            data_stamp = data_stamp[-self.input_len:]
        data = data[None, :]  # add batch dim
        return data, data_stamp

    def generate_future_mark(self, data_stamp: pd.DataFrame, future_len: int) -> pd.DatetimeIndex:
        time_deltas = data_stamp.diff().dropna()
        mean_timedelta = time_deltas.mean()[0]
        extrapolated_timestamp = pd.date_range(data_stamp.values[0][0], periods=future_len,
                                               freq=mean_timedelta)
        return extrapolated_timestamp[:, None]
