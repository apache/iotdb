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
import multiprocessing
import os
from abc import abstractmethod
from typing import Dict

import optuna
from torch import nn
from torch.utils.data import Dataset

from iotdb.mlnode.log import logger
from iotdb.mlnode.process.trial import ForecastingTrainingTrial
from iotdb.mlnode.algorithm.factory import create_forecast_model
from iotdb.mlnode.client import client_manager
from iotdb.mlnode.config import descriptor
from iotdb.thrift.common.ttypes import TrainingState


class TrainingTrialObjective:
    """
    A class which serve as a function, should accept trial as args
    and return the optimization objective.
    Optuna will try to minimize the objective.
    """

    def __init__(self, trial_configs: Dict, model_configs: Dict, dataset: Dataset, pid_info: Dict):
        self.trial_configs = trial_configs
        self.model_configs = model_configs
        self.dataset = dataset
        self.pid_info = pid_info

    def __call__(self, trial: optuna.Trial):
        # TODO: decide which parameters to tune
        trial_configs = self.trial_configs
        trial_configs['learning_rate'] = trial.suggest_float("lr", 1e-7, 1e-1, log=True)
        trial_configs['trial_id'] = 'tid_' + str(trial._trial_id)
        # TODO: check args
        model, model_cfg = create_forecast_model(**self.model_configs)
        self.pid_info[self.trial_configs['model_id']][trial._trial_id] = os.getpid()
        _trial = ForecastingTrainingTrial(trial_configs, model, self.model_configs, self.dataset)
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
        model: nn.Module,
        dataset: Dataset,
        pid_info: Dict
    ):
        """
        Args:
            task_configs:
            model_configs:
            model:
            dataset:
            pid_info:
        """
        self.pid_info = pid_info
        self.task_configs = task_configs
        self.model_configs = model_configs
        self.model = model
        self.dataset = dataset

    @abstractmethod
    def __call__(self):
        raise NotImplementedError


class ForecastingTrainingTask(_BasicTask):
    def __init__(self, task_configs: Dict, model_configs: Dict, model: nn.Module, dataset: Dataset,
                 pid_info: Dict):
        """
        Args:
            task_configs: dict of task configurations
            model_configs: dict of model configurations
            model: nn.Module
            dataset: training dataset
            pid_info: a map shared between processes, can be used to find the pid with model_id and trial_id
        """
        super(ForecastingTrainingTask, self).__init__(task_configs, model_configs, model, dataset, pid_info)
        self.model_id = self.task_configs['model_id']
        self.tuning = self.task_configs['tuning']
        self.confignode_client = client_manager.borrow_config_node_client()

        if self.tuning:
            self.study = optuna.create_study(direction='minimize')
        else:
            self.default_trial_id = 'tid_0'
            self.task_configs['trial_id'] = self.default_trial_id
            self.trial = ForecastingTrainingTrial(self.task_configs, self.model, self.model_configs, self.dataset)
            self.pid_info[self.model_id][self.default_trial_id] = os.getpid()

    def __call__(self):
        try:
            if self.tuning:
                self.study.optimize(TrainingTrialObjective(
                    self.task_configs,
                    self.model_configs,
                    self.dataset,
                    self.pid_info),
                    n_trials=descriptor.get_config().get_mn_tuning_trial_num(),
                    n_jobs=descriptor.get_config().get_mn_tuning_trial_concurrency())
                best_trial_id = 'tid_' + str(self.study.best_trial._trial_id)
                self.confignode_client.update_model_state(self.model_id, TrainingState.FINISHED, best_trial_id)
            else:
                self.trial.start()
                self.confignode_client.update_model_state(self.model_id, TrainingState.FINISHED, self.default_trial_id)
        except Exception as e:
            logger.warn(e)
            raise e
