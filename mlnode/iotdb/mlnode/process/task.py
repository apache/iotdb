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

import sys
import os
import time
import signal
import optuna
import psutil
import multiprocessing as mp
from iotdb.mlnode.process.trial import ForecastingTrainingTrial
from iotdb.mlnode.algorithm.model_factory import create_forecast_model
from iotdb.mlnode.datats.data_factory import create_forecasting_dataset
from iotdb.mlnode.datats.offline.data_source import DataSource


class TrainingTrialObjective:
    """
    A class which serve as a function, should accept trial as args
    and return the optimization objective.
    Optuna will try to minimize the objective.
    """

    def __init__(self, trial_configs, model_configs, data_configs):
        self.trial_configs = trial_configs
        self.model_configs = model_configs
        self.data_configs = data_configs

    def __call__(self, trial: optuna.Trial):
        # TODO: decide which parameters to tune
        trial_configs = self.trial_configs
        trial_configs['learning_rate'] = trial.suggest_float("lr", 1e-5, 1e-1, log=True)

        # TODO: check args
        model, model_cfg = create_forecast_model(**self.model_configs)
        dataset, dataset_cfg = create_forecasting_dataset(**self.data_configs)

        trial = ForecastingTrainingTrial(self.trial_configs, model, self.model_configs, dataset)
        loss = trial.start()
        return loss


class BasicTask(object):
    """
    This class serve as a function, accepting configs and launch trials
    according to the configs.
    """
    def __init__(self, task_configs, model_configs, data_configs, task_trial_map):
        self.task_trial_map = task_trial_map
        self.task_configs = task_configs
        self.model_configs = model_configs
        self.data_configs = data_configs

    def __call__(self):
        raise NotImplementedError


class ForecastingTrainingTask(BasicTask):
    def __init__(self, task_configs, model_configs, data_configs, task_trial_map):
        super(ForecastingTrainingTask, self).__init__(task_configs, model_configs, data_configs, task_trial_map)

    def __call__(self):
        tuning = self.task_configs["tuning"]
        if tuning:
            study = optuna.create_study(direction='minimize')
            study.optimize(TrainingTrialObjective(
                self.task_configs,
                self.model_configs,
                self.data_configs
            ), n_trials=20)
        else:
            #TODO: use logger, with more meaningful informations
            print('call the task')
            model, model_cfg = create_forecast_model(**self.model_configs)
            print('model created')
            datasource = DataSource(**self.data_configs)
            print('datasource created')
            dataset, dataset_cfg = create_forecasting_dataset(
                data_source=datasource,
                **self.data_configs)
            print('data created')
            self.task_configs['trial_id'] = 0
            trial = ForecastingTrainingTrial(self.task_configs, model, self.model_configs, dataset)
            loss = trial.start()
