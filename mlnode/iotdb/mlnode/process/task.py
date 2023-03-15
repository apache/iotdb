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
import optuna
from iotdb.mlnode.process.trial import ForecastingTrainingTrial
from iotdb.mlnode.algorithm.model_factory import create_forecast_model
from iotdb.mlnode.datats.data_factory import create_forecasting_dataset
from iotdb.mlnode.datats.offline.data_source import DataSource
from iotdb.mlnode.log import logger


class TrainingTrialObjective:
    """
    A class which serve as a function, should accept trial as args
    and return the optimization objective.
    Optuna will try to minimize the objective.
    """

    def __init__(self, trial_configs, model_configs, data_configs, task_trial_map):
        self.trial_configs = trial_configs
        self.model_configs = model_configs
        self.data_configs = data_configs
        self.task_trial_map = task_trial_map

    def __call__(self, trial: optuna.Trial):
        # TODO: decide which parameters to tune
        trial_configs = self.trial_configs
        trial_configs['learning_rate'] = trial.suggest_float("lr", 1e-5, 1e-1, log=True)

        # TODO: check args
        model, model_cfg = create_forecast_model(**self.model_configs)
        dataset, dataset_cfg = create_forecasting_dataset(**self.data_configs)

        self.task_trial_map[self.trial_configs['model_id']][trial.trial_id] = os.getpid()
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
        super(ForecastingTrainingTask, self).__init__ \
            (task_configs, model_configs, data_configs, task_trial_map)
        model_id = self.task_configs['model_id']
        self.tuning = self.task_configs["tuning"]

        if self.tuning:             # TODO implement tuning task
            self.study = optuna.create_study(direction='minimize')
        else:
            model, model_cfg = create_forecast_model(**self.model_configs)
            logger.info(f'Task: ({model_id}) - Model created')
            datasource = DataSource(**self.data_configs)
            logger.info(f'Task: ({model_id}) - Data source created')
            dataset, dataset_cfg = create_forecasting_dataset(
                data_source=datasource,
                **self.data_configs)
            logger.info(f'Task: ({model_id}) - Data loader created')
            self.task_configs['trial_id'] = 'tid_0'  # TODO: set a default trial id
            self.trial = ForecastingTrainingTrial(self.task_configs, model, self.model_configs, dataset)

            self.task_trial_map[self.task_configs['model_id']][0] = os.getpid()

    def __call__(self):
        try:
            if self.tuning:
                self.study.optimize(TrainingTrialObjective(
                    self.task_configs,  # TODO: How to generate diff trial_id by optuna
                    self.model_configs,
                    self.data_configs,
                    self.task_trial_map
                ), n_trials=20)
            else:
                loss = self.trial.start()
        except Exception as e:
            logger.exception(e)
