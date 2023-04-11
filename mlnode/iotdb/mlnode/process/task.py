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

from iotdb.mlnode.log import logger
from iotdb.mlnode.process.trial import ForecastingTrainingTrial


class _BasicTask(object):
    """
    This class serve as a function, accepting configs and launch trials
    according to the configs.
    """

    def __init__(self, task_configs, model_configs, model, dataset, task_trial_map):
        self.task_trial_map = task_trial_map
        self.task_configs = task_configs
        self.model_configs = model_configs
        self.model = model
        self.dataset = dataset

    @abstractmethod
    def __call__(self):
        raise NotImplementedError


class ForecastingTrainingTask(_BasicTask):
    def __init__(self, task_configs, model_configs, model, dataset, trial_pid_info):
        super(ForecastingTrainingTask, self).__init__(task_configs, model_configs, model, dataset, trial_pid_info)
        self.model_id = self.task_configs['model_id']

        self.task_configs['trial_id'] = 'tid_0'
        self.trial = ForecastingTrainingTrial(self.task_configs, self.model, self.model_configs, self.dataset)
        self.task_trial_map[self.model_id]['tid_0'] = os.getpid()

    def __call__(self):
        try:
            self.trial.start()
        except Exception as e:
            logger.warn(e)
            raise e
