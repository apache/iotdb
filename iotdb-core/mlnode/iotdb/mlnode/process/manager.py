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

import multiprocessing as mp
import signal
import sys
from subprocess import call
from typing import Dict

import pandas as pd
import psutil

from iotdb.mlnode.das.dataset import TsDataset
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import ForecastTaskOptions
from iotdb.mlnode.process.task import (ForecastAutoTuningTrainingTask,
                                       ForecastFixedParamTrainingTask,
                                       ForecastingInferenceTask,
                                       _BasicTrainingTask)


class TaskManager(object):
    def __init__(self, pool_size: int):
        """
        Args:
            pool_size: specify the maximum process number of the process pool

        __shared_resource_manager: a manager that manage resources shared between processes
        __pid_info: a map shared between processes, can be used to find the pid with model_id and trial_id
        __training_process_pool: a multiprocessing process pool
        """
        self.__shared_resource_manager = mp.Manager()
        self.__pid_info = self.__shared_resource_manager.dict()
        self.__training_process_pool = mp.Pool(pool_size)
        self.__inference_process_pool = mp.Pool(pool_size)

    def create_forecast_training_task(self,
                                      model_id: str,
                                      task_options: ForecastTaskOptions,
                                      hyperparameters: Dict[str, str],
                                      dataset: TsDataset):
        """
        Create a training task for forecasting, which contains the training process

        Args:
            model_id: the unique id of the model
            task_options: the options of the task, contains task_type, model_type, etc.
            dataset: a torch dataset to be used for training
            hyperparameters: a dict of hyperparameters, which is consisted of model_hyperparameters
            and task_hyperparameters

        Returns:
            task: a training task for forecasting, which can be submitted to self.__training_process_pool
        """
        if task_options.auto_tuning:
            return ForecastAutoTuningTrainingTask(
                model_id=model_id,
                task_options=task_options,
                hyperparameters=hyperparameters,
                dataset=dataset,
                pid_info=self.__pid_info,
            )
        else:
            return ForecastFixedParamTrainingTask(
                model_id=model_id,
                task_options=task_options,
                hyperparameters=hyperparameters,
                dataset=dataset,
                pid_info=self.__pid_info,
            )

    def submit_training_task(self, task: _BasicTrainingTask) -> None:
        self.__training_process_pool.apply_async(task, args=())
        logger.info(f'Task: ({task.model_id}) - Training process submitted successfully')

    def create_forecast_task(self,
                             predict_length,
                             data,
                             model_path) -> ForecastingInferenceTask:
        task = ForecastingInferenceTask(
            predict_length,
            self.__pid_info,
            data,
            model_path
        )
        return task

    def submit_forecast_task(self, task: ForecastingInferenceTask) -> pd.DataFrame:
        read_pipe, send_pipe = mp.Pipe()
        if task is not None:
            self.__inference_process_pool.apply_async(task, args=(send_pipe,))
            logger.info('Forecasting process submitted successfully')
        return read_pipe.recv()

    def kill_task(self, model_id):
        """
        Kill the process by pid, will check whether the pid is training or inference process
        """
        pid = self.__pid_info[model_id]
        if sys.platform == 'win32':
            try:
                process = psutil.Process(pid=pid)
                process.send_signal(signal.CTRL_BREAK_EVENT)
            except psutil.NoSuchProcess:
                print(f'Tried to kill process (pid = {pid}), '
                      f'but the process does not exist.')
        else:
            cmds = ['kill', str(pid)]
            call(cmds)
