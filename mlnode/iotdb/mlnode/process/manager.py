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
import pandas as pd

from typing import Dict, Union
from torch.utils.data import Dataset
from iotdb.mlnode.log import logger
from iotdb.mlnode.process.task import ForecastingSingleTrainingTask, ForecastingTuningTrainingTask, \
    ForecastingInferenceTask


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
        # self.__inference_process_pool = mp.Pool(pool_size)

    def create_training_task(self,
                             dataset: Dataset,
                             data_configs: Dict,
                             model_configs: Dict,
                             task_configs: Dict):
        """

        Args:
            dataset: a torch dataset to be used for training
            data_configs: dict of data configurations
            model_configs: dict of model configurations
            task_configs: dict of task configurations

        Returns:
            task: a training task for forecasting, which can be submitted to self.__training_process_pool
        """
        model_id = task_configs['model_id']
        self.__pid_info[model_id] = self.__shared_resource_manager.dict()
        if task_configs['tuning']:
            task = ForecastingTuningTrainingTask(
                task_configs,
                model_configs,
                self.__pid_info,
                data_configs,
                dataset,
                model_id,
            )
        else:
            task = ForecastingSingleTrainingTask(
                task_configs,
                model_configs,
                self.__pid_info,
                data_configs,
                dataset,
                model_id,
            )
        return task

    def submit_training_task(self, task: Union[ForecastingTuningTrainingTask, ForecastingSingleTrainingTask]) -> None:
        if task is not None:
            self.__training_process_pool.apply_async(task, args=())
            logger.info(f'Task: ({task.model_id}) - Training process submitted successfully')

    def create_forecast_task(self,
                             task_configs,
                             model_configs,
                             data,
                             model_path,
                             model_id) -> ForecastingInferenceTask:
        task = ForecastingInferenceTask(
            task_configs,
            model_configs,
            self.__pid_info,
            data,
            model_path,
            model_id
        )
        return task

    def submit_forecast_task(self, task: ForecastingInferenceTask) -> pd.DataFrame:
        read_pipe, send_pipe = mp.Pipe()
        if task is not None:
            self.__training_process_pool.apply_async(task, args=(send_pipe,))
            logger.info(f'Forecasting process submitted successfully')
            # task(send_pipe)
        return read_pipe.recv()
