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

from iotdb.mlnode.log import logger
from iotdb.mlnode.process.task_factory import create_task


class TaskManager(object):
    def __init__(self, pool_num):
        """
        resource_manager: a manager that manage resources shared between processes
        task_map: a map shared between processes and storing the tasks' states
        pool: a multiprocessing process pool
        """
        self.__shared_resource_manager = mp.Manager()
        self.__pid_info = self.__shared_resource_manager.dict()
        self.__training_process_pool = mp.Pool(pool_num)

    def submit_training_task(self, task_configs, model_configs, model, dataset):
        assert 'model_id' in task_configs.keys(), 'Task config should contain model_id'
        model_id = task_configs['model_id']
        self.__pid_info[model_id] = self.__shared_resource_manager.dict()
        try:
            task = create_task(
                task_configs,
                model_configs,
                model,
                dataset,
                self.__pid_info
            )
        except Exception as e:
            logger.exception(e)
            return e, False

        logger.info(f'Task: ({model_id}) - Training process submitted successfully')
        self.__training_process_pool.apply_async(task, args=())
        return model_id, True
