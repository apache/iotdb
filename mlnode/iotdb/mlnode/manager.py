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
import signal
import psutil
import multiprocessing as mp
from subprocess import call
from iotdb.mlnode.process.task import ForecastingTrainingTask

# def _create_inference_task(configs, task_map, task_id):
#     trial = ForecastingInferenceTrial(configs, debug_inference_data())
#     trial.start()


class Manager(object):
    def __init__(self, pool_num):
        """
        resource_manager: a manager that manage resources shared between processes
        task_map: a map shared between processes and storing the tasks' states #TODO: trial_id-pid
        pool: a multiprocessing process pool
        """
        self.resource_manager = mp.Manager()
        self.task_trial_map = self.resource_manager.dict()
        # signal.signal(signal.SIGCHLD, signal.SIG_IGN)  # leave to the os to clean up zombie processes
        self.training_pool = mp.Pool(pool_num)

    def submit_training_task(self, data_configs, model_configs, task_configs):
        model_id = task_configs['model_id']
        self.task_trial_map[model_id] = self.resource_manager.dict()
        self.training_pool.apply_async(
            ForecastingTrainingTask(
                task_configs,
                model_configs,
                data_configs,
                self.task_trial_map
            ), args=()
        )
        self.training_pool.close()
        self.training_pool.join()

    # def create_inference_task_pool(self, configs):
    #     """
    #     Create an inference based on configs; will add the inference process to the pool
    #     """
    #     task_id = self.generate_taskid()
    #     self.pool.apply_async(_create_inference_task, args=(configs, self.task_map, task_id,))
    #     self.task_map[task_id] = self.resource_manager.dict()
    #     return task_id

    def kill_process(self, pid):
        """
        Kill the process by pid
        """
        if sys.platform == 'win32':
            try:
                process = psutil.Process(pid=pid)
                process.send_signal(signal.CTRL_BREAK_EVENT)
            except psutil.NoSuchProcess:
                print(f'Tried to kill process (pid = {pid}), but the process does not exist.')
        else:
            cmds = ['kill', str(pid)]
            call(cmds)

    # def _generate_taskid(self, model_id):
    #     """
    #     Generate a unique task id
    #     """
    #     return model_id + '_' + str(int(time.time() * 1000000))

    # def _get_task_state(self, task_id):
    #     return self.task_map[task_id]


# TaskManager = Manager(10)
