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

from subprocess import call
from mlnode.iotdb.mlnode.process.trial import ForecastingTrainingTrial, ForecastingInferenceTrial
from datafactory.build_dataset_debug import *

import psutil
import sys
import os
import time
import signal
import optuna

class TrainingTrialObjective:
    """
    A class which serve as a function, should accept trial as args
    and return the optimization objective. 
    Optuna will try to minimize the objective.
    """
    def __init__(self, configs):
        self.configs = configs
    
    def __call__(self, trial: optuna.Trial):
        configs = self.configs
        configs.learning_rate = trial.suggest_float("lr", 1e-5, 1e-1, log=True)
        configs.d_model = trial.suggest_categorical("d_model", [32, 64, 128, 256, 512, 768])
        task = ForecastingTrainingTrial(configs)
        loss = task.start()
        return loss

def _create_training_task(configs, task_map, task_id):
    trial = ForecastingTrainingTrial(configs)
    trial.start() 
    pid = os.getpid()
    task_map[task_id][pid] = 'finished'

def _create_tunning_task(configs, task_map, task_id):
    study = optuna.create_study(direction='minimize')
    study.optimize(TrainingTrialObjective(configs), n_trials=20)
    pid = os.getpid()
    task_map[task_id][trial_id] = pid

def _create_inference_task(configs, task_map, task_id):
    trial = ForecastingInferenceTrial(configs, debug_inference_data())
    trial.start()

class Manager(object):
    def __init__(self, pool_num):
        """
        resource_manager: a manager that manage resources shared between processes
        task_map: a map shared between processes and storing the tasks' states #TODO: trial_id-pid
        pool: a multiprocessing process pool
        """
        self.resource_manager = mp.Manager()
        self.task_map = self.resource_manager.dict()
        signal.signal(signal.SIGCHLD, signal.SIG_IGN) # leave to the os to clean up zombie processes
        self.pool = mp.Pool(pool_num)

    def submit_single_training_task(self, configs):
        """
        Create a single training task based on configs; will add a process to the pool
        # TODO: extract code pieces
        """
        task_id = self.generate_taskid()
        self.pool.apply_async(_create_training_task, args=(configs, self.task_map, task_id, ))
        self.task_map[task_id] = self.resource_manager.dict()
        return task_id
    
    def create_tune_training_task_pool(self, configs):
        """
        Create a tuning task based on configs; will add a optuna process to the pool
        """
        task_id = self.generate_taskid()
        self.pool.apply_async(_create_tunning_task, args=(configs, self.task_map, task_id, ))
        self.task_map[task_id] = self.resource_manager.dict()
        return task_id
    
    def create_inference_task_pool(self, configs):
        """
        Create an inference based on configs; will add the inference process to the pool
        """
        task_id = self.generate_taskid()
        self.pool.apply_async(_create_inference_task, args=(configs, self.task_map, task_id, ))
        self.task_map[task_id] = self.resource_manager.dict()
        return task_id

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
    
    def _generate_taskid(self):
        """
        Generate a unique task id
        """
        return str(int(time.time() * 1000000))

    def _get_task_state(self, task_id):
        return self.task_map[task_id]
        

# TaskManager = Manager(10)

if __name__ == '__main__':
    # manager = Manager()
    # configs = default_configs()
    # task_id = manager.createTuneTrainingTask(configs)

    # while True:
    #     time.sleep(5)
    #     manager.update_process_state()
    #     print(manager.task_map[task_id])
    pass




