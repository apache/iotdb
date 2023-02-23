import multiprocessing as mp

from subprocess import call
from trial import ForecastingTrainingTrial, ForecastingInferenceTrial
from data_provider.build_dataset_debug import *

import psutil
import sys
import os
import time
import signal
import optuna

class Objective:
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
    study.optimize(Objective(configs), n_trials=20)
    pid = os.getpid()
    task_map[task_id][pid] = 'finished'

def _create_inference_task(configs, task_map, task_id):
    trial = ForecastingInferenceTrial(configs, debug_inference_data())
    trial.start()

class Manager(object):
    def __init__(self, pool_num):
        self.resource_manager = mp.Manager()
        self.task_map = self.resource_manager.dict()
        signal.signal(signal.SIGCHLD, signal.SIG_IGN) # leave to the os to clean up zombie processes
        self.pool = mp.Pool(pool_num)

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance
    
    def createSingleTrainingTask_Pool(self, configs):
        task_id = self.generate_taskid()
        self.pool.apply_async(_create_training_task, args=(configs, self.task_map, task_id, ))
        self.task_map[task_id] = self.resource_manager.dict()
        return task_id
    
    def createTuneTrainingTask_Pool(self, configs):
        task_id = self.generate_taskid()
        self.pool.apply_async(_create_tunning_task, args=(configs, self.task_map, task_id, ))
        self.task_map[task_id] = self.resource_manager.dict()
        return task_id
    
    def createInferenceTask_Pool(self, configs):
        task_id = self.generate_taskid()
        self.pool.apply_async(_create_inference_task, args=(configs, self.task_map, task_id, ))
        self.task_map[task_id] = self.resource_manager.dict()
        return task_id

    def createSingleTrainingTask(self, configs):
        task_id = self.generate_taskid()
        result = mp.Process(target=_create_training_task,args=(configs, self.task_map, task_id, ), )
        result.start()
        time.sleep(0.1)

        self.task_map[task_id] = self.resource_manager.dict()
        if result.is_alive():
            self.task_map[task_id][result.pid] = 'running'
        else:
            self.task_map[task_id][result.pid] = 'failed'
        print(self.task_map[task_id])
        return task_id
    
    def createTuneTrainingTask(self, configs):
        task_id = self.generate_taskid()
        result = mp.Process(target=_create_tunning_task,args=(configs, self.task_map, task_id, ), )
        result.start()
        time.sleep(0.1)

        self.task_map[task_id] = self.resource_manager.dict()
        if result.is_alive():
            self.task_map[task_id][result.pid] = 'running'
        else:
            self.task_map[task_id][result.pid] = 'failed'
        print(self.task_map[task_id])
        return task_id

    def kill_process(self, pid):
        if sys.platform == 'win32':
            try:
                process = psutil.Process(pid=pid)
                process.send_signal(signal.CTRL_BREAK_EVENT)
            except psutil.NoSuchProcess:
                print(f'Tried to kill process (pid = {pid}), but the process does not exist.')
        else:
            cmds = ['kill', str(pid)]
            call(cmds)
    
    def generate_taskid(self):
        return str(int(time.time() * 1000000))

    def get_task_state(self, task_id):
        return self.task_map[task_id]

    def update_process_state(self):
        for task_id in self.task_map.keys():
            for pid in self.task_map[task_id].keys():
                if (not psutil.pid_exists(pid)) and self.task_map[task_id][pid] == 'running':
                    self.task_map[task_id][pid] = 'failed'
        

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




