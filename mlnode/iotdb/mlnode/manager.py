import multiprocessing as mp

from subprocess import call
from task import ForecastingTrainingTask
from data_provider.build_dataset_debug import *

import psutil
import sys
import time
import signal

def _create_training_task(configs, task_map, task_id):
    task = ForecastingTrainingTask(configs, task_map, task_id)
    task.start() 

class Manager(object):
    def __init__(self):
        self.resource_manager = mp.Manager()
        self.task_map = self.resource_manager.dict()
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    
    def _create_training_task(self, configs):
        print("creating...")
        task = ForecastingTrainingTask(configs)
        task.start()

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
        raise NotImplementedError
    
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


    def update_process_state(self):
        for task_id in self.task_map.keys():
            for pid in self.task_map[task_id].keys():
                if (not psutil.pid_exists(pid)) and self.task_map[task_id][pid] == 'running':
                    self.task_map[task_id][pid] = 'failed'
        
    
if __name__ == '__main__':
    mp.set_start_method('spawn')
    manager = Manager()
    configs = debug_configs()
    task_id = manager.createTrainingTask(configs)

    while True:
        time.sleep(2)
        manager.update_process_state()
        print(manager.task_map[task_id])




