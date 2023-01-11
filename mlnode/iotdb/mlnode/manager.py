import multiprocessing as mp
import time
from task import ForecastingTrainingTask
from data_provider.build_dataset_debug import *

def _create_training_task(configs):
    print('当前进程:%s pid:%d' % (mp.current_process().name,
                              mp.current_process().pid))
    task = ForecastingTrainingTask(configs)
    return task.start()

class Manager(object):
    def __init__(self, processes):
        self.pool = mp.Pool(processes=processes)
    
    def _create_training_task(self, configs):
        task = ForecastingTrainingTask(configs)
        task.start()

    def createTrainingTask(self, configs):
        self.pool.apply_async(_create_training_task, args=(configs,))

    def shutdown(self):
        self.pool.close()
        self.pool.join()
    
    
if __name__ == '__main__':
    mp.set_start_method('spawn')
    manager = Manager(5)
    configs = debug_configs()
    manager.createTrainingTask(configs)
    manager.shutdown()
