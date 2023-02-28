import threading
import shutil
import os
import torch


class ModelInfo(object):
    def __init__(self, root_path, model_id, capacity=10):
        self.root_path = root_path
        self.model_id = model_id
        self.capacity = capacity
        self.model_dir = self.root_path + '/' + str(model_id)
        self.best_trail_dir = self.model_dir + str(0)
        self.lock = threading.Semaphore(capacity)
        if not os.path.exists(self.model_dir):
            os.mkdir(self.model_dir)

    def exist_model(self):
        return os.path.exists(self.model_dir)

    def save_model(self, model, trail_id, updateBestTrail):
        trail_dir = self.model_dir + '/' + str(trail_id)
        if not os.path.exists(trail_dir):
            os.mkdir(trail_dir)
        torch.save(model, trail_dir + '/' + 'checkpoint.pth')
        if updateBestTrail:
            self.best_trail_dir = trail_dir
        print("Model Saved Successfully in " + trail_dir)

    def load_best_model(self):
        if not self.exist_model():
            print("Model not exist")
            return None
        else:
            self.lock.acquire()
            model = torch.load(self.best_trail_dir + '/' + 'checkpoint.pth')
            print('Load ' + str(self.model_id) + ' Successfully')
            self.lock.release()
            return model

    def delete_model(self):
        count = 0
        while count < self.capacity:
            try:
                self.lock.acquire()
                count += 1
            except:
                pass
        shutil.rmtree(self.model_dir)
        print('Delete ' + str(self.model_id) + ' Successfully')
        return self.exist_model()
