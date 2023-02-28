import torchvision
import os

from ModelInfo import ModelInfo
from ThreadWithReturn import ThreadWithReturn

from pylru import lrucache

class ModelManger(object):
    def __init__(self, root_path='checkpoints', cache_size=10):
        self.root_path = root_path
        if not os.path.exists(root_path):
            os.mkdir(root_path)
        self.ModelMap = {}
        self.lastModelID = None
        self.lastModel = None
        # store best trial of models
        self.model_cache = lrucache(cache_size)

    def get_model_info_by_id(self, model_id):
        return self.ModelMap.get(model_id)

    def register_model(self, model_id):
        modelinfo = ModelInfo(root_path=self.root_path, model_id=model_id)
        self.ModelMap[model_id] = modelinfo
        return modelinfo

    def delete_model_by_id(self, model_id):
        modelinfo = self.get_model_info_by_id(model_id)
        thread = ThreadWithReturn(func=modelinfo.deleteModel, args=())
        thread.start()
        thread.join()
        self.ModelMap.pop(model_id)
        # delete model in cache
        self.model_cache.pop(model_id)

    def load_best_model_by_id(self, model_id):
        # if model in cache, return directly
        if model_id in self.model_cache:
            return self.model_cache[model_id]
        modelinfo = self.get_model_info_by_id(model_id)
        thread = ThreadWithReturn(func=modelinfo.loadBestModel, args=())
        thread.start()
        thread.join()
        return thread.get_result()

    def save_model(self, model, model_id, trail_id, updateBestTrail=True):
        if not model_id in self.ModelMap:
            modelinfo = self.register_model(model_id)
        else:
            modelinfo = self.get_model_info_by_id(model_id)
        modelinfo.save_model(model, trail_id, updateBestTrail)
        # update best trial in cache
        if updateBestTrail:
            self.model_cache[model_id] = model


modelManager = ModelManger()

if __name__ == '__main__':
    mm = ModelManger()
    model = torchvision.models.resnet18(pretrained=True)
    for i in range(5):
        mm.save_model(model, i, 0, True)
    for i in range(5):
        mm.load_best_model_by_id(2)
    mm.delete_model_by_id(2)

