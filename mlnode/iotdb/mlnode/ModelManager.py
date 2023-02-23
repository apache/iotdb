import torchvision
import os

from ModelInfo import ModelInfo
from ThreadWithReturn import ThreadWithReturn

from pylru import lrucache

class ModelManger(object):
    def __init__(self, root_path='ModelData', cache_size=10):
        self.root_path = root_path
        if not os.path.exists(root_path):
            os.mkdir(root_path)
        self.ModelMap = {}
        self.lastModelID = None
        self.lastModel = None
        # store best trial of models
        self.model_cache = lrucache(cache_size)

    def getModelinfobyID(self, model_id):
        return self.ModelMap.get(model_id)

    def registerModel(self, model_id):
        modelinfo = ModelInfo(root_path=self.root_path, model_id=model_id)
        self.ModelMap[model_id] = modelinfo
        return modelinfo

    def deleteModelbyID(self, model_id):
        modelinfo = self.getModelinfobyID(model_id)
        thread = ThreadWithReturn(func=modelinfo.deleteModel, args=())
        thread.start()
        thread.join()
        self.ModelMap.pop(model_id)
        # delete model in cache
        self.model_cache.pop(model_id)

    def loadBestModelbyID(self, model_id):
        # if model in cache, return directly
        if model_id in self.model_cache:
            return self.model_cache[model_id]
        modelinfo = self.getModelinfobyID(model_id)
        thread = ThreadWithReturn(func=modelinfo.loadBestModel, args=())
        thread.start()
        thread.join()
        return thread.get_result()

    def saveModel(self, model, model_id, trail_id, updateBestTrail=True):
        if not model_id in self.ModelMap:
            modelinfo = self.registerModel(model_id)
        else:
            modelinfo = self.getModelinfobyID(model_id)
        modelinfo.saveModel(model, trail_id, updateBestTrail)
        # update best trial in cache
        if updateBestTrail:
            self.model_cache[model_id] = model


modelManager = ModelManger()

if __name__ == '__main__':
    mm = ModelManger()
    model = torchvision.models.resnet18(pretrained=True)
    for i in range(5):
        mm.saveModel(model, i, 0, True)
    for i in range(5):
        mm.loadBestModelbyID(2)
    mm.deleteModelbyID(2)

