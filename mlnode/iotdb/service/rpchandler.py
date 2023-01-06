import argparse


from iotdb.conf.config import *

from iotdb.thrift.common.ttypes import (
    TSStatus,
)
from iotdb.thrift.mlnode.ttypes import (
    TDeleteModelReq,
    TCreateTrainingTaskReq,
    TForecastReq,
    TForecastResp,
)

from iotdb.algorithm.forecast.models import *


class IMLNodeRPCServiceHandler(object):
    def __init__(self):
        # self.session = Session(DB_HOST, DB_PORT, USERNAME, PASSWORD, fetch_size=1024, zone_id='UTC+8')
        # self.session.open(False)
        # print(self.session)
        pass

    def deleteModel(self, deleteModelReq: TDeleteModelReq):
        modelPath = deleteModelReq.modelPath
        print(modelPath)
        return TSStatus(code=SUCCESS_STATUS)


    def createTrainingTask(self, createTrainingTaskReq: TCreateTrainingTaskReq):
        modelId = createTrainingTaskReq.modelId
        isAuto = createTrainingTaskReq.isAuto
        modelConfigs = createTrainingTaskReq.modelConfigs
        queryExpressions = createTrainingTaskReq.queryExpressions
        queryFilter = createTrainingTaskReq.queryFilter
        modelType = modelConfigs['type']
        model = eval(modelType)(self.__parseModelConfigs(modelType, modelConfigs))
        print(model)
        return TSStatus(code=SUCCESS_STATUS)


    def forecast(self, forecastReq: TForecastReq):
        modelPath = forecastReq.modelPath
        dataset = forecastReq.dataset
        print(modelPath)
        print(dataset)
        status = TSStatus(code=SUCCESS_STATUS)
        forecastResult = b'forecast result'
        return TForecastResp(status, forecastResult)


    def __parseModelConfigs(self, modelType, modelConfigs):
        if modelType == 'DLinear':
            configs = argparse.Namespace(
                seq_len=int(modelConfigs['input_length']),
                pred_len=int(modelConfigs['output_length']),
                enc_in=int(modelConfigs['num_series']),
                individual=modelConfigs['individual'] == str(True),
            )
        else:
            raise NotImplementedError
        return configs
