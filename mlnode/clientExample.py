from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from iotdb.thrift.mlnode.ttypes import (
    TDeleteModelReq,
    TCreateTrainingTaskReq,
    TForecastReq,
)

from iotdb.thrift.mlnode.IMLNodeRPCService import (
    Client
)

from iotdb.conf.config import *


tsocket = TSocket.TSocket(ML_HOST, ML_PORT)
transport = TTransport.TBufferedTransport(tsocket)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Client(protocol)


transport.open()

deleteModelReq = TDeleteModelReq(modelPath='./path/to/model')
res = client.deleteModel(deleteModelReq)

createTrainingTaskReq = TCreateTrainingTaskReq(
    modelId='0',
    isAuto=True,
    modelConfigs={'type': 'DLinear',
                  'input_length': '96',
                  'output_length': '96',
                  'num_series': '8',
                  'individual': 'False'},
    queryExpressions=['SELECT OT FROM root.eg.exchange WHERE Time<=2017-08-01'],
    queryFilter=None
)
res = client.createTrainingTask(createTrainingTaskReq)

dataset = b'dataset'
forecastReq = TForecastReq(modelPath='./path/to/model', dataset=dataset)
res = client.forecast(forecastReq)
print(res.forecastResult)

transport.close()
