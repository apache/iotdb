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
    modelConfigs={'model_type': 'DLinear',
                  'input_length': '96',
                  'output_length': '96',
                  'num_series': '8',
                  'individual': 'True',
                  'root_path': 'dataset',
                  'data_path': 'exchange.csv'},
    queryExpressions=['SELECT OT FROM root.eg.exchange WHERE Time<=2017-08-01'],
    queryFilter=None
)
res = client.createTrainingTask(createTrainingTaskReq)

dataset = b'dataset'
forecastReq = TForecastReq(modelPath='./path/to/model', dataset=dataset)
res = client.forecast(forecastReq)
print(res.forecastResult)

transport.close()
