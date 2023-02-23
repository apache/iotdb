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
from thrift.protocol import TCompactProtocol
from thrift.transport import TSocket, TTransport

from log import logger
from utils.thrift.mlnode import IMLNodeRPCService
from utils.thrift.mlnode.ttypes import TCreateTrainingTaskReq, TDeleteModelReq, TForecastReq


class MLNodeClient(object):
    def __init__(self, host, port):
        self.__host = host
        self.__port = port

        transport = TTransport.TBufferedTransport(
            TSocket.TSocket(self.__host, self.__port)
        )
        if not transport.isOpen():
            try:
                transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        protocol = TCompactProtocol.TCompactProtocol(transport)
        self.__client = IMLNodeRPCService.Client(protocol)
    
    def create_training_task(self):
        req = TCreateTrainingTaskReq()
        return self.__client.createTrainingTask(req)
    
    def create_forecast_task(self):
        req = TForecastReq()
        return self.__client.forecast(req)

    def delete_model(self, model_path: str):
        req = TDeleteModelReq(model_path)
        return self.__client.deleteModel(req)


if __name__ == "__main__":
    # test rpc service
    client = MLNodeClient(host="127.0.0.1", port=10810)
    print(client.create_forecast_task())
    
