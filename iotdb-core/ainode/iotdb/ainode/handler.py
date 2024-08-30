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

import psutil
from yaml import YAMLError

from iotdb.ainode.constant import TSStatusCode
from iotdb.ainode.exception import InvaildUriError, BadConfigValueError
from iotdb.ainode.inference import inference_with_registered_model, inference_with_built_in_model
from iotdb.ainode.log import logger
from iotdb.ainode.parser import (parse_inference_request)
from iotdb.ainode.serde import convert_to_binary
from iotdb.ainode.storage import model_storage
from iotdb.ainode.util import get_status
from iotdb.thrift.ainode import IAINodeRPCService
from iotdb.thrift.ainode.ttypes import (TDeleteModelReq, TRegisterModelReq,
                                        TRegisterModelResp, TAIHeartbeatReq, TAIHeartbeatResp,
                                        TInferenceReq, TInferenceResp)
from iotdb.thrift.common.ttypes import TLoadSample


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self):
        # for training, it's not open now.
        self.__task_manager = None

    def registerModel(self, req: TRegisterModelReq):
        logger.debug(f"register model {req.modelId} from {req.uri}")
        try:
            configs, attributes = model_storage.register_model(req.modelId, req.uri)
            return TRegisterModelResp(get_status(TSStatusCode.SUCCESS_STATUS), configs, attributes)
        except InvaildUriError as e:
            logger.warning(e)
            model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.INVALID_URI_ERROR, e.message))
        except BadConfigValueError as e:
            logger.warning(e)
            model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, e.message))
        except YAMLError as e:
            logger.warning(e)
            model_storage.delete_model(req.modelId)
            if hasattr(e, 'problem_mark'):
                mark = e.problem_mark
                return TRegisterModelResp(get_status(TSStatusCode.INVALID_INFERENCE_CONFIG,
                                                     f"An error occurred while parsing the yaml file, "
                                                     f"at line {mark.line + 1} column {mark.column + 1}."))
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, f"An error occurred while parsing the yaml file"))
        except Exception as e:
            logger.warning(e)
            model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.AINODE_INTERNAL_ERROR))

    def deleteModel(self, req: TDeleteModelReq):
        logger.debug(f"delete model {req.modelId}")
        try:
            model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warning(e)
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def inference(self, req: TInferenceReq):
        logger.info(f"infer {req.modelId}")
        model_id, full_data, window_interval, window_step, inference_attributes = parse_inference_request(
            req)
        try:
            if model_id.startswith('_'):
                # built-in models
                inference_results = inference_with_built_in_model(
                    model_id, full_data, inference_attributes)
            else:
                # user-registered models
                inference_results = inference_with_registered_model(
                    model_id, full_data, window_interval, window_step, inference_attributes)
            for i in range(len(inference_results)):
                inference_results[i] = convert_to_binary(inference_results[i])
            return TInferenceResp(
                get_status(
                    TSStatusCode.SUCCESS_STATUS),
                inference_results)
        except Exception as e:
            logger.warning(e)
            inference_results = []
            return TInferenceResp(get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e)), inference_results)

    def getAIHeartbeat(self, req: TAIHeartbeatReq):
        if req.needSamplingLoad:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            disk_usage = psutil.disk_usage('/')
            disk_free = disk_usage.free
            load_sample = TLoadSample(cpuUsageRate=cpu_percent,
                                      memoryUsageRate=memory_percent,
                                      diskUsageRate=disk_usage.percent,
                                      freeDiskSpace=disk_free / 1024 / 1024 / 1024)
            return TAIHeartbeatResp(heartbeatTimestamp=req.heartbeatTimestamp,
                                    status="Running",
                                    loadSample=load_sample)
        else:
            return TAIHeartbeatResp(heartbeatTimestamp=req.heartbeatTimestamp,
                                    status="Running")
