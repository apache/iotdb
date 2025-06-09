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

from ainode.core.constant import TSStatusCode
from ainode.core.log import Logger
from ainode.thrift.common.ttypes import TSStatus
from ainode.core.constant import TSStatusCode


def get_status(status_code: TSStatusCode, message: str = None) -> TSStatus:
    status = TSStatus(status_code.get_status_code())
    status.message = message
    return status


def verify_success(status: TSStatus, err_msg: str) -> None:
    if status.code != TSStatusCode.SUCCESS_STATUS.get_status_code():
        Logger().warning(err_msg + ", error status is ", status)
        raise RuntimeError(str(status.code) + ": " + status.message)
    
# TODO: create model增强：获取模型状态并检查操作   
def get_model_status(status_code: TSStatusCode, model_id: str = "", message: str = None) -> TSStatus:
    """
    获取模型相关的状态对象
    """
    status = TSStatus(status_code.get_status_code())
    if message:
        status.message = f"Model {model_id}: {message}" if model_id else message
    else:
        status.message = f"Model {model_id} operation completed" if model_id else "Operation completed"
    return status

def verify_model_success(status: TSStatus, model_id: str, operation: str) -> None:
    """
    验证模型操作是否成功
    """
    if status.code != TSStatusCode.SUCCESS_STATUS.get_status_code():
        error_msg = f"Model {model_id} {operation} failed"
        Logger().error(error_msg + f", status: {status}")
        raise RuntimeError(f"{status.code}: {status.message}")
