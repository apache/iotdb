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

logger = Logger()


def get_status(status_code: TSStatusCode, message: str = None) -> TSStatus:
    """Create TSStatus object with given status code and message"""
    status = TSStatus(status_code.get_status_code())
    status.message = message
    return status


def get_model_status(
    status_code: TSStatusCode, model_id: str = "", message: str = None
) -> TSStatus:
    """Create model-specific TSStatus object"""
    status = TSStatus(status_code.get_status_code())
    if message:
        status.message = f"Model {model_id}: {message}" if model_id else message
    else:
        status.message = (
            f"Model {model_id} operation completed"
            if model_id
            else "Operation completed"
        )
    return status


def verify_success(status: TSStatus, err_msg: str) -> None:
    """Verify operation success and raise exception if failed"""
    if status.code != TSStatusCode.SUCCESS_STATUS.get_status_code():
        logger.warning(f"{err_msg}, error status: {status}")
        raise RuntimeError(f"{status.code}: {status.message}")


def verify_model_success(status: TSStatus, model_id: str, operation: str) -> None:
    """Verify model operation success with enhanced error message"""
    if status.code != TSStatusCode.SUCCESS_STATUS.get_status_code():
        error_msg = f"Model {model_id} {operation} failed"
        logger.error(f"{error_msg}, status: {status}")
        raise RuntimeError(f"{status.code}: {status.message}")


def create_success_status(message: str = None) -> TSStatus:
    """Create success status with optional message"""
    return get_status(TSStatusCode.SUCCESS_STATUS, message)


def create_error_status(error_type: TSStatusCode, error_msg: str) -> TSStatus:
    """Create error status with specified type and message"""
    return get_status(error_type, error_msg)


def is_success(status: TSStatus) -> bool:
    """Check if status indicates success"""
    return status.code == TSStatusCode.SUCCESS_STATUS.get_status_code()


def log_status_result(status: TSStatus, operation: str, model_id: str = None):
    """Log status result with appropriate level"""
    if is_success(status):
        if model_id:
            logger.info(f"Model {model_id} {operation} successful")
        else:
            logger.info(f"{operation} successful")
    else:
        if model_id:
            logger.error(f"Model {model_id} {operation} failed: {status.message}")
        else:
            logger.error(f"{operation} failed: {status.message}")
