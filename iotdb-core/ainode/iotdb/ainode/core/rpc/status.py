1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1from iotdb.ainode.core.constant import TSStatusCode
1from iotdb.ainode.core.log import Logger
1from iotdb.thrift.common.ttypes import TSStatus
1
1
1def get_status(status_code: TSStatusCode, message: str = None) -> TSStatus:
1    status = TSStatus(status_code.get_status_code())
1    status.message = message
1    return status
1
1
1def verify_success(status: TSStatus, err_msg: str) -> None:
1    if status.code != TSStatusCode.SUCCESS_STATUS.get_status_code():
1        Logger().warning(err_msg + ", error status is ", status)
1        raise RuntimeError(str(status.code) + ": " + status.message)
1