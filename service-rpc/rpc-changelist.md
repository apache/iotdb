<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## 0.8.0 (version-0) -> version-1

* Add Struct **TS_StatusType**, including code and message. Instead of using ~~TS_StatusCode~~, ~~errorCode~~ and ~~errorMessage~~, TS_Status use **TS_StatusType** to show specific success or error status, code and message.

* Use struct **TSRPCResp** to replace all structs with only one field `1: required TS_Status status`, including: ~~TSCloseSessionResp~~ closeSession, ~~TSCancelOperationResp~~ cancelOperation, ~~TSCloseOperationResp~~ closeOperation, ~~TSSetTimeZoneResp~~ setTimeZone.

* Add **TSBatchInsertionReq**, **TSCreateTimeseriesReq** and **TSSetStorageGroupReq**.

* Add method `TSExecuteStatementResp insert(1:TSInsertionReq req)` and `TSExecuteBatchStatementResp insertBatch(1:TSBatchInsertionReq req)` for batch inserting interface.

* Add method `TSRPCResp setStorageGroup(1:TSSetStorageGroupReq req)` and `TSRPCResp createTimeseries(1:TSCreateTimeseriesReq req)` for creating matadata interface.

* Change item in enum **TSProtocolVersion** from ~~TSFILE_SERVICE_PROTOCOL_V1~~ to IOTDB_SERVICE_PROTOCOL_V1.
