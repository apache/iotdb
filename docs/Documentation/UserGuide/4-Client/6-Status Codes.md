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

# Chapter 4: Client
# Status codes

For each request, the client will receive a status code. 
If a SQL is not successfully, the status code and some message will be returned. 

Current status codes:

  SUCCESS_STATUS(200),
  STILL_EXECUTING_STATUS(201),
  INVALID_HANDLE_STATUS(202),
  TIMESERIES_ALREADY_EXIST_ERROR(300),
  TIMESERIES_NOT_EXIST_ERROR(301),
  UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR(302),
  METADATA_ERROR(303),
  OUT_OF_TTL_ERROR(305),
  CONFIG_ADJUSTER(306),
  MERGE_ERROR(307),
  SYSTEM_CHECK_ERROR(308),
  SYNC_DEVICE_OWNER_CONFLICT_ERROR(309),
  SYNC_CONNECTION_EXCEPTION(310),
  STORAGE_GROUP_PROCESSOR_ERROR(311),
  STORAGE_GROUP_ERROR(312),
  STORAGE_ENGINE_ERROR(313),
  EXECUTE_STATEMENT_ERROR(400),
  SQL_PARSE_ERROR(401),
  GENERATE_TIME_ZONE_ERROR(402),
  SET_TIME_ZONE_ERROR(403),
  NOT_STORAGE_GROUP_ERROR(404),
  QUERY_NOT_ALLOWED(405),
  AST_FORMAT_ERROR(406),
  LOGICAL_OPERATOR_ERROR(407),
  LOGICAL_OPTIMIZE_ERROR(408),
  UNSUPPORTED_FILL_TYPE_ERROR(409),
  PATH_ERROR(410),
  INTERNAL_SERVER_ERROR(500),
  CLOSE_OPERATION_ERROR(501),
  READ_ONLY_SYSTEM_ERROR(502),
  DISK_SPACE_INSUFFICIENT_ERROR(503),
  START_UP_ERROR(504),
  WRONG_LOGIN_PASSWORD_ERROR(600),
  NOT_LOGIN_ERROR(601),
  NO_PERMISSION_ERROR(602),
  UNINITIALIZED_AUTH_ERROR(603),
  INCOMPATIBLE_VERSION(203) 