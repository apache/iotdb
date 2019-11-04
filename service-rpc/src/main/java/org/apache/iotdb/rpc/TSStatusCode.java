/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rpc;

public enum TSStatusCode {
  SUCCESS_STATUS(200),
  STILL_EXECUTING_STATUS(201),
  INVALID_HANDLE_STATUS(202),
  TIMESERIES_NOT_EXIST_ERROR(301),
  UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR(302),
  FETCH_METADATA_ERROR(303),
  CHECK_FILE_LEVEL_ERROR(304),
  OUT_OF_TTL_ERROR(305),
  EXECUTE_STATEMENT_ERROR(400),
  SQL_PARSE_ERROR(401),
  GENERATE_TIME_ZONE_ERROR(402),
  SET_TIME_ZONE_ERROR(403),
  NOT_A_STORAGE_GROUP_ERROR(404),
  QUERY_NOT_ALLOWED(405),
  INTERNAL_SERVER_ERROR(500),
  CLOSE_OPERATION_ERROR(501),
  READ_ONLY_SYSTEM_ERROR(502),
  WRONG_LOGIN_PASSWORD_ERROR(600),
  NOT_LOGIN_ERROR(601),
  NO_PERMISSION_ERROR(602),
  UNINITIALIZED_AUTH_ERROR(603);

  private int statusCode;

  TSStatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
