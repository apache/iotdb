/**
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

public enum TSStatusType {
  SUCCESS_STATUS(200, ""),
  STILL_EXECUTING_STATUS(201, ""),
  INVALID_HANDLE_STATUS(202, ""),
  TIMESERIES_NOT_EXIST_ERROR(301, "Timeseries does not exist"),
  UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR(302, "Unsupported fetch metadata operation"),
  FETCH_METADATA_ERROR(303, "Failed to fetch metadata"),
  CHECK_FILE_LEVEL_ERROR(304, "Meet error while checking file level"),
  OUT_OF_TTL_ERROR(305, "timestamp falls out of TTL"),
  EXECUTE_STATEMENT_ERROR(400, "Execute statement error"),
  SQL_PARSE_ERROR(401, "Meet error while parsing SQL"),
  GENERATE_TIME_ZONE_ERROR(402, "Meet error while generating time zone"),
  SET_TIME_ZONE_ERROR(403, "Meet error while setting time zone"),
  INTERNAL_SERVER_ERROR(500, "Internal server error"),
  WRONG_LOGIN_PASSWORD_ERROR(600,  "Username or password is wrong"),
  NOT_LOGIN_ERROR(601, "Has not logged in"),
  NO_PERMISSION_ERROR(602, "No permissions for this operation"),
  UNINITIALIZED_AUTH_ERROR(603, "Uninitialized authorizer");

  private int statusCode;
  private String statusMessage;

  private TSStatusType(int statusCode, String statusMessage) {
    this.statusCode = statusCode;
    this.statusMessage = statusMessage;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getStatusMessage() {
    return statusMessage;
  }
}
