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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Arrays;
import java.util.Map;

public class StatusUtils {
  private StatusUtils() {}

  public static final TSStatus OK = getStatus(TSStatusCode.SUCCESS_STATUS);
  public static final TSStatus INTERNAL_ERROR = getStatus(TSStatusCode.INTERNAL_SERVER_ERROR);
  public static final TSStatus EXECUTE_STATEMENT_ERROR =
      getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);

  /**
   * @param statusMap index -> status
   * @param size the total number of status to generate
   */
  public static TSStatus[] getFailingStatus(Map<Integer, TSStatus> statusMap, int size) {
    if (statusMap == null || statusMap.isEmpty()) {
      return new TSStatus[0];
    }
    TSStatus[] failingStatus = new TSStatus[size];
    Arrays.fill(failingStatus, RpcUtils.SUCCESS_STATUS);
    for (Map.Entry<Integer, TSStatus> status : statusMap.entrySet()) {
      failingStatus[status.getKey()] = status.getValue();
    }
    return failingStatus;
  }

  public static TSStatus getStatus(TSStatusCode statusCode) {
    TSStatus status = new TSStatus();
    status.setCode(statusCode.getStatusCode());
    switch (statusCode) {
      case SUCCESS_STATUS:
        status.setMessage("Executed successfully.");
        break;
      case INTERNAL_REQUEST_TIME_OUT:
        status.setMessage("Request timed out.");
        break;
      case INCOMPATIBLE_VERSION:
        status.setMessage("Incompatible version.");
        break;
      case REMOVE_DATANODE_ERROR:
        status.setMessage("Failed while removing DataNode.");
        break;
      case ALIAS_ALREADY_EXIST:
        status.setMessage("Alias already exists.");
        break;
      case PATH_ALREADY_EXIST:
        status.setMessage("Path already exist.");
        break;
      case PATH_NOT_EXIST:
        status.setMessage("Path does not exist.");
        break;
      case METADATA_ERROR:
        status.setMessage("Meet error when dealing with metadata.");
        break;
      case OUT_OF_TTL:
        status.setMessage("Insertion time is less than TTL time bound.");
        break;
      case COMPACTION_ERROR:
        status.setMessage("Meet error while merging.");
        break;
      case DISPATCH_ERROR:
        status.setMessage("Meet error while dispatching.");
        break;
      case DATAREGION_PROCESS_ERROR:
        status.setMessage("Database processor related error.");
        break;
      case STORAGE_ENGINE_ERROR:
        status.setMessage("Storage engine related error.");
        break;
      case TSFILE_PROCESSOR_ERROR:
        status.setMessage("TsFile processor related error.");
        break;
      case ILLEGAL_PATH:
        status.setMessage("Illegal path.");
        break;
      case LOAD_FILE_ERROR:
        status.setMessage("Meet error while loading file.");
        break;
      case EXECUTE_STATEMENT_ERROR:
        status.setMessage("Execute statement error.");
        break;
      case SQL_PARSE_ERROR:
        status.setMessage("Meet error while parsing SQL.");
        break;
      case GENERATE_TIME_ZONE_ERROR:
        status.setMessage("Meet error while generating time zone.");
        break;
      case SET_TIME_ZONE_ERROR:
        status.setMessage("Meet error while setting time zone.");
        break;
      case QUERY_NOT_ALLOWED:
        status.setMessage("Query statements are not allowed error.");
        break;
      case LOGICAL_OPERATOR_ERROR:
        status.setMessage("Logical operator related error.");
        break;
      case LOGICAL_OPTIMIZE_ERROR:
        status.setMessage("Logical optimize related error.");
        break;
      case UNSUPPORTED_FILL_TYPE:
        status.setMessage("Unsupported fill type related error.");
        break;
      case QUERY_PROCESS_ERROR:
        status.setMessage("Query process related error.");
        break;
      case WRITE_PROCESS_ERROR:
        status.setMessage("Writing data related error.");
        break;
      case INTERNAL_SERVER_ERROR:
        status.setMessage("Internal server error.");
        break;
      case CLOSE_OPERATION_ERROR:
        status.setMessage("Meet error in close operation.");
        break;
      case SYSTEM_READ_ONLY:
        status.setMessage("Database is read-only.");
        break;
      case DISK_SPACE_INSUFFICIENT:
        status.setMessage("Disk space is insufficient.");
        break;
      case START_UP_ERROR:
        status.setMessage("Meet error while starting up.");
        break;
      case WRONG_LOGIN_PASSWORD:
        status.setMessage("Username or password is wrong.");
        break;
      case NOT_LOGIN:
        status.setMessage("Has not logged in.");
        break;
      case NO_PERMISSION:
        status.setMessage("No permissions for this operation, please add privilege.");
        break;
      case INIT_AUTH_ERROR:
        status.setMessage("Failed to init authorizer.");
        break;
      case UNSUPPORTED_OPERATION:
        status.setMessage("Unsupported operation.");
        break;
      case CAN_NOT_CONNECT_DATANODE:
        status.setMessage("Node cannot be reached.");
        break;
      default:
        status.setMessage("");
        break;
    }
    return status;
  }
}
