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

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

public class StatusUtils {

  private StatusUtils() {
    // util class
  }

  public static final TSStatus PARTITION_TABLE_NOT_READY =
      getStatus(TSStatusCode.PARTITION_NOT_READY);
  public static final TSStatus OK = getStatus(TSStatusCode.SUCCESS_STATUS);
  public static final TSStatus TIME_OUT = getStatus(TSStatusCode.TIME_OUT);
  public static final TSStatus NO_LEADER = getStatus(TSStatusCode.NO_LEADER);
  public static final TSStatus INTERNAL_ERROR = getStatus(TSStatusCode.INTERNAL_SERVER_ERROR);
  public static final TSStatus UNSUPPORTED_OPERATION =
      getStatus(TSStatusCode.UNSUPPORTED_OPERATION);
  public static final TSStatus EXECUTE_STATEMENT_ERROR =
      getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  public static final TSStatus NO_STORAGE_GROUP = getStatus(TSStatusCode.STORAGE_GROUP_ERROR);
  public static final TSStatus NODE_READ_ONLY = getStatus(TSStatusCode.NODE_READ_ONLY);
  public static final TSStatus CONSISTENCY_FAILURE = getStatus(TSStatusCode.CONSISTENCY_FAILURE);
  public static final TSStatus TIMESERIES_NOT_EXIST_ERROR =
      getStatus(TSStatusCode.TIMESERIES_NOT_EXIST);
  public static final TSStatus NO_CONNECTION = getStatus(TSStatusCode.NO_CONNECTION);
  public static final TSStatus PARSE_LOG_ERROR = getStatus(TSStatusCode.PARSE_LOG_ERROR);
  public static final TSStatus DUPLICATED_TEMPLATE = getStatus(TSStatusCode.DUPLICATED_TEMPLATE);

  public static TSStatus getStatus(TSStatusCode statusCode) {
    TSStatus status = new TSStatus();
    status.setCode(statusCode.getStatusCode());
    switch (statusCode) {
      case SUCCESS_STATUS:
        status.setMessage("Executed successfully. ");
        break;
      case TIME_OUT:
        status.setMessage("Request timed out. ");
        break;
      case NO_LEADER:
        status.setMessage("Leader cannot be found. ");
        break;
      case PARTITION_NOT_READY:
        status.setMessage("Partition table is not ready. ");
        break;
      case NODE_READ_ONLY:
        status.setMessage(
            "Current node is read-only, please retry to find another available node. ");
        break;
      case INCOMPATIBLE_VERSION:
        status.setMessage("Incompatible version. ");
        break;
      case NODE_DELETE_FAILED_ERROR:
        status.setMessage("Failed while deleting node. ");
        break;
      case ALIAS_ALREADY_EXIST_ERROR:
        status.setMessage("Alias already exists. ");
        break;
      case PATH_ALREADY_EXIST_ERROR:
        status.setMessage("Path already exist. ");
        break;
      case PATH_NOT_EXIST_ERROR:
      case TIMESERIES_NOT_EXIST:
        status.setMessage("Path does not exist. ");
        break;
      case UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR:
        status.setMessage("Unsupported fetch metadata operation. ");
        break;
      case METADATA_ERROR:
        status.setMessage("Meet error when dealing with metadata. ");
        break;
      case OUT_OF_TTL_ERROR:
        status.setMessage("Insertion time is less than TTL time bound. ");
        break;
      case CONFIG_ADJUSTER:
        status.setMessage("IoTDB system load is too large. ");
        break;
      case MERGE_ERROR:
        status.setMessage("Meet error while merging. ");
        break;
      case SYSTEM_CHECK_ERROR:
        status.setMessage("Meet error while system checking. ");
        break;
      case SYNC_DEVICE_OWNER_CONFLICT_ERROR:
        status.setMessage("Sync device owners conflict. ");
        break;
      case SYNC_CONNECTION_EXCEPTION:
        status.setMessage("Meet error while sync connecting. ");
        break;
      case STORAGE_GROUP_PROCESSOR_ERROR:
        status.setMessage("Storage group processor related error. ");
        break;
      case STORAGE_GROUP_ERROR:
        status.setMessage("No associated storage group. ");
        break;
      case STORAGE_ENGINE_ERROR:
        status.setMessage("Storage engine related error. ");
        break;
      case TSFILE_PROCESSOR_ERROR:
        status.setMessage("TsFile processor related error. ");
        break;
      case PATH_ILLEGAL:
        status.setMessage("Illegal path. ");
        break;
      case LOAD_FILE_ERROR:
        status.setMessage("Meet error while loading file. ");
        break;
      case EXECUTE_STATEMENT_ERROR:
        status.setMessage("Execute statement error. ");
        break;
      case SQL_PARSE_ERROR:
        status.setMessage("Meet error while parsing SQL. ");
        break;
      case GENERATE_TIME_ZONE_ERROR:
        status.setMessage("Meet error while generating time zone. ");
        break;
      case SET_TIME_ZONE_ERROR:
        status.setMessage("Meet error while setting time zone. ");
        break;
      case NOT_STORAGE_GROUP_ERROR:
        status.setMessage("Operating object is not a storage group. ");
        break;
      case QUERY_NOT_ALLOWED:
        status.setMessage("Query statements are not allowed error. ");
        break;
      case AST_FORMAT_ERROR:
        status.setMessage("AST format related error. ");
        break;
      case LOGICAL_OPERATOR_ERROR:
        status.setMessage("Logical operator related error. ");
        break;
      case LOGICAL_OPTIMIZE_ERROR:
        status.setMessage("Logical optimize related error. ");
        break;
      case UNSUPPORTED_FILL_TYPE_ERROR:
        status.setMessage("Unsupported fill type related error. ");
        break;
      case PATH_ERROR:
        status.setMessage("Path related error. ");
        break;
      case QUERY_PROCESS_ERROR:
        status.setMessage("Query process related error. ");
        break;
      case WRITE_PROCESS_ERROR:
        status.setMessage("Writing data related error. ");
        break;
      case INTERNAL_SERVER_ERROR:
        status.setMessage("Internal server error. ");
        break;
      case CLOSE_OPERATION_ERROR:
        status.setMessage("Meet error in close operation. ");
        break;
      case READ_ONLY_SYSTEM_ERROR:
        status.setMessage("Operating system is read only. ");
        break;
      case DISK_SPACE_INSUFFICIENT_ERROR:
        status.setMessage("Disk space is insufficient. ");
        break;
      case START_UP_ERROR:
        status.setMessage("Meet error while starting up. ");
        break;
      case WRONG_LOGIN_PASSWORD_ERROR:
        status.setMessage("Username or password is wrong. ");
        break;
      case NOT_LOGIN_ERROR:
        status.setMessage("Has not logged in. ");
        break;
      case NO_PERMISSION_ERROR:
        status.setMessage("No permissions for this operation. ");
        break;
      case UNINITIALIZED_AUTH_ERROR:
        status.setMessage("Uninitialized authorizer. ");
        break;
      case UNSUPPORTED_OPERATION:
        status.setMessage("Unsupported operation. ");
        break;
      case NO_CONNECTION:
        status.setMessage("Node cannot be reached.");
        break;
      case PARSE_LOG_ERROR:
        status.setMessage("Parse log error.");
        break;
      default:
        status.setMessage("");
        break;
    }
    return status;
  }

  public static TSStatus getStatus(TSStatusCode statusCode, EndPoint redirectedNode) {
    TSStatus status = getStatus(statusCode);
    status.setRedirectNode(redirectedNode);
    return status;
  }

  public static TSStatus getStatus(TSStatus status, String message) {
    TSStatus newStatus = status.deepCopy();
    newStatus.setMessage(message);
    return newStatus;
  }

  public static TSStatus getStatus(TSStatus status, EndPoint redirectedNode) {
    TSStatus newStatus = status.deepCopy();
    newStatus.setRedirectNode(redirectedNode);
    return newStatus;
  }
}
