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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StatusUtils {
  private StatusUtils() {}

  public static final TSStatus OK = getStatus(TSStatusCode.SUCCESS_STATUS);
  public static final TSStatus INTERNAL_ERROR = getStatus(TSStatusCode.INTERNAL_SERVER_ERROR);
  public static final TSStatus EXECUTE_STATEMENT_ERROR =
      getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);

  private static final Set<Integer> NEED_RETRY = new HashSet<>();

  private static final Set<Integer> UNKNOWN_ERRORS = new HashSet<>();

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  static {
    // UNKNOWN ERRORS
    UNKNOWN_ERRORS.add(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    UNKNOWN_ERRORS.add(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());

    // KNOWN ERRORS
    NEED_RETRY.add(TSStatusCode.DISPATCH_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode());
    NEED_RETRY.add(TSStatusCode.STORAGE_ENGINE_NOT_READY.getStatusCode());
    NEED_RETRY.add(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode());
    NEED_RETRY.add(TSStatusCode.WAL_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.DISK_SPACE_INSUFFICIENT.getStatusCode());
    NEED_RETRY.add(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.INTERNAL_REQUEST_TIME_OUT.getStatusCode());
    NEED_RETRY.add(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode());
    NEED_RETRY.add(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode());
    NEED_RETRY.add(TSStatusCode.LACK_PARTITION_ALLOCATION.getStatusCode());
    NEED_RETRY.add(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
    NEED_RETRY.add(TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode());
  }

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
        status.setMessage("Fail to do non-query operations because system is read-only.");
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

  public static boolean needRetry(TSStatus status) {
    // succeed operation should never retry
    if (status == null || status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return false;
    }
    // always retry while node is in not running case
    if (!COMMON_CONFIG.isRunning()) {
      return true;
    } else {
      return needRetryHelper(status);
    }
  }

  public static boolean needRetryHelper(TSStatus status) {
    int code = status.getCode();
    if (code == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      for (TSStatus subStatus : status.subStatus) {
        // any sub codes for MULTIPLE_ERROR don't need to retry, we won't retry for the whole
        // request
        if (subStatus == null
            || (subStatus.getCode() != OK.code
                && !needRetryHelperForSingleStatus(subStatus.getCode()))) {
          return false;
        }
      }
      return true;
    } else {
      return needRetryHelperForSingleStatus(code);
    }
  }

  // without MULTIPLE_ERROR(302)
  private static boolean needRetryHelperForSingleStatus(int statusCode) {
    return NEED_RETRY.contains(statusCode)
        || (COMMON_CONFIG.isRetryForUnknownErrors() && UNKNOWN_ERRORS.contains(statusCode));
  }
}
