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
import org.apache.iotdb.commons.i18n.UtilMessages;
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
    NEED_RETRY.add(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.CREATE_REGION_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode());
    NEED_RETRY.add(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode());
    NEED_RETRY.add(TSStatusCode.LACK_PARTITION_ALLOCATION.getStatusCode());
    NEED_RETRY.add(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
    NEED_RETRY.add(TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.SYNC_CONNECTION_ERROR.getStatusCode());
    NEED_RETRY.add(TSStatusCode.PLAN_FAILED_NETWORK_PARTITION.getStatusCode());
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
        status.setMessage(UtilMessages.MESSAGE_EXECUTED_SUCCESSFULLY_1EAF1169);
        break;
      case INTERNAL_REQUEST_TIME_OUT:
        status.setMessage(UtilMessages.MESSAGE_REQUEST_TIMED_OUT_FD587FC4);
        break;
      case INCOMPATIBLE_VERSION:
        status.setMessage(UtilMessages.MESSAGE_INCOMPATIBLE_VERSION_0C5CB2AF);
        break;
      case REMOVE_DATANODE_ERROR:
        status.setMessage(UtilMessages.MESSAGE_FAILED_REMOVING_DATANODE_B4B7F050);
        break;
      case ALIAS_ALREADY_EXIST:
        status.setMessage(UtilMessages.MESSAGE_ALIAS_ALREADY_EXISTS_C05A2E5A);
        break;
      case PATH_ALREADY_EXIST:
        status.setMessage(UtilMessages.MESSAGE_PATH_ALREADY_EXIST_56F8BFF9);
        break;
      case PATH_NOT_EXIST:
        status.setMessage(UtilMessages.MESSAGE_PATH_DOES_NOT_EXIST_93310498);
        break;
      case METADATA_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_DEALING_METADATA_1C4A38B9);
        break;
      case OUT_OF_TTL:
        status.setMessage(UtilMessages.MESSAGE_INSERTION_TIME_LESS_THAN_TTL_TIME_BOUND_0F1BB861);
        break;
      case COMPACTION_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_MERGING_28424A77);
        break;
      case DISPATCH_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_DISPATCHING_73E4FD5E);
        break;
      case DATAREGION_PROCESS_ERROR:
        status.setMessage(UtilMessages.MESSAGE_DATABASE_PROCESSOR_RELATED_ERROR_C58690B1);
        break;
      case STORAGE_ENGINE_ERROR:
        status.setMessage(UtilMessages.MESSAGE_STORAGE_ENGINE_RELATED_ERROR_94DEBBCF);
        break;
      case TSFILE_PROCESSOR_ERROR:
        status.setMessage(UtilMessages.MESSAGE_TSFILE_PROCESSOR_RELATED_ERROR_B6C57C3E);
        break;
      case ILLEGAL_PATH:
        status.setMessage(UtilMessages.MESSAGE_ILLEGAL_PATH_020B26BC);
        break;
      case LOAD_FILE_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_LOADING_FILE_A90EBC21);
        break;
      case EXECUTE_STATEMENT_ERROR:
        status.setMessage(UtilMessages.MESSAGE_EXECUTE_STATEMENT_ERROR_54E4A395);
        break;
      case SQL_PARSE_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_PARSING_SQL_3C5A3B80);
        break;
      case GENERATE_TIME_ZONE_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_GENERATING_TIME_ZONE_94E03CA3);
        break;
      case SET_TIME_ZONE_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_SETTING_TIME_ZONE_CBE88DCF);
        break;
      case QUERY_NOT_ALLOWED:
        status.setMessage(UtilMessages.MESSAGE_QUERY_STATEMENTS_NOT_ALLOWED_ERROR_58D30C99);
        break;
      case LOGICAL_OPERATOR_ERROR:
        status.setMessage(UtilMessages.MESSAGE_LOGICAL_OPERATOR_RELATED_ERROR_D94D5972);
        break;
      case LOGICAL_OPTIMIZE_ERROR:
        status.setMessage(UtilMessages.MESSAGE_LOGICAL_OPTIMIZE_RELATED_ERROR_36D63CB3);
        break;
      case UNSUPPORTED_FILL_TYPE:
        status.setMessage(UtilMessages.MESSAGE_UNSUPPORTED_FILL_TYPE_RELATED_ERROR_67BE2CF2);
        break;
      case QUERY_PROCESS_ERROR:
        status.setMessage(UtilMessages.MESSAGE_QUERY_PROCESS_RELATED_ERROR_93FA1016);
        break;
      case WRITE_PROCESS_ERROR:
        status.setMessage(UtilMessages.MESSAGE_WRITING_DATA_RELATED_ERROR_0CA06C4D);
        break;
      case INTERNAL_SERVER_ERROR:
        status.setMessage(UtilMessages.MESSAGE_INTERNAL_SERVER_ERROR_12F61DF7);
        break;
      case CLOSE_OPERATION_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_CLOSE_OPERATION_1C7D0589);
        break;
      case SYSTEM_READ_ONLY:
        status.setMessage(
            UtilMessages.MESSAGE_FAIL_DO_NON_QUERY_OPERATIONS_BECAUSE_SYSTEM_READ_ONLY_10CA1ED2);
        break;
      case DISK_SPACE_INSUFFICIENT:
        status.setMessage(UtilMessages.MESSAGE_DISK_SPACE_INSUFFICIENT_DF6205B0);
        break;
      case START_UP_ERROR:
        status.setMessage(UtilMessages.MESSAGE_MEET_ERROR_STARTING_UP_22A4CBFE);
        break;
      case WRONG_LOGIN_PASSWORD:
        status.setMessage(UtilMessages.MESSAGE_USERNAME_PASSWORD_WRONG_C44C4AF0);
        break;
      case NOT_LOGIN:
        status.setMessage(UtilMessages.MESSAGE_HAS_NOT_LOGGED_A2BA0267);
        break;
      case NO_PERMISSION:
        status.setMessage(
            UtilMessages.MESSAGE_NO_PERMISSIONS_OPERATION_PLEASE_ADD_PRIVILEGE_64047D1E);
        break;
      case INIT_AUTH_ERROR:
        status.setMessage(UtilMessages.MESSAGE_FAILED_INIT_AUTHORIZER_1E2B017E);
        break;
      case UNSUPPORTED_OPERATION:
        status.setMessage(UtilMessages.MESSAGE_UNSUPPORTED_OPERATION_295CDB21);
        break;
      case CAN_NOT_CONNECT_DATANODE:
        status.setMessage(UtilMessages.MESSAGE_NODE_CANNOT_REACHED_D3FD04A8);
        break;
      default:
        status.setMessage(UtilMessages.EMPTY_MESSAGE);
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

  public static boolean isUnknownError(int statusCode) {
    return UNKNOWN_ERRORS.contains(statusCode);
  }

  public static int retrieveExitStatusCode(Throwable e) {
    while (e.getCause() != null) {
      e = e.getCause();
    }
    if (e.getMessage() != null) {
      if (e.getMessage().contains("because Could not create ServerSocket")
          || e.getMessage().contains("Failed to bind to address")
          || e.getMessage().contains("Address already in use: bind")) {
        return TSStatusCode.PORT_OCCUPIED.getStatusCode();
      }

      if (e instanceof ClassNotFoundException || e instanceof IllegalArgumentException) {
        return TSStatusCode.ILLEGAL_PARAMETER.getStatusCode();
      }
    }
    return -1;
  }
}
