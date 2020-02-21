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
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TSStatusType;

public class StatusUtils {

  private StatusUtils() {
    // util class
  }

  public static final TSStatus PARTITION_TABLE_NOT_READY = getStatus(TSStatusCode.PARTITION_NOT_READY);
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

  private static TSStatus getStatus(TSStatusCode statusCode) {
    TSStatusType tsStatusType = new TSStatusType();
    tsStatusType.setCode(statusCode.getStatusCode());
    TSStatus status = new TSStatus();
    status.setStatusType(tsStatusType);
    switch (statusCode) {
      case TIME_OUT:
        tsStatusType.setMessage("Request timed out");
        break;
      case NO_LEADER:
        tsStatusType.setMessage("Leader cannot be found");
        break;
      case PARTITION_NOT_READY:
        tsStatusType.setMessage("Partition table is not ready");
        break;
      case NODE_READ_ONLY:
        tsStatusType.setMessage("Current node is read-only, please retry to find another available node");
        break;
      default:
        tsStatusType.setMessage("");
        break;
    }
    return status;
  }
}
