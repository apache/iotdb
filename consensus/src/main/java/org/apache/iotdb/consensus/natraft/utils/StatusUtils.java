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

package org.apache.iotdb.consensus.natraft.utils;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse.Builder;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

public class StatusUtils {

  private StatusUtils() {
    // util class
  }

  public static final TSStatus OK = getStatus(TSStatusCode.SUCCESS_STATUS);
  public static final TSStatus TIME_OUT = getStatus(TSStatusCode.TIME_OUT);
  public static final TSStatus NO_LEADER = getStatus(TSStatusCode.NO_LEADER);
  public static final TSStatus INTERNAL_ERROR = getStatus(TSStatusCode.INTERNAL_SERVER_ERROR);

  public static final TSStatus EXECUTE_STATEMENT_ERROR =
      getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  public static final TSStatus NODE_READ_ONLY = getStatus(TSStatusCode.READ_ONLY);

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
      case INCOMPATIBLE_VERSION:
        status.setMessage("Incompatible version. ");
        break;
      case EXECUTE_STATEMENT_ERROR:
        status.setMessage("Execute statement error. ");
        break;
      case INTERNAL_SERVER_ERROR:
        status.setMessage("Internal server error. ");
        break;
      case START_UP_ERROR:
        status.setMessage("Meet error while starting up. ");
        break;
      case UNSUPPORTED_OPERATION:
        status.setMessage("Unsupported operation. ");
        break;
      default:
        status.setMessage("");
        break;
    }
    return status;
  }

  public static TSStatus getStatus(TSStatus status, String message) {
    TSStatus newStatus = status.deepCopy();
    newStatus.setMessage(message);
    return newStatus;
  }

  public static ConsensusGenericResponse toGenericResponse(TSStatus status) {
    Builder builder = ConsensusGenericResponse.newBuilder();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      builder.setSuccess(true);
    } else {
      builder.setSuccess(false);
      builder.setException(
          new ConsensusException(String.format("%d:%s", status.getCode(), status.getMessage())));
    }

    return ConsensusGenericResponse.newBuilder().build();
  }
}
