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

package org.apache.iotdb.confignode.client.async.handlers.rpc;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TreeDeviceViewFieldDetectionHandler
    extends DataNodeAsyncRequestRPCHandler<TDeviceViewResp> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TreeDeviceViewFieldDetectionHandler.class);

  private static final Set<Integer> EXPECTED_FAILURE_CODES =
      Set.of(
          TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode(),
          TSStatusCode.COLUMN_ALREADY_EXISTS.getStatusCode(),
          TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode());

  protected TreeDeviceViewFieldDetectionHandler(
      final CnToDnAsyncRequestType requestType,
      final int requestId,
      final TDataNodeLocation targetNode,
      final Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      final Map<Integer, TDeviceViewResp> integerTDeviceViewRespMap,
      final CountDownLatch countDownLatch) {
    super(
        requestType,
        requestId,
        targetNode,
        dataNodeLocationMap,
        integerTDeviceViewRespMap,
        countDownLatch);
  }

  @Override
  public void onComplete(final TDeviceViewResp response) {
    // Put response
    responseMap.put(requestId, response);

    if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info(
          ConfigNodeMessages.SUCCESSFULLY_ON_DATANODE, requestType, formattedTargetLocation);
    } else if (isExpectedFailure(response.getStatus())) {
      // The response is not logged, as it carries the detected fields and is of no use here.
      LOGGER.info(
          ConfigNodeMessages.FAILED_TO_ON_DATANODE_WITH_STATUS,
          requestType,
          formattedTargetLocation,
          response.getStatus().getCode(),
          failureMessage(response.getStatus()));
    } else {
      LOGGER.error(
          ConfigNodeMessages.FAILED_TO_ON_DATANODE_RESPONSE,
          requestType,
          formattedTargetLocation,
          response);
    }

    // Always remove to avoid retrying
    nodeLocationMap.remove(requestId);

    // Always CountDown
    countDownLatch.countDown();
  }

  /**
   * Whether every failure the status carries is decided by the input of the client. The DataNodes
   * aggregate the failures of their regions into MULTIPLE_ERROR, so the sub statuses are checked
   * instead of the code of the status itself.
   */
  private static boolean isExpectedFailure(final TSStatus status) {
    if (status.getCode() != TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      return EXPECTED_FAILURE_CODES.contains(status.getCode());
    }
    if (!status.isSetSubStatus() || status.getSubStatus().isEmpty()) {
      return false;
    }
    for (final TSStatus subStatus : status.getSubStatus()) {
      if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && !EXPECTED_FAILURE_CODES.contains(subStatus.getCode())) {
        return false;
      }
    }
    return true;
  }

  /**
   * The message of the status, or of its first failing sub status when the status only aggregates
   * the failures of several regions and carries no message of its own.
   */
  private static String failureMessage(final TSStatus status) {
    if (status.getMessage() != null && !status.getMessage().isEmpty()) {
      return status.getMessage();
    }
    if (status.isSetSubStatus()) {
      for (final TSStatus subStatus : status.getSubStatus()) {
        if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && subStatus.getMessage() != null) {
          return subStatus.getMessage();
        }
      }
    }
    return "";
  }

  @Override
  public void onError(final Exception e) {
    final String errorMsg =
        "Failed to "
            + requestType
            + " on DataNode: "
            + formattedTargetLocation
            + ", exception: "
            + e.getMessage();
    LOGGER.warn(errorMsg, e);

    // Ensure that the map is always non-null
    responseMap.put(
        requestId,
        new TDeviceViewResp()
            .setStatus(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, errorMsg))
            .setDeviewViewFieldTypeMap(new HashMap<>()));

    // Always CountDown
    countDownLatch.countDown();
  }
}
