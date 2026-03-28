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

package org.apache.iotdb.confignode.client.async.handlers.rpc.subscription;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.handlers.rpc.DataNodeAsyncRequestRPCHandler;
import org.apache.iotdb.mpp.rpc.thrift.TPullCommitProgressResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class PullCommitProgressRPCHandler
    extends DataNodeAsyncRequestRPCHandler<TPullCommitProgressResp> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PullCommitProgressRPCHandler.class);

  public PullCommitProgressRPCHandler(
      CnToDnAsyncRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TPullCommitProgressResp> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TPullCommitProgressResp response) {
    responseMap.put(requestId, response);

    if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      logSuspiciousRegionProgressPayloads(response);
      LOGGER.info("Successfully {} on DataNode: {}", requestType, formattedTargetLocation);
    } else {
      LOGGER.error(
          "Failed to {} on DataNode: {}, response: {}",
          requestType,
          formattedTargetLocation,
          response);
    }

    nodeLocationMap.remove(requestId);
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    String errorMsg =
        "Failed to "
            + requestType
            + " on DataNode: "
            + formattedTargetLocation
            + ", exception: "
            + e.getMessage();
    LOGGER.error(errorMsg, e);

    responseMap.put(
        requestId,
        new TPullCommitProgressResp(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, errorMsg)));

    countDownLatch.countDown();
  }

  private void logSuspiciousRegionProgressPayloads(final TPullCommitProgressResp response) {
    if (response == null || !response.isSetCommitRegionProgress()) {
      return;
    }
    for (final Map.Entry<String, java.nio.ByteBuffer> entry :
        response.getCommitRegionProgress().entrySet()) {
      if (isSuspiciousRegionProgressPayload(entry.getValue())) {
        LOGGER.warn(
            "PULL_COMMIT_PROGRESS confignode recv suspicious payload from DataNode {}, key={}, summary={}",
            formattedTargetLocation,
            entry.getKey(),
            summarizeRegionProgressPayload(entry.getValue()));
      }
    }
  }

  private boolean isSuspiciousRegionProgressPayload(final java.nio.ByteBuffer buffer) {
    if (buffer == null) {
      return true;
    }
    final java.nio.ByteBuffer duplicate = buffer.slice();
    if (duplicate.remaining() < Integer.BYTES) {
      return true;
    }
    final int firstInt = duplicate.getInt();
    return firstInt < 0 || firstInt > 1_000_000;
  }

  private String summarizeRegionProgressPayload(final java.nio.ByteBuffer buffer) {
    if (buffer == null) {
      return "null";
    }
    final int position = buffer.position();
    final int limit = buffer.limit();
    final int capacity = buffer.capacity();
    final java.nio.ByteBuffer duplicate = buffer.slice();
    final int remaining = duplicate.remaining();
    final String firstIntSummary;
    if (remaining >= Integer.BYTES) {
      final int firstInt = duplicate.getInt();
      firstIntSummary = firstInt + "(0x" + String.format("%08x", firstInt) + ")";
      duplicate.position(0);
    } else {
      firstIntSummary = "n/a";
    }
    final int sampleLength = Math.min(16, remaining);
    final byte[] sample = new byte[sampleLength];
    duplicate.get(sample, 0, sampleLength);
    return "pos="
        + position
        + ", limit="
        + limit
        + ", capacity="
        + capacity
        + ", remaining="
        + remaining
        + ", firstInt="
        + firstIntSummary
        + ", firstBytes="
        + bytesToHex(sample);
  }

  private String bytesToHex(final byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return "<empty>";
    }
    final StringBuilder builder = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      builder.append(String.format("%02x", b));
    }
    return builder.toString();
  }
}
