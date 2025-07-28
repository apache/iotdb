/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.sink.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSslSyncSink;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeSyncClientManager;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public abstract class IoTDBDataNodeSyncSink extends IoTDBSslSyncSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataNodeSyncSink.class);

  protected IoTDBDataNodeSyncClientManager clientManager;

  @Override
  protected IoTDBSyncClientManager constructClient(
      final List<TEndPoint> nodeUrls,
      final String username,
      final String password,
      final boolean useSSL,
      final String trustStorePath,
      final String trustStorePwd,
      final boolean useLeaderCache,
      final String loadBalanceStrategy,
      final boolean shouldReceiverConvertOnTypeMismatch,
      final String loadTsFileStrategy,
      final boolean validateTsFile,
      final boolean shouldMarkAsPipeRequest) {
    clientManager =
        new IoTDBDataNodeSyncClientManager(
            nodeUrls,
            username,
            password,
            useSSL,
            Objects.nonNull(trustStorePath) ? IoTDBConfig.addDataHomeDir(trustStorePath) : null,
            trustStorePwd,
            useLeaderCache,
            loadBalanceStrategy,
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy,
            validateTsFile,
            shouldMarkAsPipeRequest);
    return clientManager;
  }

  protected void doTransferWrapper(
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent) throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionWritePlanEvent.increaseReferenceCount(
        IoTDBDataNodeSyncSink.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeSchemaRegionWritePlanEvent);
    } finally {
      pipeSchemaRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBDataNodeSyncSink.class.getName(), false);
    }
  }

  protected void doTransfer(final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    final TPipeTransferResp resp;
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferPlanNodeReq.toTPipeTransferReq(
                  pipeSchemaRegionWritePlanEvent.getPlanNode()));
      rateLimitIfNeeded(
          pipeSchemaRegionWritePlanEvent.getPipeName(),
          pipeSchemaRegionWritePlanEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer schema region write plan %s, because %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Only handle the failed statuses to avoid string format performance overhead
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && resp.getStatus().getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "Transfer data node write plan %s error, result status %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), status),
          pipeSchemaRegionWritePlanEvent.getPlanNode().toString());
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Successfully transferred schema event {}.", pipeSchemaRegionWritePlanEvent);
    }
  }
}
