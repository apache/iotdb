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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBSslSyncConnector;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.client.IoTDBDataNodeSyncClientManager;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class IoTDBDataNodeSyncConnector extends IoTDBSslSyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataNodeSyncConnector.class);

  protected IoTDBDataNodeSyncClientManager clientManager;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final IoTDBConfig iotdbConfig = IoTDBDescriptor.getInstance().getConfig();
    final Set<TEndPoint> givenNodeUrls = parseNodeUrls(validator.getParameters());

    validator.validate(
        empty -> {
          try {
            // Ensure the sink doesn't point to the thrift receiver on DataNode itself
            return !NodeUrlUtils.containsLocalAddress(
                givenNodeUrls.stream()
                    .filter(tEndPoint -> tEndPoint.getPort() == iotdbConfig.getRpcPort())
                    .map(TEndPoint::getIp)
                    .collect(Collectors.toList()));
          } catch (final UnknownHostException e) {
            LOGGER.warn("Unknown host when checking pipe sink IP.", e);
            return false;
          }
        },
        String.format(
            "One of the endpoints %s of the receivers is pointing back to the thrift receiver %s on sender itself, "
                + "or unknown host when checking pipe sink IP.",
            givenNodeUrls, new TEndPoint(iotdbConfig.getRpcAddress(), iotdbConfig.getRpcPort())));
  }

  @Override
  protected IoTDBSyncClientManager constructClient(
      final List<TEndPoint> nodeUrls,
      final boolean useSSL,
      final String trustStorePath,
      final String trustStorePwd,
      final boolean useLeaderCache,
      final String loadBalanceStrategy,
      final boolean shouldReceiverConvertOnTypeMismatch,
      final String loadTsFileStrategy) {
    clientManager =
        new IoTDBDataNodeSyncClientManager(
            nodeUrls,
            useSSL,
            trustStorePath,
            trustStorePwd,
            useLeaderCache,
            loadBalanceStrategy,
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy);
    return clientManager;
  }

  protected void doTransferWrapper(
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent) throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionWritePlanEvent.increaseReferenceCount(
        IoTDBDataNodeSyncConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeSchemaRegionWritePlanEvent);
    } finally {
      pipeSchemaRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBDataNodeSyncConnector.class.getName(), false);
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
