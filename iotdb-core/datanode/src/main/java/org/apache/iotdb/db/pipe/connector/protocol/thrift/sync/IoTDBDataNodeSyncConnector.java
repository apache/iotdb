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
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBSyncSslConnector;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.event.common.schema.PipeWritePlanNodeEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public abstract class IoTDBDataNodeSyncConnector extends IoTDBSyncSslConnector {
  @Override
  protected IoTDBThriftSyncClientManager constructClient(
      List<TEndPoint> nodeUrls,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      boolean useLeaderCache) {
    return new IoTDBThriftSyncClientDataNodeManager(
        nodeUrls, useSSL, trustStorePath, trustStorePwd, useLeaderCache);
  }

  protected void doTransfer(PipeWritePlanNodeEvent pipeWritePlanNodeEvent) throws PipeException {
    Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus = clientManager.getClient();
    final TPipeTransferResp resp;

    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferPlanNodeReq.toTPipeTransferReq(pipeWritePlanNodeEvent.getPlanNode()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer pipe write schema plan event, because %s.",
              e.getMessage()),
          e);
    }
    final TSStatus status = resp.getStatus();
    receiverStatusHandler.handleReceiverStatus(
        status,
        String.format(
            "Transfer PipeWriteSchemaPlanEvent %s error, result status %s",
            pipeWritePlanNodeEvent, status),
        pipeWritePlanNodeEvent.getPlanNode().toString());
  }
}
