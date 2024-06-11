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

package org.apache.iotdb.db.protocol.client.dn;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.client.gg.AsyncRequestContext;
import org.apache.iotdb.commons.client.gg.AsyncRequestManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iotdb.db.protocol.client.dn.DataNodeToDataNodeRequestType.TEST_CONNECTION;

public class AsyncDataNodeMPPServiceRequestManager
    extends AsyncRequestManager<
        DataNodeToDataNodeRequestType,
        TDataNodeLocation,
        AsyncDataNodeMPPDataExchangeServiceClient> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncDataNodeMPPServiceRequestManager.class);

  public AsyncDataNodeMPPServiceRequestManager() {}

  @Override
  protected void initClientManager() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeMPPDataExchangeServiceClientPoolFactory());
  }

  @Override
  protected TEndPoint nodeLocationToEndPoint(TDataNodeLocation dataNodeLocation) {
    return null;
  }

  @Override
  protected void sendAsyncRequestToNode(
      AsyncRequestContext<?, ?, DataNodeToDataNodeRequestType, TDataNodeLocation> requestContext,
      int requestId,
      TDataNodeLocation targetNode,
      int retryCount) {
    try {
      AsyncDataNodeMPPDataExchangeServiceClient client;
      client = clientManager.borrowClient(targetNode.getInternalEndPoint());
      Object req = requestContext.getRequest(requestId);
      AsyncDataNodeRPCHandler<?> handler =
          AsyncDataNodeRPCHandler.createAsyncRPCHandler(requestContext, requestId, targetNode);
      AsyncTSStatusRPCHandler defaultHandler = (AsyncTSStatusRPCHandler) handler;

      switch (requestContext.getRequestType()) {
        case TEST_CONNECTION:
          client.testConnection(defaultHandler);
          break;
        default:
          throw new UnsupportedOperationException(
              "unsupported request type: " + requestContext.getRequestType());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on DataNode {}, because {}, retrying {}...",
          requestContext.getRequestType(),
          targetNode.getInternalEndPoint(),
          e.getMessage(),
          retryCount);
    }
  }

  private static class ClientPoolHolder {

    private static final AsyncDataNodeMPPServiceRequestManager INSTANCE =
        new AsyncDataNodeMPPServiceRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncDataNodeMPPServiceRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
