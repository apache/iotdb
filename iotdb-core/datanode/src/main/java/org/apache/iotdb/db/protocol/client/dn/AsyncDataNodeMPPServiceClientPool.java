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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncDataNodeMPPServiceClientPool
    extends AsyncDataNodeClientPool<AsyncDataNodeMPPDataExchangeServiceClient> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncDataNodeMPPServiceClientPool.class);

  public AsyncDataNodeMPPServiceClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeMPPDataExchangeServiceClientPoolFactory());
  }

  @Override
  void sendAsyncRequestToDataNode(
      AsyncDataNodeRequestContext<?, ?> clientHandler,
      int requestId,
      TDataNodeLocation targetDataNode,
      int retryCount) {
    try {
      AsyncDataNodeMPPDataExchangeServiceClient client;
      client = clientManager.borrowClient(targetDataNode.getInternalEndPoint());
      Object req = clientHandler.getRequest(requestId);
      AbstractAsyncRPCHandler<?> handler =
          clientHandler.createAsyncRPCHandler(requestId, targetDataNode);
      AsyncTSStatusRPCHandler defaultHandler = (AsyncTSStatusRPCHandler) handler;

      switch (clientHandler.getRequestType()) {
        case TEST_CONNECTION:
          client.testConnection(defaultHandler);
          break;
        default:
          LOGGER.error(
              "Unexpected DataNode Request Type: {} when sendAsyncRequestToDataNode",
              clientHandler.getRequestType());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on DataNode {}, because {}, retrying {}...",
          clientHandler.getRequestType(),
          targetDataNode.getInternalEndPoint(),
          e.getMessage(),
          retryCount);
    }
  }

  private static class ClientPoolHolder {

    private static final AsyncDataNodeMPPServiceClientPool INSTANCE =
        new AsyncDataNodeMPPServiceClientPool();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncDataNodeMPPServiceClientPool getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
