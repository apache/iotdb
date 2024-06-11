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
import org.apache.iotdb.commons.client.async.AsyncDataNodeExternalServiceClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncDataNodeExternalServiceClientPool
    extends AsyncDataNodeClientPool<AsyncDataNodeExternalServiceClient> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncDataNodeExternalServiceClientPool.class);

  public AsyncDataNodeExternalServiceClientPool() {
    super();
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeExternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeExternalServiceClientPoolFactory());
  }

  @Override
  void sendAsyncRequestToDataNode(
      AsyncClientHandler<?, ?> clientHandler,
      int requestId,
      TDataNodeLocation targetDataNode,
      int retryCount) {
    {
      try {
        AsyncDataNodeExternalServiceClient client;
        client = clientManager.borrowClient(targetDataNode.getClientRpcEndPoint());
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
            targetDataNode.getClientRpcEndPoint(),
            e.getMessage(),
            retryCount);
      }
    }
  }

  private static class ClientPoolHolder {

    private static final AsyncDataNodeExternalServiceClientPool INSTANCE =
        new AsyncDataNodeExternalServiceClientPool();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncDataNodeExternalServiceClientPool getInstance() {
    return AsyncDataNodeExternalServiceClientPool.ClientPoolHolder.INSTANCE;
  }
}
