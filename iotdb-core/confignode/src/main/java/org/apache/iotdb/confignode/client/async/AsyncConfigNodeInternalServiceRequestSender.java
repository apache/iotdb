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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeInternalServiceClient;
import org.apache.iotdb.commons.client.gg.AsyncRequestContext;
import org.apache.iotdb.commons.client.gg.AsyncRequestSender;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.rpc.ConfigNodeAbstractAsyncRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.AsyncTSStatusRPCHandler2;
import org.apache.iotdb.confignode.client.async.handlers.rpc.SubmitTestConnectionTaskToConfigNodeRPCHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Asynchronously send RPC requests to ConfigNodes. See queryengine.thrift for more details. */
public class AsyncConfigNodeInternalServiceRequestSender extends AsyncRequestSender<ConfigNodeRequestType, TConfigNodeLocation, AsyncConfigNodeInternalServiceClient> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncConfigNodeInternalServiceRequestSender.class);

  private AsyncConfigNodeInternalServiceRequestSender() {
    super();
  }

  @Override
  protected void initClientManager() {
    clientManager =
            new IClientManager.Factory<TEndPoint, AsyncConfigNodeInternalServiceClient>()
                    .createClientManager(new ClientPoolFactory.AsyncConfigNodeInternalServiceClientPoolFactory());
  }

  @Override
  protected TEndPoint nodeLocationToEndPoint(TConfigNodeLocation configNodeLocation) {
    return configNodeLocation.getInternalEndPoint();
  }

  @Override
  protected void sendAsyncRequestToNode(
          AsyncRequestContext<?, ?, ConfigNodeRequestType, TConfigNodeLocation> requestContext, int requestId, TConfigNodeLocation targetNode, int retryCount) {


    try {
      AsyncConfigNodeInternalServiceClient client;
      client = clientManager.borrowClient(nodeLocationToEndPoint(targetNode));
      Object req = requestContext.getRequest(requestId);
      ConfigNodeAbstractAsyncRPCHandler<?> handler =
              ConfigNodeAbstractAsyncRPCHandler.buildHandler(requestContext, requestId, targetNode);
      AsyncTSStatusRPCHandler2 defaultHandler = null;
      if (handler instanceof AsyncTSStatusRPCHandler2) {
        defaultHandler = (AsyncTSStatusRPCHandler2) handler;
      }
      switch (requestContext.getRequestType()) {
        case SUBMIT_TEST_CONNECTION_TASK:
          client.submitTestConnectionTask(
              (TNodeLocations) req, (SubmitTestConnectionTaskToConfigNodeRPCHandler) handler);
          break;
        case TEST_CONNECTION:
          client.testConnection(defaultHandler);
          break;
        default:
          LOGGER.error(
              "Unexpected ConfigNode Request Type: {} when sendAsyncRequestToConfigNode",
              requestContext.getRequestType());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on ConfigNode {}, because {}, retrying {}...",
          requestContext.getRequestType(),
          nodeLocationToEndPoint(targetNode),
          e.getMessage(),
          retryCount);
    }
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class ClientPoolHolder {

    private static final AsyncConfigNodeInternalServiceRequestSender INSTANCE = new AsyncConfigNodeInternalServiceRequestSender();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncConfigNodeInternalServiceRequestSender getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
