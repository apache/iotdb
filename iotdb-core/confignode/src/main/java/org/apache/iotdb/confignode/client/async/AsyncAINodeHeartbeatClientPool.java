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

import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatReq;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncAINodeInternalServiceClient;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.AINodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

/** Asynchronously send RPC requests to AINodes. */
public class AsyncAINodeHeartbeatClientPool {

  private final IClientManager<TEndPoint, AsyncAINodeInternalServiceClient> clientManager;

  private AsyncAINodeHeartbeatClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncAINodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncAINodeHeartbeatServiceClientPoolFactory(
                    ConfigNodeDescriptor.getInstance().getConf().getSelectorNumOfClientManager()));
  }

  /**
   * Only used in LoadManager.
   *
   * @param endPoint The specific DataNode
   */
  public void getAINodeHeartBeat(
      TEndPoint endPoint, TAIHeartbeatReq req, AINodeHeartbeatHandler handler) {
    AsyncAINodeInternalServiceClient client = null;
    boolean dispatched = false;
    try {
      client = clientManager.borrowClient(endPoint);
      client.getAIHeartbeat(req, handler);
      dispatched = true;
    } catch (Exception e) {
      handleError(handler, e);
    } finally {
      // After the async call is dispatched, the client's onComplete/onError callback is
      // responsible for returning the client. If the RPC was not dispatched (exception
      // before/during the call), the client must be returned here to prevent pool leakage.
      if (!dispatched && client != null && clientManager instanceof ClientManager) {
        ((ClientManager<TEndPoint, AsyncAINodeInternalServiceClient>) clientManager)
            .returnClient(endPoint, client);
      }
    }
  }

  private void handleError(final AINodeHeartbeatHandler handler, final Exception e) {
    try {
      handler.onError(e);
    } catch (final Exception ignore) {
      // Ignore handler failures in heartbeat best-effort path.
    }
  }

  private static class AsyncAINodeHeartbeatClientPoolHolder {

    private static final AsyncAINodeHeartbeatClientPool INSTANCE =
        new AsyncAINodeHeartbeatClientPool();

    private AsyncAINodeHeartbeatClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncAINodeHeartbeatClientPool getInstance() {
    return AsyncAINodeHeartbeatClientPoolHolder.INSTANCE;
  }
}
