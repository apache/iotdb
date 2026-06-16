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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;

/** Asynchronously send RPC requests to DataNodes. See queryengine.thrift for more details. */
public class AsyncDataNodeHeartbeatClientPool {

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager;

  private AsyncDataNodeHeartbeatClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeHeartbeatServiceClientPoolFactory(
                    ConfigNodeDescriptor.getInstance().getConf().getSelectorNumOfClientManager()));
  }

  /**
   * Only used in LoadManager.
   *
   * @param endPoint The specific DataNode
   */
  public void getDataNodeHeartBeat(
      TEndPoint endPoint, TDataNodeHeartbeatReq req, DataNodeHeartbeatHandler handler) {
    AsyncDataNodeInternalServiceClient client = null;
    boolean dispatched = false;
    try {
      client = clientManager.borrowClient(endPoint);
      client.getDataNodeHeartBeat(req, handler);
      dispatched = true;
    } catch (Exception e) {
      handleError(handler, e);
    } finally {
      returnClientIfNotDispatched(endPoint, client, dispatched);
    }
  }

  // After the async call is dispatched, the client's onComplete/onError callback is responsible
  // for returning the client. If the RPC was not dispatched (exception before/during the call),
  // the client must be returned here to prevent pool leakage.
  private void returnClientIfNotDispatched(
      TEndPoint endPoint, AsyncDataNodeInternalServiceClient client, boolean dispatched) {
    if (!dispatched && client != null && clientManager instanceof ClientManager) {
      ((ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>) clientManager)
          .returnClient(endPoint, client);
    }
  }

  private void handleError(final DataNodeHeartbeatHandler handler, final Exception e) {
    try {
      handler.onError(e);
    } catch (final Exception ignore) {
      // Ignore handler failures in heartbeat best-effort path.
    }
  }

  private static class AsyncDataNodeHeartbeatClientPoolHolder {

    private static final AsyncDataNodeHeartbeatClientPool INSTANCE =
        new AsyncDataNodeHeartbeatClientPool();

    private AsyncDataNodeHeartbeatClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncDataNodeHeartbeatClientPool getInstance() {
    return AsyncDataNodeHeartbeatClientPoolHolder.INSTANCE;
  }
}
