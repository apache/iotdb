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
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;

/** Asynchronously send RPC requests to DataNodes. See mpp.thrift for more details. */
public class AsyncDataNodeHeartbeatClientPool {

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager;

  private AsyncDataNodeHeartbeatClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeHeartbeatServiceClientPoolFactory());
  }

  /**
   * Only used in LoadManager
   *
   * @param endPoint The specific DataNode
   */
  public void getDataNodeHeartBeat(
      TEndPoint endPoint, THeartbeatReq req, DataNodeHeartbeatHandler handler) {
    try {
      clientManager.borrowClient(endPoint).getDataNodeHeartBeat(req, handler);
    } catch (Exception ignore) {
      // Just ignore
    }
  }

  // TODO: Is the AsyncDataNodeHeartbeatClientPool must be a singleton?
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
