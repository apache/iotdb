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
import org.apache.iotdb.commons.client.async.AsyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.ConfigNodeHeartbeatHandler;

public class AsyncConfigNodeHeartbeatClientPool {

  private final IClientManager<TEndPoint, AsyncConfigNodeIServiceClient> clientManager;

  private AsyncConfigNodeHeartbeatClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncConfigNodeIServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncConfigNodeHeartbeatServiceClientPoolFactory());
  }

  /**
   * Only used in LoadManager
   *
   * @param endPoint The specific ConfigNode
   */
  public void getConfigNodeHeartBeat(
      TEndPoint endPoint, long timestamp, ConfigNodeHeartbeatHandler handler) {
    try {
      clientManager.borrowClient(endPoint).getConfigNodeHeartBeat(timestamp, handler);
    } catch (Exception ignore) {
      // Just ignore
    }
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class AsyncConfigNodeHeartbeatClientPoolHolder {

    private static final AsyncConfigNodeHeartbeatClientPool INSTANCE =
        new AsyncConfigNodeHeartbeatClientPool();

    private AsyncConfigNodeHeartbeatClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncConfigNodeHeartbeatClientPool getInstance() {
    return AsyncConfigNodeHeartbeatClientPoolHolder.INSTANCE;
  }
}
