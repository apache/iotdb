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
package org.apache.iotdb.confignode.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.client.handlers.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncConfigNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncConfigNodeClientPool.class);

  private final IClientManager<TEndPoint, AsyncConfigNodeIServiceClient> clientManager;

  private AsyncConfigNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncConfigNodeIServiceClient>()
            .createClientManager(
                new DataNodeClientPoolFactory.AsyncConfigNodeIServiceClientPoolFactory());
  }

  /**
   * Only used in LoadManager
   *
   * @param endPoint The specific ConfigNode
   */
  public void getConfigNodeHeartBeat(
      TEndPoint endPoint, long timestamp, ConfigNodeHeartbeatHandler handler) {
    AsyncConfigNodeIServiceClient client;
    try {
      client = clientManager.borrowClient(endPoint);
      client.getConfigNodeHeartBeat(timestamp, handler);
    } catch (Exception e) {
      LOGGER.error("Asking ConfigNode: {}, for heartbeat failed", endPoint, e);
    }
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class AsyncConfigNodeClientPoolHolder {

    private static final AsyncConfigNodeClientPool INSTANCE = new AsyncConfigNodeClientPool();

    private AsyncConfigNodeClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncConfigNodeClientPool getInstance() {
    return AsyncConfigNodeClientPool.AsyncConfigNodeClientPoolHolder.INSTANCE;
  }
}
