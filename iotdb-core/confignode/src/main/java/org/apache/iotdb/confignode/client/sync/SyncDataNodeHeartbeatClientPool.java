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

package org.apache.iotdb.confignode.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

/** Synchronously send RPC requests to DataNodes. See queryengine.thrift for more details. */
public class SyncDataNodeHeartbeatClientPool {

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  private SyncDataNodeHeartbeatClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeHeartbeatServiceClientPoolFactory());
  }

  /**
   * Only used in LoadManager.
   *
   * @param endPoint The specific DataNode
   */
  public void getDataNodeHeartBeat(
      TEndPoint endPoint, TDataNodeHeartbeatReq req, DataNodeHeartbeatHandler handler) {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      TDataNodeHeartbeatResp resp = client.getDataNodeHeartBeat(req);
      handler.onComplete(resp);
    } catch (Exception e) {
      handler.onError(e);
    }
  }

  private static class SyncDataNodeHeartbeatClientPoolHolder {

    private static final SyncDataNodeHeartbeatClientPool INSTANCE =
        new SyncDataNodeHeartbeatClientPool();

    private SyncDataNodeHeartbeatClientPoolHolder() {
      // Empty constructor
    }
  }

  public static SyncDataNodeHeartbeatClientPool getInstance() {
    return SyncDataNodeHeartbeatClientPoolHolder.INSTANCE;
  }
}
