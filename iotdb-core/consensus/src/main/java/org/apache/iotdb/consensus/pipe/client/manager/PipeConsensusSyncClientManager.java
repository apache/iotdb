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

package org.apache.iotdb.consensus.pipe.client.manager;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.consensus.config.PipeConsensusConfig.PipeConsensusRPCConfig;
import org.apache.iotdb.consensus.pipe.client.PipeConsensusClientPool.SyncPipeConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.pipe.client.SyncPipeConsensusServiceClient;

/**
 * Note: This class is shared by all pipeConsensusTasks of one leader to its peers in a consensus
 * group. And unlike pipe engine, PipeConsensus doesn't need to handshake before establish RPC
 * connection between leader and follower.
 *
 * <p>Usage: please use this class with <strong>TRY_WITH_RESOURCE</strong>
 *
 * <p>eg: {@code try (PipeConsensusSyncClientManager.getInstance().borrowClient(endpoint)) { // your
 * code } catch (Exception e) { // your code }}
 */
public class PipeConsensusSyncClientManager {

  private final IClientManager<TEndPoint, SyncPipeConsensusServiceClient> SYNC_CLIENT_MANAGER =
      new IClientManager.Factory<TEndPoint, SyncPipeConsensusServiceClient>()
          .createClientManager(
              new SyncPipeConsensusServiceClientPoolFactory(new PipeConsensusRPCConfig()));

  private PipeConsensusSyncClientManager() {
    // do nothing
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConsensusSyncClientManagerHolder {
    private static final PipeConsensusSyncClientManager INSTANCE_HOLDER =
        new PipeConsensusSyncClientManager();

    private PipeConsensusSyncClientManagerHolder() {}
  }

  public static IClientManager<TEndPoint, SyncPipeConsensusServiceClient> getInstance() {
    return PipeConsensusSyncClientManagerHolder.INSTANCE_HOLDER.SYNC_CLIENT_MANAGER;
  }
}
