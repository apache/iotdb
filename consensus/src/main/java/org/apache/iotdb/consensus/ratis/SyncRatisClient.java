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
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.commons.client.BaseClientFactory;
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SyncRatisClient {
  private final Logger logger = LoggerFactory.getLogger(SyncRatisClient.class);
  private final RaftGroup serveGroup;
  private final RaftClient raftClient;
  private final ClientManager<RaftGroup, SyncRatisClient> clientManager;

  public SyncRatisClient(
      RaftGroup serveGroup,
      RaftClient client,
      ClientManager<RaftGroup, SyncRatisClient> clientManager) {
    this.serveGroup = serveGroup;
    this.raftClient = client;
    this.clientManager = clientManager;
  }

  public RaftClient getRaftClient() {
    return raftClient;
  }

  public void close() {
    try {
      raftClient.close();
    } catch (IOException e) {
      logger.warn("cannot close raft client ", e);
    }
  }

  public void returnSelf() {
    if (clientManager != null) {
      clientManager.returnClient(serveGroup, this);
    }
  }

  public static class Factory extends BaseClientFactory<RaftGroup, SyncRatisClient> {

    private final RaftProperties raftProperties;
    private final RaftClientRpc clientRpc;

    public Factory(
        ClientManager<RaftGroup, SyncRatisClient> clientManager,
        ClientFactoryProperty clientPoolProperty,
        RaftProperties raftProperties,
        RaftClientRpc clientRpc) {
      super(clientManager, clientPoolProperty);
      this.raftProperties = raftProperties;
      this.clientRpc = clientRpc;
    }

    @Override
    public void destroyObject(RaftGroup key, PooledObject<SyncRatisClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<SyncRatisClient> makeObject(RaftGroup group) throws Exception {
      return new DefaultPooledObject<>(
          new SyncRatisClient(
              group,
              RaftClient.newBuilder()
                  .setProperties(raftProperties)
                  .setRaftGroup(group)
                  .setClientRpc(clientRpc)
                  .build(),
              clientManager));
    }

    @Override
    public boolean validateObject(RaftGroup key, PooledObject<SyncRatisClient> pooledObject) {
      return true;
    }
  }
}
