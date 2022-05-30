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
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RatisClient {
  private final Logger logger = LoggerFactory.getLogger(RatisClient.class);
  private final RaftGroup serveGroup;
  private final RaftClient raftClient;
  private final ClientManager<RaftGroup, RatisClient> clientManager;

  public RatisClient(
      RaftGroup serveGroup,
      RaftClient client,
      ClientManager<RaftGroup, RatisClient> clientManager) {
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

  public static class Factory extends BaseClientFactory<RaftGroup, RatisClient> {

    private final RaftProperties raftProperties;
    private final RaftClientRpc clientRpc;

    public Factory(
        ClientManager<RaftGroup, RatisClient> clientManager,
        ClientFactoryProperty clientPoolProperty,
        RaftProperties raftProperties,
        RaftClientRpc clientRpc) {
      super(clientManager, clientPoolProperty);
      this.raftProperties = raftProperties;
      this.clientRpc = clientRpc;
    }

    @Override
    public void destroyObject(RaftGroup key, PooledObject<RatisClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<RatisClient> makeObject(RaftGroup group) throws Exception {
      return new DefaultPooledObject<>(
          new RatisClient(
              group,
              RaftClient.newBuilder()
                  .setProperties(raftProperties)
                  .setRaftGroup(group)
                  .setRetryPolicy(
                      ExponentialBackoffRetry.newBuilder()
                          .setBaseSleepTime(TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))
                          .setMaxSleepTime(TimeDuration.valueOf(10, TimeUnit.SECONDS))
                          .setMaxAttempts(10)
                          .build())
                  .setClientRpc(clientRpc)
                  .build(),
              clientManager));
    }

    @Override
    public boolean validateObject(RaftGroup key, PooledObject<RatisClient> pooledObject) {
      return true;
    }
  }
}
