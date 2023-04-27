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

import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.factory.BaseClientFactory;
import org.apache.iotdb.consensus.config.RatisConfig;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

class RatisClient {

  private final Logger logger = LoggerFactory.getLogger(RatisClient.class);
  private final RaftGroup serveGroup;
  private final RaftClient raftClient;
  private final ClientManager<RaftGroup, RatisClient> clientManager;

  RatisClient(
      RaftGroup serveGroup,
      RaftClient client,
      ClientManager<RaftGroup, RatisClient> clientManager) {
    this.serveGroup = serveGroup;
    this.raftClient = client;
    this.clientManager = clientManager;
  }

  RaftClient getRaftClient() {
    return raftClient;
  }

  private void close() {
    try {
      raftClient.close();
    } catch (IOException e) {
      logger.warn("cannot close raft client ", e);
    }
  }

  void returnSelf() {
    clientManager.returnClient(serveGroup, this);
  }

  static class Factory extends BaseClientFactory<RaftGroup, RatisClient> {

    private final RaftProperties raftProperties;
    private final RaftClientRpc clientRpc;
    private final RatisConfig.Client config;

    public Factory(
        ClientManager<RaftGroup, RatisClient> clientManager,
        RaftProperties raftProperties,
        RaftClientRpc clientRpc,
        RatisConfig.Client config) {
      super(clientManager);
      this.raftProperties = raftProperties;
      this.clientRpc = clientRpc;
      this.config = config;
    }

    @Override
    public void destroyObject(RaftGroup key, PooledObject<RatisClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<RatisClient> makeObject(RaftGroup group) {
      return new DefaultPooledObject<>(
          new RatisClient(
              group,
              RaftClient.newBuilder()
                  .setProperties(raftProperties)
                  .setRaftGroup(group)
                  .setRetryPolicy(new RatisRetryPolicy(config))
                  .setClientRpc(clientRpc)
                  .build(),
              clientManager));
    }

    @Override
    public boolean validateObject(RaftGroup key, PooledObject<RatisClient> pooledObject) {
      return true;
    }
  }

  /**
   * RatisRetryPolicy is similar to ExceptionDependentRetry 1. By default, use
   * ExponentialBackoffRetry to handle request failure 2. If unexpected IOException is caught,
   * immediately fail the request and let application choose retry action.
   *
   * <p>potential IOException can be categorized into expected and unexpected 1. expected, instance
   * of RaftException, like LeaderNotReady / GroupMisMatch etc. 2. unexpected, IOException which is
   * not an instance of RaftException
   */
  private static class RatisRetryPolicy implements RetryPolicy {

    private static final Logger logger = LoggerFactory.getLogger(RatisClient.class);
    private final RetryPolicy defaultPolicy;

    RatisRetryPolicy(RatisConfig.Client config) {
      defaultPolicy =
          ExponentialBackoffRetry.newBuilder()
              .setBaseSleepTime(
                  TimeDuration.valueOf(
                      config.getClientRetryInitialSleepTimeMs(), TimeUnit.MILLISECONDS))
              .setMaxSleepTime(
                  TimeDuration.valueOf(
                      config.getClientRetryMaxSleepTimeMs(), TimeUnit.MILLISECONDS))
              .setMaxAttempts(config.getClientMaxRetryAttempt())
              .build();
    }

    @Override
    public Action handleAttemptFailure(Event event) {

      if (event.getCause() instanceof IOException && !(event.getCause() instanceof RaftException)) {
        // unexpected. may be caused by statemachine.
        logger.info("raft client request failed and caught exception: ", event.getCause());
        return NO_RETRY_ACTION;
      }

      return defaultPolicy.handleAttemptFailure(event);
    }
  }
}
