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

package org.apache.iotdb.confignode.manager.load.service;

import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatReq;
import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.AINodeHeartbeatHandler;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class HeartbeatServiceTest {

  private static final int AI_NODE_ID = 1;

  @Test
  public void ainodeHeartbeatShouldNotOverlapTest() {
    LoadCache loadCache = new LoadCache();
    IManager configManager = Mockito.mock(IManager.class);
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(configManager.getLoadManager()).thenReturn(loadManager);
    Mockito.when(loadManager.getLoadCache()).thenReturn(loadCache);
    TestHeartbeatService heartbeatService = new TestHeartbeatService(configManager, loadCache);

    TAINodeConfiguration aiNodeConfiguration =
        new TAINodeConfiguration()
            .setLocation(
                new TAINodeLocation()
                    .setAiNodeId(AI_NODE_ID)
                    .setInternalEndPoint(new TEndPoint("127.0.0.1", 10810)));
    TAIHeartbeatReq heartbeatReq = new TAIHeartbeatReq(System.nanoTime(), false);

    heartbeatService.pingRegisteredAINodes(
        heartbeatReq, Collections.singletonList(aiNodeConfiguration));
    Assert.assertEquals(1, heartbeatService.getDispatchCount());

    heartbeatService.pingRegisteredAINodes(
        heartbeatReq, Collections.singletonList(aiNodeConfiguration));
    Assert.assertEquals(1, heartbeatService.getDispatchCount());

    heartbeatService
        .getLastHandler()
        .onComplete(new TAIHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus()));
    heartbeatService.pingRegisteredAINodes(
        heartbeatReq, Collections.singletonList(aiNodeConfiguration));
    Assert.assertEquals(2, heartbeatService.getDispatchCount());
  }

  @Test
  public void synchronousDispatchFailureShouldResetHeartbeatProcessingTest() {
    LoadCache loadCache = new LoadCache();
    IManager configManager = Mockito.mock(IManager.class);
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(configManager.getLoadManager()).thenReturn(loadManager);
    Mockito.when(loadManager.getLoadCache()).thenReturn(loadCache);
    TestHeartbeatService heartbeatService = new TestHeartbeatService(configManager, loadCache);
    TAINodeConfiguration aiNodeConfiguration =
        new TAINodeConfiguration()
            .setLocation(
                new TAINodeLocation()
                    .setAiNodeId(AI_NODE_ID)
                    .setInternalEndPoint(new TEndPoint("127.0.0.1", 10810)));
    TAIHeartbeatReq heartbeatReq = new TAIHeartbeatReq(System.nanoTime(), false);
    heartbeatService.setThrowOnDispatch(true);

    try {
      heartbeatService.pingRegisteredAINodes(
          heartbeatReq, Collections.singletonList(aiNodeConfiguration));
      Assert.fail();
    } catch (RuntimeException ignored) {
      // Expected from the synchronous dispatch path.
    }

    heartbeatService.setThrowOnDispatch(false);
    heartbeatService.pingRegisteredAINodes(
        heartbeatReq, Collections.singletonList(aiNodeConfiguration));
    Assert.assertEquals(2, heartbeatService.getDispatchCount());
  }

  private static class TestHeartbeatService extends HeartbeatService {

    private int dispatchCount;
    private AINodeHeartbeatHandler lastHandler;
    private boolean throwOnDispatch;

    private TestHeartbeatService(IManager configManager, LoadCache loadCache) {
      super(configManager, loadCache);
    }

    @Override
    protected void sendAINodeHeartbeat(
        TEndPoint endPoint, TAIHeartbeatReq heartbeatReq, AINodeHeartbeatHandler handler) {
      dispatchCount++;
      if (throwOnDispatch) {
        throw new RuntimeException();
      }
      lastHandler = handler;
    }

    private int getDispatchCount() {
      return dispatchCount;
    }

    private AINodeHeartbeatHandler getLastHandler() {
      return lastHandler;
    }

    private void setThrowOnDispatch(boolean throwOnDispatch) {
      this.throwOnDispatch = throwOnDispatch;
    }
  }
}
