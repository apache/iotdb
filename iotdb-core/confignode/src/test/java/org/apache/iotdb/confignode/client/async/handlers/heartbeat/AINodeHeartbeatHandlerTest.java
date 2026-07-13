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

package org.apache.iotdb.confignode.client.async.handlers.heartbeat;

import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatResp;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeoutException;

public class AINodeHeartbeatHandlerTest {

  private static final int AI_NODE_ID = 1;

  @Test
  public void successfulResponseShouldKeepProcessingUntilHandlerReturnsTest() {
    TrackingLoadCache loadCache = new TrackingLoadCache();
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(loadManager.getLoadCache()).thenReturn(loadCache);
    AINodeHeartbeatHandler handler = new AINodeHeartbeatHandler(AI_NODE_ID, loadManager);
    Assert.assertFalse(loadCache.checkAndSetHeartbeatProcessing(AI_NODE_ID));

    handler.onComplete(new TAIHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus()));

    Assert.assertTrue(loadCache.wasProcessingAfterCaching());
    Assert.assertFalse(loadCache.checkAndSetHeartbeatProcessing(AI_NODE_ID));

    // A duplicate terminal signal from the old handler must not release the next request.
    handler.onError(new TimeoutException());
    Assert.assertTrue(loadCache.checkAndSetHeartbeatProcessing(AI_NODE_ID));
  }

  @Test
  public void malformedResponseShouldResetHeartbeatProcessingTest() {
    LoadCache loadCache = new LoadCache();
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(loadManager.getLoadCache()).thenReturn(loadCache);
    AINodeHeartbeatHandler handler = new AINodeHeartbeatHandler(AI_NODE_ID, loadManager);
    Assert.assertFalse(loadCache.checkAndSetHeartbeatProcessing(AI_NODE_ID));

    try {
      handler.onComplete(new TAIHeartbeatResp(System.nanoTime(), "InvalidStatus"));
      Assert.fail();
    } catch (RuntimeException ignored) {
      // Expected because the malformed status cannot be converted to a NodeHeartbeatSample.
    }

    Assert.assertFalse(loadCache.checkAndSetHeartbeatProcessing(AI_NODE_ID));
  }

  @Test
  public void errorShouldResetHeartbeatProcessingTest() {
    LoadCache loadCache = new LoadCache();
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(loadManager.getLoadCache()).thenReturn(loadCache);
    AINodeHeartbeatHandler handler = new AINodeHeartbeatHandler(AI_NODE_ID, loadManager);
    Assert.assertFalse(loadCache.checkAndSetHeartbeatProcessing(AI_NODE_ID));

    handler.onError(new TimeoutException());

    Assert.assertFalse(loadCache.checkAndSetHeartbeatProcessing(AI_NODE_ID));
  }

  private static class TrackingLoadCache extends LoadCache {

    private boolean processingAfterCaching;

    @Override
    public void cacheAINodeHeartbeatSample(int nodeId, NodeHeartbeatSample sample) {
      super.cacheAINodeHeartbeatSample(nodeId, sample);
      processingAfterCaching = checkAndSetHeartbeatProcessing(nodeId);
    }

    private boolean wasProcessingAfterCaching() {
      return processingAfterCaching;
    }
  }
}
