/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConfigManagerLatestRegionRouteMapTest {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final long RETRY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
  private final AtomicLong nanoTime = new AtomicLong();
  private final LoadManager loadManager = mock(LoadManager.class);
  private final ConfigManager configManager = mock(ConfigManager.class);
  private long originalHeartbeatIntervalInMs;

  @Before
  public void setUp() throws InterruptedException {
    originalHeartbeatIntervalInMs = CONF.getHeartbeatIntervalInMs();
    CONF.setHeartbeatIntervalInMs(200);
    doCallRealMethod().when(configManager).getLatestRegionRouteMap(any(LongSupplier.class));
    when(configManager.confirmLeader())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    when(configManager.getLoadManager()).thenReturn(loadManager);
    doAnswer(
            invocation -> {
              nanoTime.addAndGet(Math.min(RETRY_NANOS, (Long) invocation.getArgument(0)));
              return null;
            })
        .when(configManager)
        .waitForRegionRouteMap(anyLong());
  }

  @After
  public void tearDown() {
    CONF.setHeartbeatIntervalInMs(originalHeartbeatIntervalInMs);
  }

  @Test
  public void testWaitsUntilRegionRouteMapIsReady() {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0);
    final Map<TConsensusGroupId, TRegionReplicaSet> routeMap =
        Map.of(regionId, new TRegionReplicaSet(regionId, List.of()));
    when(loadManager.getRegionPriorityMapSnapshot()).thenReturn(null, new Pair<>(200L, routeMap));

    final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
    assertEquals(routeMap, response.getRegionRouteMap());
    assertEquals(200L, response.getTimestamp());
    verify(loadManager, times(2)).getRegionPriorityMapSnapshot();
  }

  @Test
  public void testReturnsRetryableStatusWhenRegionRouteMapIsNotReady() {
    final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);

    assertWarmingWithoutRoutes(response);
    assertEquals(
        ManagerMessages
            .MESSAGE_THE_CURRENT_CONFIGNODE_LEADER_CANNOT_PROVIDE_THE_REGION_ROUTE_MAP_YET_RETRY_LATER_FA500387,
        response.getStatus().getMessage());
    assertEquals(RETRY_NANOS * 8, nanoTime.get());
    verify(loadManager, times(8)).getRegionPriorityMapSnapshot();
  }

  @Test
  public void testDeadlineIncludesRouteMapValidationTime() throws InterruptedException {
    when(loadManager.getRegionPriorityMapSnapshot())
        .thenAnswer(
            invocation -> {
              nanoTime.set(TimeUnit.SECONDS.toNanos(1));
              return new Pair<>(200L, Map.of());
            });

    assertWarmingWithoutRoutes(configManager.getLatestRegionRouteMap(nanoTime::get));
    verify(configManager, never()).waitForRegionRouteMap(anyLong());
  }

  @Test
  public void testWaitTimeoutSaturatesForLargeHeartbeatInterval() {
    assertEquals(100, ConfigManager.getLatestRegionRouteMapWaitTimeoutInMs(Long.MIN_VALUE));
    assertEquals(100, ConfigManager.getLatestRegionRouteMapWaitTimeoutInMs(0));
    assertEquals(100, ConfigManager.getLatestRegionRouteMapWaitTimeoutInMs(1));
    assertEquals(400, ConfigManager.getLatestRegionRouteMapWaitTimeoutInMs(100));
    assertEquals(30_000, ConfigManager.getLatestRegionRouteMapWaitTimeoutInMs(Long.MAX_VALUE));
  }

  @Test
  public void testReturnsSuccessWhenThereAreNoRegions() {
    when(loadManager.getRegionPriorityMapSnapshot()).thenReturn(new Pair<>(200L, Map.of()));

    final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
    assertTrue(response.isSetRegionRouteMap());
    assertTrue(response.getRegionRouteMap().isEmpty());
    assertEquals(200L, response.getTimestamp());
  }

  @Test
  public void testReturnsLeaderStatusWithoutReadingRouteMap() {
    when(configManager.confirmLeader())
        .thenReturn(new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()));

    final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);

    assertEquals(
        TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), response.getStatus().getCode());
    verify(loadManager, never()).getRegionPriorityMapSnapshot();
  }

  @Test
  public void testReturnsRedirectWhenLeadershipChangesWhileWaiting() throws InterruptedException {
    final TSStatus redirect = new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
    when(configManager.confirmLeader())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), redirect);
    when(loadManager.getRegionPriorityMapSnapshot()).thenReturn(null);

    final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);

    assertEquals(
        TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), response.getStatus().getCode());
    assertFalse(response.isSetTimestamp());
    assertFalse(response.isSetRegionRouteMap());
    verify(loadManager, times(1)).getRegionPriorityMapSnapshot();
    verify(configManager, times(1)).waitForRegionRouteMap(anyLong());
  }

  @Test
  public void testDoesNotReturnSnapshotAfterLeadershipChanges() throws InterruptedException {
    final TSStatus redirect = new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
    final Map<TConsensusGroupId, TRegionReplicaSet> routeMap = Map.of();
    when(configManager.confirmLeader())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), redirect);
    when(loadManager.getRegionPriorityMapSnapshot()).thenReturn(new Pair<>(200L, routeMap));

    final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);

    assertEquals(
        TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), response.getStatus().getCode());
    assertFalse(response.isSetTimestamp());
    assertFalse(response.isSetRegionRouteMap());
    verify(loadManager, times(1)).getRegionPriorityMapSnapshot();
    verify(configManager, never()).waitForRegionRouteMap(anyLong());
  }

  @Test
  public void testRechecksLeadershipWhenRouteMapWaitDeadlineExpires() {
    final TSStatus redirect = new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
    final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(800);
    final TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    when(configManager.confirmLeader())
        .thenReturn(
            success, success, success, success, success, success, success, success, redirect);
    when(loadManager.getRegionPriorityMapSnapshot()).thenReturn(null);

    final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);

    assertEquals(
        TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), response.getStatus().getCode());
    assertFalse(response.isSetTimestamp());
    assertFalse(response.isSetRegionRouteMap());
    assertEquals(timeoutNanos, nanoTime.get());
    verify(configManager, times(9)).confirmLeader();
    verify(loadManager, times(8)).getRegionPriorityMapSnapshot();
  }

  @Test
  public void testInterruptionReturnsRedirectAndRestoresInterruptFlag()
      throws InterruptedException {
    doCallRealMethod().when(configManager).waitForRegionRouteMap(anyLong());
    final boolean wasInterrupted = Thread.interrupted();
    Thread.currentThread().interrupt();
    try {
      final TRegionRouteMapResp response = configManager.getLatestRegionRouteMap(nanoTime::get);
      assertEquals(
          TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), response.getStatus().getCode());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void assertWarmingWithoutRoutes(TRegionRouteMapResp response) {
    assertEquals(
        TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode(), response.getStatus().getCode());
    assertFalse(response.isSetTimestamp());
    assertFalse(response.isSetRegionRouteMap());
  }
}
