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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeAsyncClientManager;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeCacheLeaderClientManager;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeSyncClientManager;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class IoTDBDataNodeAsyncClientManagerTest {

  private static final List<String> WILDCARD_ADDRESSES =
      Arrays.asList("0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0", "0::0");

  @Test
  public void testReceiverAttributesShouldDifferentiateSkipIfNoPrivileges() throws Exception {
    final IoTDBDataNodeAsyncClientManager managerWithSkipIf =
        new IoTDBDataNodeAsyncClientManager(
            Collections.singletonList(new TEndPoint("127.0.0.1", 6667)),
            false,
            "round-robin",
            new UserEntity(1L, "user", "cli-host"),
            "password",
            true,
            "sync",
            true,
            true,
            false,
            true);
    final IoTDBDataNodeAsyncClientManager managerWithoutSkipIf =
        new IoTDBDataNodeAsyncClientManager(
            Collections.singletonList(new TEndPoint("127.0.0.1", 6667)),
            false,
            "round-robin",
            new UserEntity(1L, "user", "cli-host"),
            "password",
            true,
            "sync",
            true,
            true,
            false,
            false);

    try {
      Assert.assertNotEquals(
          getReceiverAttributes(managerWithSkipIf), getReceiverAttributes(managerWithoutSkipIf));
      Assert.assertNotSame(
          getEndPoint2Client(managerWithSkipIf), getEndPoint2Client(managerWithoutSkipIf));
    } finally {
      managerWithSkipIf.close();
      managerWithoutSkipIf.close();
    }
  }

  @Test
  public void testClientResourcesShouldDifferentiateEndPoints() throws Exception {
    final IoTDBDataNodeAsyncClientManager firstManager =
        new IoTDBDataNodeAsyncClientManager(
            Collections.singletonList(new TEndPoint("127.0.0.1", 6667)),
            false,
            "round-robin",
            new UserEntity(1L, "user", "cli-host"),
            "password",
            true,
            "sync",
            true,
            true,
            true,
            true);
    final IoTDBDataNodeAsyncClientManager secondManager =
        new IoTDBDataNodeAsyncClientManager(
            Collections.singletonList(new TEndPoint("127.0.0.2", 6667)),
            false,
            "round-robin",
            new UserEntity(1L, "user", "cli-host"),
            "password",
            true,
            "sync",
            true,
            true,
            true,
            true);

    try {
      Assert.assertEquals(
          getReceiverAttributes(firstManager), getReceiverAttributes(secondManager));
      Assert.assertNotEquals(
          getClientResourceKey(firstManager), getClientResourceKey(secondManager));
      Assert.assertNotSame(getEndPoint2Client(firstManager), getEndPoint2Client(secondManager));
      Assert.assertNotSame(getExecutor(firstManager), getExecutor(secondManager));
    } finally {
      firstManager.close();
      secondManager.close();
    }
  }

  @Test
  public void testAsyncManagerShouldIgnoreWildcardAndAcceptIPv6LeaderEndPoints() {
    final TEndPoint originalEndPoint = new TEndPoint("127.0.0.1", 6667);
    final List<TEndPoint> endPoints = new ArrayList<>(Collections.singletonList(originalEndPoint));
    final IoTDBDataNodeAsyncClientManager manager = createAsyncManager(endPoints);

    try {
      for (int i = 0; i < WILDCARD_ADDRESSES.size(); i++) {
        final String deviceId = "async-wildcard-device-" + i;
        manager.updateLeaderCache(deviceId, new TEndPoint(WILDCARD_ADDRESSES.get(i), 6667 + i));

        Assert.assertEquals(Collections.singletonList(originalEndPoint), endPoints);
        Assert.assertNull(
            IoTDBDataNodeCacheLeaderClientManager.LEADER_CACHE_MANAGER.getLeaderEndPoint(deviceId));
      }

      final String ipv6DeviceId = "async-ipv6-device";
      final TEndPoint ipv6EndPoint = new TEndPoint("::1", 6677);
      manager.updateLeaderCache(ipv6DeviceId, ipv6EndPoint);

      Assert.assertEquals(Arrays.asList(originalEndPoint, ipv6EndPoint), endPoints);
      Assert.assertEquals(
          ipv6EndPoint,
          IoTDBDataNodeCacheLeaderClientManager.LEADER_CACHE_MANAGER.getLeaderEndPoint(
              ipv6DeviceId));
    } finally {
      manager.close();
    }
  }

  @Test
  public void testSyncManagerShouldIgnoreWildcardAndAcceptIPv6LeaderEndPoints() {
    final TEndPoint originalEndPoint = new TEndPoint("127.0.0.1", 6667);
    final List<TEndPoint> endPoints = new ArrayList<>(Collections.singletonList(originalEndPoint));
    final TestIoTDBDataNodeSyncClientManager manager =
        new TestIoTDBDataNodeSyncClientManager(endPoints);

    try {
      for (int i = 0; i < WILDCARD_ADDRESSES.size(); i++) {
        final String deviceId = "sync-wildcard-device-" + i;
        manager.updateLeaderCache(deviceId, new TEndPoint(WILDCARD_ADDRESSES.get(i), 6667 + i));

        Assert.assertEquals(Collections.singletonList(originalEndPoint), endPoints);
        Assert.assertEquals(0, manager.getReconstructionCount());
        Assert.assertNull(
            IoTDBDataNodeCacheLeaderClientManager.LEADER_CACHE_MANAGER.getLeaderEndPoint(deviceId));
      }

      final String ipv6DeviceId = "sync-ipv6-device";
      final TEndPoint ipv6EndPoint = new TEndPoint("::1", 6677);
      manager.updateLeaderCache(ipv6DeviceId, ipv6EndPoint);

      Assert.assertEquals(Arrays.asList(originalEndPoint, ipv6EndPoint), endPoints);
      Assert.assertEquals(1, manager.getReconstructionCount());
      Assert.assertEquals(
          ipv6EndPoint,
          IoTDBDataNodeCacheLeaderClientManager.LEADER_CACHE_MANAGER.getLeaderEndPoint(
              ipv6DeviceId));
    } finally {
      manager.close();
    }
  }

  private static IoTDBDataNodeAsyncClientManager createAsyncManager(
      final List<TEndPoint> endPoints) {
    return new IoTDBDataNodeAsyncClientManager(
        endPoints,
        true,
        "round-robin",
        new UserEntity(1L, "user", "cli-host"),
        "password",
        true,
        "sync",
        true,
        true,
        false,
        true);
  }

  private static String getReceiverAttributes(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field =
        IoTDBDataNodeAsyncClientManager.class.getDeclaredField("receiverAttributes");
    field.setAccessible(true);
    return (String) field.get(manager);
  }

  private static String getClientResourceKey(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field = IoTDBDataNodeAsyncClientManager.class.getDeclaredField("clientResourceKey");
    field.setAccessible(true);
    return (String) field.get(manager);
  }

  private static Object getEndPoint2Client(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field = IoTDBDataNodeAsyncClientManager.class.getDeclaredField("endPoint2Client");
    field.setAccessible(true);
    return field.get(manager);
  }

  private static ExecutorService getExecutor(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field = IoTDBDataNodeAsyncClientManager.class.getDeclaredField("executor");
    field.setAccessible(true);
    return (ExecutorService) field.get(manager);
  }

  private static class TestIoTDBDataNodeSyncClientManager extends IoTDBDataNodeSyncClientManager {

    private int reconstructionCount;

    private TestIoTDBDataNodeSyncClientManager(final List<TEndPoint> endPoints) {
      super(
          endPoints,
          false,
          null,
          null,
          null,
          null,
          true,
          "round-robin",
          new UserEntity(1L, "user", "cli-host"),
          "password",
          true,
          "sync",
          true,
          true,
          true);
    }

    @Override
    protected void reconstructClient(final TEndPoint endPoint) {
      reconstructionCount++;
    }

    private int getReconstructionCount() {
      return reconstructionCount;
    }
  }
}
