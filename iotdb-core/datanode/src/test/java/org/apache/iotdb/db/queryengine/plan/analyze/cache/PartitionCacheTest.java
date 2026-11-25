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

package org.apache.iotdb.db.queryengine.plan.analyze.cache;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.partition.PartitionCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PartitionCacheTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SeriesPartitionExecutor partitionExecutor =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          config.getSeriesPartitionExecutorClass(), config.getSeriesPartitionSlotNum());

  private static final Set<String> databases = new HashSet<>();
  private static final Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>>
      schemaPartitionTable = new HashMap<>();
  private static final Map<
          String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
      dataPartitionTable = new HashMap<>();
  private static final Map<TConsensusGroupId, TRegionReplicaSet>
      consensusGroupIdToRegionReplicaSet = new HashMap<>();

  private static final String STORAGE_GROUP_PREFIX = "root.sg";
  private static final Integer STORAGE_GROUP_NUMBER = 5;
  private static final String DEVICE_PREFIX = "d";
  private static final Integer DEVICE_PER_STORAGE_GROUP = 10;
  private static final Integer TIME_PARTITION_PER_STORAGE_GROUP = 10;

  private PartitionCache partitionCache;
  private ConfigNodeClient routeClient;

  static {
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      // init each database
      String storageGroupName = getDatabaseName(storageGroupNumber);
      databases.add(storageGroupName);
      if (!schemaPartitionTable.containsKey(storageGroupName)) {
        schemaPartitionTable.put(storageGroupName, new HashMap<>());
      }
      if (!dataPartitionTable.containsKey(storageGroupName)) {
        dataPartitionTable.put(storageGroupName, new HashMap<>());
      }
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        // init each device
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        TSeriesPartitionSlot seriesPartitionSlot =
            new TSeriesPartitionSlot(partitionExecutor.getSeriesPartitionSlot(deviceID));
        // init schemaRegion of device
        TConsensusGroupId schemaConsensusGroupId =
            new TConsensusGroupId(
                TConsensusGroupType.SchemaRegion,
                (storageGroupNumber * DEVICE_PER_STORAGE_GROUP + deviceNumber)
                    * TIME_PARTITION_PER_STORAGE_GROUP);
        schemaPartitionTable.get(storageGroupName).put(seriesPartitionSlot, schemaConsensusGroupId);
        // init regionReplicaSet of specific schemaRegion
        List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
        TRegionReplicaSet schemaRegionReplicaSet =
            new TRegionReplicaSet(schemaConsensusGroupId, dataNodeLocations);
        consensusGroupIdToRegionReplicaSet.put(schemaConsensusGroupId, schemaRegionReplicaSet);
        // init dataRegion of device
        dataPartitionTable.get(storageGroupName).put(seriesPartitionSlot, new HashMap<>());
        for (int timePartitionSlotNumber = 0;
            timePartitionSlotNumber < TIME_PARTITION_PER_STORAGE_GROUP;
            timePartitionSlotNumber++) {
          // init each timePartition
          TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(timePartitionSlotNumber);
          // init regionReplicaSet of specific timePartition
          TConsensusGroupId dataConsensusGroupId =
              new TConsensusGroupId(
                  TConsensusGroupType.DataRegion,
                  (storageGroupNumber * DEVICE_PER_STORAGE_GROUP + deviceNumber)
                          * TIME_PARTITION_PER_STORAGE_GROUP
                      + timePartitionSlotNumber
                      + 1);
          dataPartitionTable
              .get(storageGroupName)
              .get(seriesPartitionSlot)
              .put(timePartitionSlot, Collections.singletonList(dataConsensusGroupId));
          TRegionReplicaSet dataRegionReplicaSet =
              new TRegionReplicaSet(schemaConsensusGroupId, dataNodeLocations);
          consensusGroupIdToRegionReplicaSet.put(dataConsensusGroupId, dataRegionReplicaSet);
        }
      }
    }
  }

  private static String getDatabaseName(int storageGroupNumber) {
    return STORAGE_GROUP_PREFIX + storageGroupNumber;
  }

  private static String getDeviceName(String storageGroupName, int deviceNumber) {
    return storageGroupName + "." + DEVICE_PREFIX + deviceNumber;
  }

  @Before
  public void setUp() throws Exception {
    final IClientManager<ConfigRegionId, ConfigNodeClient> clientManager =
        mock(IClientManager.class);
    routeClient = mock(ConfigNodeClient.class);
    when(clientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)).thenReturn(routeClient);
    partitionCache = new PartitionCache(clientManager) {};
    partitionCache.updateDatabaseCache(databases);
    partitionCache.updateSchemaPartitionCache(schemaPartitionTable);
    partitionCache.updateDataPartitionCache(dataPartitionTable);
    partitionCache.updateGroupIdToReplicaSetMap(100, consensusGroupIdToRegionReplicaSet);
  }

  @After
  public void tearDown() throws Exception {
    partitionCache.invalidAllCache();
  }

  @Test
  public void testStorageGroupCache() {
    Map<String, List<IDeviceID>> storageGroupToDeviceMap;
    Map<IDeviceID, String> deviceToStorageGroupMap;
    // test devices in one database
    List<List<IDeviceID>> existedDevicesInOneStorageGroup =
        Arrays.asList(
            Arrays.asList(
                Factory.DEFAULT_FACTORY.create("root.sg1.d1"),
                Factory.DEFAULT_FACTORY.create("root.sg1.d2")),
            Arrays.asList(
                Factory.DEFAULT_FACTORY.create("root.sg2.d1"),
                Factory.DEFAULT_FACTORY.create("root.sg2.d2")));
    for (List<IDeviceID> searchDevices : existedDevicesInOneStorageGroup) {
      storageGroupToDeviceMap =
          partitionCache.getDatabaseToDevice(
              searchDevices, false, false, AuthorityChecker.SUPER_USER);
      assertEquals(1, storageGroupToDeviceMap.size());
      for (List<IDeviceID> devices : storageGroupToDeviceMap.values()) {
        assertEquals(2, devices.size());
      }
      deviceToStorageGroupMap =
          partitionCache.getDeviceToDatabase(
              searchDevices, false, false, AuthorityChecker.SUPER_USER);
      assertEquals(2, deviceToStorageGroupMap.size());
    }
    // test devices in two database
    List<List<IDeviceID>> existedDevicesInMultiStorageGroup =
        Arrays.asList(
            Arrays.asList(
                Factory.DEFAULT_FACTORY.create("root.sg1.d1"),
                Factory.DEFAULT_FACTORY.create("root.sg2.d2")),
            Arrays.asList(
                Factory.DEFAULT_FACTORY.create("root.sg1.d1"),
                Factory.DEFAULT_FACTORY.create("root.sg2.d2")));
    for (List<IDeviceID> searchDevices : existedDevicesInMultiStorageGroup) {
      storageGroupToDeviceMap =
          partitionCache.getDatabaseToDevice(
              searchDevices, false, false, AuthorityChecker.SUPER_USER);
      assertEquals(2, storageGroupToDeviceMap.size());
      for (List<IDeviceID> devices : storageGroupToDeviceMap.values()) {
        assertEquals(1, devices.size());
      }
      deviceToStorageGroupMap =
          partitionCache.getDeviceToDatabase(
              searchDevices, false, false, AuthorityChecker.SUPER_USER);
      assertEquals(2, deviceToStorageGroupMap.size());
    }
    // test missed devices in storageGroupCache
    List<List<IDeviceID>> nonExistedDevices =
        Arrays.asList(
            Arrays.asList(
                Factory.DEFAULT_FACTORY.create("root.sg5.d1"),
                Factory.DEFAULT_FACTORY.create("root.sg5.d2")),
            Arrays.asList(
                Factory.DEFAULT_FACTORY.create("root.sg.d1"),
                Factory.DEFAULT_FACTORY.create("root.sg.d2")),
            Arrays.asList(
                Factory.DEFAULT_FACTORY.create("root.sg3.**"),
                Factory.DEFAULT_FACTORY.create("root.sg4.**")));
    for (List<IDeviceID> searchDevices : nonExistedDevices) {
      storageGroupToDeviceMap =
          partitionCache.getDatabaseToDevice(
              searchDevices, false, false, AuthorityChecker.SUPER_USER);
      assertEquals(0, storageGroupToDeviceMap.size());
      deviceToStorageGroupMap =
          partitionCache.getDeviceToDatabase(
              searchDevices, false, false, AuthorityChecker.SUPER_USER);
      assertEquals(0, deviceToStorageGroupMap.size());
    }
    // test invalid all cache
    partitionCache.invalidAllCache();
    List<IDeviceID> oneDeviceList =
        Collections.singletonList(Factory.DEFAULT_FACTORY.create("root.sg1.d1"));
    storageGroupToDeviceMap =
        partitionCache.getDatabaseToDevice(
            oneDeviceList, false, false, AuthorityChecker.SUPER_USER);
    assertEquals(0, storageGroupToDeviceMap.size());
    deviceToStorageGroupMap =
        partitionCache.getDeviceToDatabase(
            oneDeviceList, false, false, AuthorityChecker.SUPER_USER);
    assertEquals(0, deviceToStorageGroupMap.size());
  }

  @Test
  public void testNeedLastCacheDefaultsToTrueWhenUnset() throws Exception {
    when(routeClient.getMatchedDatabaseSchemas(any()))
        .thenReturn(
            new TDatabaseSchemaResp()
                .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                .setDatabaseSchemaMap(Collections.emptyMap()));
    final Map<String, TDatabaseSchema> databaseSchemaMap = new HashMap<>();

    final TDatabaseSchema unsetSchema = new TDatabaseSchema();
    databaseSchemaMap.put("root.unset", unsetSchema);

    final TDatabaseSchema enabledSchema = new TDatabaseSchema();
    enabledSchema.setNeedLastCache(true);
    databaseSchemaMap.put("root.enabled", enabledSchema);

    final TDatabaseSchema disabledSchema = new TDatabaseSchema();
    disabledSchema.setNeedLastCache(false);
    databaseSchemaMap.put("root.disabled", disabledSchema);

    partitionCache.updateDatabaseCache(databaseSchemaMap);

    assertTrue(partitionCache.isNeedLastCache("root.unset"));
    assertTrue(partitionCache.isNeedLastCache("root.enabled"));
    assertFalse(partitionCache.isNeedLastCache("root.disabled"));
    assertTrue(partitionCache.isNeedLastCache("root.missing"));
  }

  @Test
  public void testRegionReplicaSetCache() {
    // test update regionReplicaSetCache with small timestamp
    assertFalse(partitionCache.updateGroupIdToReplicaSetMap(0, consensusGroupIdToRegionReplicaSet));

    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        TConsensusGroupId schemaConsensusGroupId =
            new TConsensusGroupId(
                TConsensusGroupType.SchemaRegion,
                (storageGroupNumber * DEVICE_PER_STORAGE_GROUP + deviceNumber)
                    * TIME_PARTITION_PER_STORAGE_GROUP);
        checkRegionReplicaSet(schemaConsensusGroupId);
        for (int timePartitionSlotNumber = 0;
            timePartitionSlotNumber < TIME_PARTITION_PER_STORAGE_GROUP;
            timePartitionSlotNumber++) {
          TConsensusGroupId dataConsensusGroupId =
              new TConsensusGroupId(
                  TConsensusGroupType.DataRegion,
                  (storageGroupNumber * DEVICE_PER_STORAGE_GROUP + deviceNumber)
                          * TIME_PARTITION_PER_STORAGE_GROUP
                      + timePartitionSlotNumber
                      + 1);
          checkRegionReplicaSet(dataConsensusGroupId);
        }
      }
    }
  }

  @Test
  public void testDelayedRouteMapCannotReplaceNewerSnapshot() {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 9000);
    final TRegionReplicaSet staleReplicaSet =
        new TRegionReplicaSet(regionId, Collections.emptyList());
    final TDataNodeLocation currentLocation = new TDataNodeLocation().setDataNodeId(99);
    final TRegionReplicaSet currentReplicaSet =
        new TRegionReplicaSet(regionId, Collections.singletonList(currentLocation));

    assertTrue(
        partitionCache.updateGroupIdToReplicaSetMap(
            200, Collections.singletonMap(regionId, currentReplicaSet)));
    partitionCache.invalidReplicaSetCache(regionId);
    assertFalse(
        partitionCache.updateGroupIdToReplicaSetMap(
            150, Collections.singletonMap(regionId, staleReplicaSet)));
    assertTrue(
        partitionCache.updateGroupIdToReplicaSetMap(
            200, Collections.singletonMap(regionId, currentReplicaSet)));
    assertEquals(
        currentReplicaSet,
        partitionCache.getRegionReplicaSet(Collections.singletonList(regionId)).get(0));
  }

  @Test
  public void testNewPushWinsWhileOlderPullIsInFlight() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 9000);
    final TRegionReplicaSet stale = new TRegionReplicaSet(regionId, List.of());
    final TRegionReplicaSet current =
        new TRegionReplicaSet(regionId, List.of(new TDataNodeLocation().setDataNodeId(99)));
    final CountDownLatch fetching = new CountDownLatch(1);
    final CountDownLatch completePull = new CountDownLatch(1);
    when(routeClient.getLatestRegionRouteMap())
        .thenAnswer(
            invocation -> {
              fetching.countDown();
              assertTrue(completePull.await(10, TimeUnit.SECONDS));
              return new TRegionRouteMapResp(
                      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                  .setRegionRouteMap(Map.of(regionId, stale))
                  .setTimestamp(150);
            });
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<List<TRegionReplicaSet>> pull =
          executor.submit(() -> partitionCache.getRegionReplicaSet(List.of(regionId)));
      assertTrue(fetching.await(10, TimeUnit.SECONDS));
      final Future<Boolean> push =
          executor.submit(
              () -> partitionCache.updateGroupIdToReplicaSetMap(200, Map.of(regionId, current)));
      assertTrue(push.get(5, TimeUnit.SECONDS));
      completePull.countDown();
      assertEquals(List.of(current), pull.get(5, TimeUnit.SECONDS));
    } finally {
      completePull.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testInvalidationDuringPullCannotRestoreStaleRoute() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 9001);
    final TRegionReplicaSet stale = new TRegionReplicaSet(regionId, List.of());
    final TRegionReplicaSet current =
        new TRegionReplicaSet(regionId, List.of(new TDataNodeLocation().setDataNodeId(100)));
    assertTrue(partitionCache.updateGroupIdToReplicaSetMap(150, Map.of(regionId, stale)));
    partitionCache.invalidReplicaSetCache(regionId);

    final CountDownLatch fetching = new CountDownLatch(1);
    final CountDownLatch completePull = new CountDownLatch(1);
    final AtomicInteger fetchCount = new AtomicInteger();
    when(routeClient.getLatestRegionRouteMap())
        .thenAnswer(
            invocation -> {
              if (fetchCount.incrementAndGet() == 1) {
                fetching.countDown();
                assertTrue(completePull.await(10, TimeUnit.SECONDS));
                return new TRegionRouteMapResp(
                        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                    .setRegionRouteMap(Map.of(regionId, stale))
                    .setTimestamp(150);
              }
              return new TRegionRouteMapResp(
                      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                  .setRegionRouteMap(Map.of(regionId, current))
                  .setTimestamp(200);
            });

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<List<TRegionReplicaSet>> pull =
          executor.submit(() -> partitionCache.getRegionReplicaSet(List.of(regionId)));
      assertTrue(fetching.await(10, TimeUnit.SECONDS));
      partitionCache.invalidReplicaSetCache(regionId);
      completePull.countDown();

      assertEquals(List.of(current), pull.get(5, TimeUnit.SECONDS));
      assertEquals(2, fetchCount.get());
    } finally {
      completePull.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testFailedRouteResponseCannotAdvanceCacheVersion() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 9000);
    final TRegionReplicaSet replicaSet = new TRegionReplicaSet(regionId, List.of());
    when(routeClient.getLatestRegionRouteMap())
        .thenReturn(
            new TRegionRouteMapResp(
                    new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()))
                .setTimestamp(300)
                .setRegionRouteMap(Map.of(regionId, replicaSet)));

    assertThrows(
        StatementAnalyzeException.class,
        () -> partitionCache.getRegionReplicaSet(List.of(regionId)));
    verify(routeClient, times(3)).getLatestRegionRouteMap();
    assertTrue(partitionCache.updateGroupIdToReplicaSetMap(200, Map.of(regionId, replicaSet)));
    assertEquals(List.of(replicaSet), partitionCache.getRegionReplicaSet(List.of(regionId)));
  }

  @Test
  public void testRouteMapFetchRetriesAfterLeaderWarmingUp() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 9001);
    final TRegionReplicaSet replicaSet = new TRegionReplicaSet(regionId, List.of());
    when(routeClient.getLatestRegionRouteMap())
        .thenReturn(
            new TRegionRouteMapResp(
                    new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()))
                .setTimestamp(300)
                .setRegionRouteMap(Map.of(regionId, replicaSet)),
            new TRegionRouteMapResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                .setTimestamp(301)
                .setRegionRouteMap(Map.of(regionId, replicaSet)));

    assertEquals(List.of(replicaSet), partitionCache.getRegionReplicaSet(List.of(regionId)));
    verify(routeClient, times(2)).getLatestRegionRouteMap();
  }

  /**
   * Dropping the cached route of one region keeps the routes of the others, so re-resolving one
   * region does not send every other lookup to the ConfigNode.
   */
  @Test
  public void testInvalidatingOneRegionReplicaSetCacheKeepsTheOthers() {
    final List<TConsensusGroupId> regions =
        new ArrayList<>(consensusGroupIdToRegionReplicaSet.keySet());
    partitionCache.invalidReplicaSetCache(regions.get(0));
    checkRegionReplicaSet(regions.get(1));
  }

  private void checkRegionReplicaSet(TConsensusGroupId consensusGroupId) {
    try {
      List<TRegionReplicaSet> regionReplicaSets =
          partitionCache.getRegionReplicaSet(Collections.singletonList(consensusGroupId));
      assertEquals(1, regionReplicaSets.size());
      assertNotNull(regionReplicaSets.get(0));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSchemaRegionCache() {
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      String storageGroupName = getDatabaseName(storageGroupNumber);
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        TSeriesPartitionSlot seriesPartitionSlot =
            partitionExecutor.getSeriesPartitionSlot(deviceID);
        Map<String, List<IDeviceID>> searchMap = new HashMap<>();
        searchMap.put(storageGroupName, Collections.singletonList(deviceID));
        SchemaPartition schemaPartition = partitionCache.getSchemaPartition(searchMap);
        assertNotNull(schemaPartition);
        Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> result =
            schemaPartition.getSchemaPartitionMap();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertNotNull(result.get(storageGroupName));
        assertEquals(1, result.get(storageGroupName).size());
        assertNotNull(result.get(storageGroupName).get(seriesPartitionSlot));
      }
    }
    // test missed storageGroups in schemaPartitionCache
    List<String> missedStorageGroupNames = Arrays.asList("root.sg", "root.*");
    for (String missedStorageGroupName : missedStorageGroupNames) {
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(missedStorageGroupName, deviceNumber));
        Map<String, List<IDeviceID>> searchMap = new HashMap<>();
        searchMap.put(missedStorageGroupName, Collections.singletonList(deviceID));
        SchemaPartition schemaPartition = partitionCache.getSchemaPartition(searchMap);
        assertNull(schemaPartition);
      }
    }
    // test missed devices in schemaPartitionCache
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      String storageGroupName = getDatabaseName(storageGroupNumber);
      for (int deviceNumber = DEVICE_PER_STORAGE_GROUP;
          deviceNumber < 2 * DEVICE_PER_STORAGE_GROUP;
          deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        Map<String, List<IDeviceID>> searchMap = new HashMap<>();
        searchMap.put(storageGroupName, Collections.singletonList(deviceID));
        SchemaPartition schemaPartition = partitionCache.getSchemaPartition(searchMap);
        assertNull(schemaPartition);
      }
    }
    // test invalid SchemaPartitionCache
    partitionCache.invalidAllSchemaPartitionCache();
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      String storageGroupName = getDatabaseName(storageGroupNumber);
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        Map<String, List<IDeviceID>> searchMap = new HashMap<>();
        searchMap.put(storageGroupName, Collections.singletonList(deviceID));
        SchemaPartition schemaPartition = partitionCache.getSchemaPartition(searchMap);
        assertNull(schemaPartition);
      }
    }
  }

  @Test
  public void testDataPartitionCache() {
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      String storageGroupName = getDatabaseName(storageGroupNumber);
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        IDeviceID deviceID =
            IDeviceID.Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        TSeriesPartitionSlot seriesPartitionSlot =
            partitionExecutor.getSeriesPartitionSlot(deviceID);
        // try to get DataPartition from partitionCache
        Map<String, List<DataPartitionQueryParam>> searchMap =
            getStorageGroupToQueryParamsMap(storageGroupName, deviceID, false);
        DataPartition dataPartition = partitionCache.getDataPartition(searchMap);
        // try to check DataPartition
        assertNotNull(dataPartition);
        assertNotNull(dataPartition.getDataPartitionMap());
        Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
            result = dataPartition.getDataPartitionMap();
        assertEquals(1, result.size());
        assertNotNull(result.get(storageGroupName));
        Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListMap =
            result.get(storageGroupName).get(seriesPartitionSlot);
        assertNotNull(timePartitionSlotListMap);
        for (int timePartitionSlotNumber = 0;
            timePartitionSlotNumber < TIME_PARTITION_PER_STORAGE_GROUP;
            timePartitionSlotNumber++) {
          TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(timePartitionSlotNumber);
          assertNotNull(timePartitionSlotListMap.get(timePartitionSlot));
        }
      }
    }

    // test missed storageGroups in dataPartitionCache
    List<String> missedStorageGroupNames = Arrays.asList("root.sg", "root.*");
    for (String missedStorageGroupName : missedStorageGroupNames) {
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(missedStorageGroupName, deviceNumber));
        Map<String, List<DataPartitionQueryParam>> searchMap =
            getStorageGroupToQueryParamsMap(missedStorageGroupName, deviceID, false);
        DataPartition dataPartition = partitionCache.getDataPartition(searchMap);
        assertNull(dataPartition);
      }
    }

    // test missed devices in dataPartitionCache
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      String storageGroupName = getDatabaseName(storageGroupNumber);
      for (int deviceNumber = DEVICE_PER_STORAGE_GROUP;
          deviceNumber < 2 * DEVICE_PER_STORAGE_GROUP;
          deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        Map<String, List<DataPartitionQueryParam>> searchMap =
            getStorageGroupToQueryParamsMap(storageGroupName, deviceID, false);
        DataPartition dataPartition = partitionCache.getDataPartition(searchMap);
        assertNull(dataPartition);
      }
    }

    // test missed timePartitionSlots in dataPartitionCache
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      String storageGroupName = getDatabaseName(storageGroupNumber);
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        Map<String, List<DataPartitionQueryParam>> searchMap =
            getStorageGroupToQueryParamsMap(storageGroupName, deviceID, true);
        DataPartition dataPartition = partitionCache.getDataPartition(searchMap);
        assertNull(dataPartition);
      }
    }

    // test invalid dataPartitionCache
    partitionCache.invalidAllDataPartitionCache();
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      String storageGroupName = getDatabaseName(storageGroupNumber);
      for (int deviceNumber = 0; deviceNumber < DEVICE_PER_STORAGE_GROUP; deviceNumber++) {
        IDeviceID deviceID =
            Factory.DEFAULT_FACTORY.create(getDeviceName(storageGroupName, deviceNumber));
        Map<String, List<DataPartitionQueryParam>> searchMap =
            getStorageGroupToQueryParamsMap(storageGroupName, deviceID, false);
        DataPartition dataPartition = partitionCache.getDataPartition(searchMap);
        assertNull(dataPartition);
      }
    }
  }

  /**
   * get StorageGroupToQueryParamsMap
   *
   * @param timePartitionSlotMissed whether the timePartitionSlot in result is missed in cache
   */
  private Map<String, List<DataPartitionQueryParam>> getStorageGroupToQueryParamsMap(
      String storageGroupName, IDeviceID deviceID, boolean timePartitionSlotMissed) {
    Map<String, List<DataPartitionQueryParam>> storageGroupToQueryParamsMap = new HashMap<>();
    List<TTimePartitionSlot> timePartitionSlotList = new ArrayList<>();

    int startTime = 0;
    if (timePartitionSlotMissed) {
      startTime = TIME_PARTITION_PER_STORAGE_GROUP;
    }
    for (int timePartitionSlotNumber = startTime;
        timePartitionSlotNumber < startTime + TIME_PARTITION_PER_STORAGE_GROUP;
        timePartitionSlotNumber++) {
      TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(timePartitionSlotNumber);
      timePartitionSlotList.add(timePartitionSlot);
    }
    DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
    dataPartitionQueryParam.setDeviceID(deviceID);
    dataPartitionQueryParam.setTimePartitionSlotList(timePartitionSlotList);
    storageGroupToQueryParamsMap.put(
        storageGroupName, Collections.singletonList(dataPartitionQueryParam));
    return storageGroupToQueryParamsMap;
  }
}
