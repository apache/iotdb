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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.partition.PartitionCache;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class PartitionCacheTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final SeriesPartitionExecutor partitionExecutor =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          config.getSeriesPartitionExecutorClass(), config.getSeriesPartitionSlotNum());

  private static final Set<String> storageGroups = new HashSet<>();
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

  static {
    for (int storageGroupNumber = 0;
        storageGroupNumber < STORAGE_GROUP_NUMBER;
        storageGroupNumber++) {
      // init each database
      String storageGroupName = getDatabaseName(storageGroupNumber);
      storageGroups.add(storageGroupName);
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
    partitionCache = new PartitionCache();
    partitionCache.updateDatabaseCache(storageGroups);
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

  private void checkRegionReplicaSet(TConsensusGroupId consensusGroupId) {
    try {
      assertNotNull(partitionCache.getRegionReplicaSet(consensusGroupId));
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
