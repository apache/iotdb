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

package org.apache.iotdb.db.mpp.plan.analyze.cache;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

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
    for (int i = 0; i < STORAGE_GROUP_NUMBER; i++) {
      String storageGroupName = STORAGE_GROUP_PREFIX + i;
      storageGroups.add(storageGroupName);
      if (!schemaPartitionTable.containsKey(storageGroupName)) {
        schemaPartitionTable.put(storageGroupName, new HashMap<>());
      }
      if (!dataPartitionTable.containsKey(storageGroupName)) {
        dataPartitionTable.put(storageGroupName, new HashMap<>());
      }
      for (int j = 0; j < DEVICE_PER_STORAGE_GROUP; j++) {
        String deviceName = storageGroupName + "." + DEVICE_PREFIX + j;
        TSeriesPartitionSlot seriesPartitionSlot =
            new TSeriesPartitionSlot(partitionExecutor.getSeriesPartitionSlot(deviceName));
        // schemaConsensusGroupId = (storageGroupNumber * DEVICE_PER_STORAGE_GROUP + deviceNumber) *
        // TIME_PARTITION_PER_STORAGE_GROUP
        TConsensusGroupId schemaConsensusGroupId =
            new TConsensusGroupId(
                TConsensusGroupType.SchemaRegion,
                (i * DEVICE_PER_STORAGE_GROUP + j) * TIME_PARTITION_PER_STORAGE_GROUP);
        schemaPartitionTable.get(storageGroupName).put(seriesPartitionSlot, schemaConsensusGroupId);
        // put into consensusGroupIdToRegionReplicaSet
        List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
        TRegionReplicaSet schemaRegionReplicaSet =
            new TRegionReplicaSet(schemaConsensusGroupId, dataNodeLocations);
        consensusGroupIdToRegionReplicaSet.put(schemaConsensusGroupId, schemaRegionReplicaSet);
        // put into DataPartitionTable
        dataPartitionTable.get(storageGroupName).put(seriesPartitionSlot, new HashMap<>());
        for (int k = 0; k < TIME_PARTITION_PER_STORAGE_GROUP; k++) {
          TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(k);
          // dataConsensusGroupId = (storageGroupNumber * DEVICE_PER_STORAGE_GROUP + deviceNumber) *
          // TIME_PARTITION_PER_STORAGE_GROUP + timeSlotNumber + 1
          TConsensusGroupId dataConsensusGroupId =
              new TConsensusGroupId(
                  TConsensusGroupType.DataRegion,
                  (i * DEVICE_PER_STORAGE_GROUP + j) * TIME_PARTITION_PER_STORAGE_GROUP + k + 1);
          dataPartitionTable
              .get(storageGroupName)
              .get(seriesPartitionSlot)
              .put(timePartitionSlot, Collections.singletonList(dataConsensusGroupId));
          TRegionReplicaSet dataRegionReplicaSet =
              new TRegionReplicaSet(schemaConsensusGroupId, dataNodeLocations);
          // put into consensusGroupIdToRegionReplicaSet
          consensusGroupIdToRegionReplicaSet.put(dataConsensusGroupId, dataRegionReplicaSet);
        }
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    partitionCache = new PartitionCache();
    partitionCache.updateStorageCache(storageGroups);
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
    Map<String, List<String>> storageGroupToDeviceMap;
    Map<String, String> deviceToStorageGroupMap;
    // test devices in one storage group
    List<List<String>> existedDevicesInOneStorageGroup =
        Arrays.asList(
            Arrays.asList("root.sg1.d1", "root.sg1.d2"),
            Arrays.asList("root.sg2.d1", "root.sg2.d2"));
    for (List<String> searchDevices : existedDevicesInOneStorageGroup) {
      storageGroupToDeviceMap = partitionCache.getStorageGroupToDevice(searchDevices, false, false);
      assertEquals(1, storageGroupToDeviceMap.size());
      for (List<String> devices : storageGroupToDeviceMap.values()) {
        assertEquals(2, devices.size());
      }
      deviceToStorageGroupMap = partitionCache.getDeviceToStorageGroup(searchDevices, false, false);
      assertEquals(2, deviceToStorageGroupMap.size());
    }
    // test devices in two storage group
    List<List<String>> existedDevicesInMultiStorageGroup =
        Arrays.asList(
            Arrays.asList("root.sg1.d1", "root.sg2.d2"),
            Arrays.asList("root.sg1.d1", "root.sg2.d2"));
    for (List<String> searchDevices : existedDevicesInMultiStorageGroup) {
      storageGroupToDeviceMap = partitionCache.getStorageGroupToDevice(searchDevices, false, false);
      assertEquals(2, storageGroupToDeviceMap.size());
      for (List<String> devices : storageGroupToDeviceMap.values()) {
        assertEquals(1, devices.size());
      }
      deviceToStorageGroupMap = partitionCache.getDeviceToStorageGroup(searchDevices, false, false);
      assertEquals(2, deviceToStorageGroupMap.size());
    }
    // test non-existed devices
    List<List<String>> nonExistedDevices =
        Arrays.asList(
            Arrays.asList("root.sg5.d1", "root.sg5.d2"),
            Arrays.asList("root.sg.d1", "root.sg.d2"),
            Arrays.asList("root.sg3.**", "root.sg4.**"));
    for (List<String> searchDevices : nonExistedDevices) {
      storageGroupToDeviceMap = partitionCache.getStorageGroupToDevice(searchDevices, false, false);
      assertEquals(0, storageGroupToDeviceMap.size());
      deviceToStorageGroupMap = partitionCache.getDeviceToStorageGroup(searchDevices, false, false);
      assertEquals(0, deviceToStorageGroupMap.size());
    }
    // invalid storage group cache
    partitionCache.invalidAllCache();
    List<String> oneDeviceList = Collections.singletonList("root.sg1.d1");
    storageGroupToDeviceMap = partitionCache.getStorageGroupToDevice(oneDeviceList, false, false);
    assertEquals(0, storageGroupToDeviceMap.size());
    deviceToStorageGroupMap = partitionCache.getDeviceToStorageGroup(oneDeviceList, false, false);
    assertEquals(0, deviceToStorageGroupMap.size());
  }
}
