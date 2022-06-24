/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionLocationsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.response.RegionLocationsResp;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class PartitionInfoTest {

  private static PartitionInfo partitionInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  public enum testFlag {
    DataPartition(20),
    SchemaPartition(30);

    private final int flag;

    testFlag(int flag) {
      this.flag = flag;
    }

    public int getFlag() {
      return flag;
    }
  }

  @BeforeClass
  public static void setup() {
    partitionInfo = new PartitionInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    partitionInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException {

    partitionInfo.generateNextRegionGroupId();

    // Set StorageGroup
    partitionInfo.setStorageGroup(new SetStorageGroupReq(new TStorageGroupSchema("root.test")));

    // Create a SchemaRegion
    CreateRegionsReq createRegionGroupsReq = new CreateRegionsReq();
    TRegionReplicaSet schemaRegionReplicaSet =
        generateTRegionReplicaSet(
            testFlag.SchemaPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.SchemaPartition.getFlag(), TConsensusGroupType.SchemaRegion));
    createRegionGroupsReq.addRegionGroup("root.test", schemaRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionGroupsReq);

    // Create a DataRegion
    createRegionGroupsReq = new CreateRegionsReq();
    TRegionReplicaSet dataRegionReplicaSet =
        generateTRegionReplicaSet(
            testFlag.DataPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.DataPartition.getFlag(), TConsensusGroupType.DataRegion));
    createRegionGroupsReq.addRegionGroup("root.test", dataRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionGroupsReq);

    // Create a SchemaPartition
    CreateSchemaPartitionReq createSchemaPartitionReq =
        generateCreateSchemaPartitionReq(
            testFlag.SchemaPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.SchemaPartition.getFlag(), TConsensusGroupType.SchemaRegion));
    partitionInfo.createSchemaPartition(createSchemaPartitionReq);

    // Create a DataPartition
    CreateDataPartitionReq createDataPartitionReq =
        generateCreateDataPartitionReq(
            testFlag.DataPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.DataPartition.getFlag(), TConsensusGroupType.DataRegion));
    partitionInfo.createDataPartition(createDataPartitionReq);

    partitionInfo.getDeletedRegionSet().add(dataRegionReplicaSet);
    partitionInfo.getDeletedRegionSet().add(schemaRegionReplicaSet);

    partitionInfo.processTakeSnapshot(snapshotDir);

    PartitionInfo partitionInfo1 = new PartitionInfo();
    partitionInfo1.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(partitionInfo, partitionInfo1);
  }

  @Test
  public void testShowRegion() {
    partitionInfo.generateNextRegionGroupId();

    // Set StorageGroup
    partitionInfo.setStorageGroup(new SetStorageGroupReq(new TStorageGroupSchema("root.test")));

    // Create a SchemaRegion
    CreateRegionsReq createRegionsReq = new CreateRegionsReq();
    TRegionReplicaSet schemaRegionReplicaSet =
        generateTRegionReplicaSet(
            testFlag.SchemaPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.SchemaPartition.getFlag(), TConsensusGroupType.SchemaRegion));
    createRegionsReq.addRegionGroup("root.test", schemaRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionsReq);

    // Create a DataRegion
    createRegionsReq = new CreateRegionsReq();
    TRegionReplicaSet dataRegionReplicaSet =
        generateTRegionReplicaSet(
            testFlag.DataPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.DataPartition.getFlag(), TConsensusGroupType.DataRegion));
    createRegionsReq.addRegionGroup("root.test", dataRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionsReq);

    GetRegionLocationsReq regionReq = new GetRegionLocationsReq();
    regionReq.setRegionType(null);
    RegionLocationsResp regionLocations1 =
        (RegionLocationsResp) partitionInfo.getRegionLocations(regionReq);
    Assert.assertEquals(regionLocations1.getRegionInfosList().size(), 10);
    regionLocations1
        .getRegionInfosList()
        .forEach(
            (regionLocation) -> {
              Assert.assertEquals(regionLocation.getRpcAddresss(), "127.0.0.1");
            });

    regionReq.setRegionType(TConsensusGroupType.SchemaRegion);
    RegionLocationsResp regionLocations2 =
        (RegionLocationsResp) partitionInfo.getRegionLocations(regionReq);
    Assert.assertEquals(regionLocations2.getRegionInfosList().size(), 5);
    regionLocations2
        .getRegionInfosList()
        .forEach(
            (regionLocation) -> {
              Assert.assertEquals(
                  regionLocation.getConsensusGroupId().getType(), TConsensusGroupType.SchemaRegion);
            });

    regionReq.setRegionType(TConsensusGroupType.DataRegion);
    RegionLocationsResp regionLocations3 =
        (RegionLocationsResp) partitionInfo.getRegionLocations(regionReq);
    Assert.assertEquals(regionLocations3.getRegionInfosList().size(), 5);
    regionLocations3
        .getRegionInfosList()
        .forEach(
            (regionLocation) -> {
              Assert.assertEquals(
                  regionLocation.getConsensusGroupId().getType(), TConsensusGroupType.DataRegion);
            });
  }

  private TRegionReplicaSet generateTRegionReplicaSet(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
    tRegionReplicaSet.setRegionId(tConsensusGroupId);
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    int locationNum = 5;
    for (int i = startFlag; i < locationNum + startFlag; i++) {
      TDataNodeLocation tDataNodeLocation = new TDataNodeLocation();
      tDataNodeLocation.setDataNodeId(i);
      tDataNodeLocation.setExternalEndPoint(new TEndPoint("127.0.0.1", 6000 + i));
      tDataNodeLocation.setInternalEndPoint(new TEndPoint("127.0.0.1", 7000 + i));
      tDataNodeLocation.setDataBlockManagerEndPoint(new TEndPoint("127.0.0.1", 8000 + i));
      tDataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 9000 + i));
      tDataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 10000 + i));
      dataNodeLocations.add(tDataNodeLocation);
    }
    tRegionReplicaSet.setDataNodeLocations(dataNodeLocations);
    return tRegionReplicaSet;
  }

  private CreateSchemaPartitionReq generateCreateSchemaPartitionReq(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    CreateSchemaPartitionReq createSchemaPartitionReq = new CreateSchemaPartitionReq();
    // Map<StorageGroup, Map<TSeriesPartitionSlot, TSchemaRegionPlaceInfo>>
    Map<String, SchemaPartitionTable> assignedSchemaPartition = new HashMap<>();
    Map<TSeriesPartitionSlot, TConsensusGroupId> relationInfo = new HashMap<>();
    relationInfo.put(new TSeriesPartitionSlot(startFlag), tConsensusGroupId);
    assignedSchemaPartition.put("root.test", new SchemaPartitionTable(relationInfo));
    createSchemaPartitionReq.setAssignedSchemaPartition(assignedSchemaPartition);
    return createSchemaPartitionReq;
  }

  private CreateDataPartitionReq generateCreateDataPartitionReq(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    startFlag = startFlag / 10;
    CreateDataPartitionReq createSchemaPartitionReq = new CreateDataPartitionReq();
    // Map<StorageGroup, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionMessage>>>>
    Map<String, DataPartitionTable> dataPartitionMap = new HashMap<>();

    Map<TTimePartitionSlot, List<TConsensusGroupId>> relationInfo = new HashMap<>();

    for (int i = 0; i <= startFlag; i++) {
      relationInfo.put(
          new TTimePartitionSlot((System.currentTimeMillis() / 1000) + i),
          Collections.singletonList(tConsensusGroupId));
    }

    Map<TSeriesPartitionSlot, SeriesPartitionTable> slotInfo = new HashMap<>();

    for (int i = 0; i <= startFlag; i++) {
      slotInfo.put(new TSeriesPartitionSlot(startFlag + i), new SeriesPartitionTable(relationInfo));
    }

    dataPartitionMap.put("root.test", new DataPartitionTable(slotInfo));
    createSchemaPartitionReq.setAssignedDataPartition(dataPartitionMap);
    return createSchemaPartitionReq;
  }

  private TConsensusGroupId generateTConsensusGroupId(
      int startFlag, TConsensusGroupType consensusGroupType) {
    return new TConsensusGroupId(consensusGroupType, 111000 + startFlag);
  }
}
