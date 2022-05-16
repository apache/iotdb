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
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;

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

  enum testFlag {
    RegionReplica(10),
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

    CreateRegionsReq createRegionsReq = new CreateRegionsReq();

    TRegionReplicaSet tRegionReplicaSet =
        generateTRegionReplicaSet(
            testFlag.RegionReplica.getFlag(),
            generateTConsensusGroupId(testFlag.RegionReplica.getFlag()));
    createRegionsReq.addRegion("root.test", tRegionReplicaSet);
    partitionInfo.createRegions(createRegionsReq);

    CreateSchemaPartitionReq createSchemaPartitionReq =
        generateCreateSchemaPartitionReq(
            testFlag.SchemaPartition.getFlag(),
            generateTConsensusGroupId(testFlag.SchemaPartition.getFlag()));
    partitionInfo.createSchemaPartition(createSchemaPartitionReq);

    CreateDataPartitionReq createDataPartitionReq =
        generateCreateDataPartitionReq(
            testFlag.DataPartition.getFlag(),
            generateTConsensusGroupId(testFlag.DataPartition.getFlag()));
    partitionInfo.createDataPartition(createDataPartitionReq);
    int nextId = partitionInfo.getNextRegionGroupId();

    partitionInfo.processTakeSnapshot(snapshotDir);
    partitionInfo.clear();
    partitionInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(nextId, (int) partitionInfo.getNextRegionGroupId());

    List<TRegionReplicaSet> reloadTRegionReplicaSet =
        partitionInfo.getRegionReplicaSets(
            Collections.singletonList(generateTConsensusGroupId(testFlag.RegionReplica.getFlag())));
    Assert.assertEquals(1, reloadTRegionReplicaSet.size());
    Assert.assertEquals(tRegionReplicaSet, reloadTRegionReplicaSet.get(0));

    Assert.assertEquals(
        createDataPartitionReq.getAssignedDataPartition(),
        partitionInfo.getDataPartition().getDataPartitionMap());

    Assert.assertEquals(
        createSchemaPartitionReq.getAssignedSchemaPartition(),
        partitionInfo.getSchemaPartition().getSchemaPartitionMap());
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
      tDataNodeLocation.setConsensusEndPoint(new TEndPoint("127.0.0.1", 9000 + i));
      dataNodeLocations.add(tDataNodeLocation);
    }
    tRegionReplicaSet.setDataNodeLocations(dataNodeLocations);
    return tRegionReplicaSet;
  }

  private CreateSchemaPartitionReq generateCreateSchemaPartitionReq(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    CreateSchemaPartitionReq createSchemaPartitionReq = new CreateSchemaPartitionReq();
    // Map<StorageGroup, Map<TSeriesPartitionSlot, TSchemaRegionPlaceInfo>>
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition =
        new HashMap<>();
    Map<TSeriesPartitionSlot, TRegionReplicaSet> relationInfo = new HashMap<>();
    relationInfo.put(
        new TSeriesPartitionSlot(startFlag),
        generateTRegionReplicaSet(startFlag, tConsensusGroupId));
    assignedSchemaPartition.put("root.test.sg", relationInfo);
    createSchemaPartitionReq.setAssignedSchemaPartition(assignedSchemaPartition);
    return createSchemaPartitionReq;
  }

  private CreateDataPartitionReq generateCreateDataPartitionReq(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    CreateDataPartitionReq createSchemaPartitionReq = new CreateDataPartitionReq();
    // Map<StorageGroup, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionMessage>>>>
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();

    Map<TTimePartitionSlot, List<TRegionReplicaSet>> relationInfo = new HashMap<>();
    relationInfo.put(
        new TTimePartitionSlot(System.currentTimeMillis() / 1000),
        Collections.singletonList(generateTRegionReplicaSet(startFlag, tConsensusGroupId)));

    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> slotInfo =
        new HashMap<>();
    slotInfo.put(new TSeriesPartitionSlot(startFlag), relationInfo);

    dataPartitionMap.put("root.test.data.sg", slotInfo);
    createSchemaPartitionReq.setAssignedDataPartition(dataPartitionMap);
    return createSchemaPartitionReq;
  }

  private TConsensusGroupId generateTConsensusGroupId(int startFlag) {
    return new TConsensusGroupId(TConsensusGroupType.PartitionRegion, 111000 + startFlag);
  }
}
