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
package org.apache.iotdb.confignode.physical;

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.CreateRegionsPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializeDeserializeUT {

  private final ByteBuffer buffer = ByteBuffer.allocate(10240);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void RegisterDataNodePlanTest() throws IOException {
    RegisterDataNodePlan plan0 =
        new RegisterDataNodePlan(new DataNodeLocation(1, new Endpoint("0.0.0.0", 6667)));
    plan0.serialize(buffer);
    RegisterDataNodePlan plan1 = (RegisterDataNodePlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void QueryDataNodeInfoPlanTest() throws IOException {
    QueryDataNodeInfoPlan plan0 = new QueryDataNodeInfoPlan(-1);
    plan0.serialize(buffer);
    QueryDataNodeInfoPlan plan1 = (QueryDataNodeInfoPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void SetStorageGroupPlanTest() throws IOException {
    SetStorageGroupPlan plan0 = new SetStorageGroupPlan(new StorageGroupSchema("sg"));
    plan0.serialize(buffer);
    SetStorageGroupPlan plan1 = (SetStorageGroupPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void DeleteStorageGroupPlanTest() {
    // TODO: Add serialize and deserialize test
  }

  @Test
  public void CreateRegionsPlanTest() throws IOException {
    CreateRegionsPlan plan0 = new CreateRegionsPlan();
    plan0.setStorageGroup("sg");
    RegionReplicaSet dataRegionSet = new RegionReplicaSet();
    dataRegionSet.setId(new DataRegionId(0));
    dataRegionSet.setDataNodeList(
        Collections.singletonList(new DataNodeLocation(0, new Endpoint("0.0.0.0", 6667))));
    plan0.addRegion(dataRegionSet);
    RegionReplicaSet schemaRegionSet = new RegionReplicaSet();
    schemaRegionSet.setId(new SchemaRegionId(1));
    schemaRegionSet.setDataNodeList(
        Collections.singletonList(new DataNodeLocation(0, new Endpoint("0.0.0.0", 6667))));
    plan0.addRegion(schemaRegionSet);

    plan0.serialize(buffer);
    CreateRegionsPlan plan1 = (CreateRegionsPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void CreateSchemaPartitionPlanTest() {
    // TODO: Add serialize and deserialize test
  }

  @Test
  public void GetOrCreateSchemaPartitionPlanTest() {
    // TODO: Add serialize and deserialize test
  }

  @Test
  public void CreateDataPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    SeriesPartitionSlot seriesPartitionSlot = new SeriesPartitionSlot(10);
    TimePartitionSlot timePartitionSlot = new TimePartitionSlot(100);
    RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
    regionReplicaSet.setId(new DataRegionId(0));
    regionReplicaSet.setDataNodeList(
        Collections.singletonList(new DataNodeLocation(0, new Endpoint("0.0.0.0", 6667))));

    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
        assignedDataPartition = new HashMap<>();
    assignedDataPartition.put(storageGroup, new HashMap<>());
    assignedDataPartition.get(storageGroup).put(seriesPartitionSlot, new HashMap<>());
    assignedDataPartition
        .get(storageGroup)
        .get(seriesPartitionSlot)
        .put(timePartitionSlot, new ArrayList<>());
    assignedDataPartition
        .get(storageGroup)
        .get(seriesPartitionSlot)
        .get(timePartitionSlot)
        .add(regionReplicaSet);

    CreateDataPartitionPlan plan0 = new CreateDataPartitionPlan();
    plan0.setAssignedDataPartition(assignedDataPartition);
    plan0.serialize(buffer);
    CreateDataPartitionPlan plan1 = (CreateDataPartitionPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }

  @Test
  public void GetOrCreateDataPartitionPlanTest() throws IOException {
    String storageGroup = "root.sg0";
    SeriesPartitionSlot seriesPartitionSlot = new SeriesPartitionSlot(10);
    TimePartitionSlot timePartitionSlot = new TimePartitionSlot(100);

    Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    partitionSlotsMap.put(storageGroup, new HashMap<>());
    partitionSlotsMap.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
    partitionSlotsMap.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);

    GetOrCreateDataPartitionPlan plan0 =
        new GetOrCreateDataPartitionPlan(PhysicalPlanType.GetDataPartition);
    plan0.setPartitionSlotsMap(partitionSlotsMap);
    plan0.serialize(buffer);
    GetOrCreateDataPartitionPlan plan1 =
        (GetOrCreateDataPartitionPlan) PhysicalPlan.Factory.create(buffer);
    Assert.assertEquals(plan0, plan1);
  }
}
