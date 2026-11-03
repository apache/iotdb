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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class RegionReplicaSetLookupTest {
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> regionTypes() {
    return Arrays.asList(
        new Object[] {TConsensusGroupType.DataRegion},
        new Object[] {TConsensusGroupType.SchemaRegion});
  }

  @Parameterized.Parameter public TConsensusGroupType regionType;

  private final List<CountingReplicaSet> replicaSets = new ArrayList<>();
  private PartitionInfo partitionInfo;
  private PartitionManager partitionManager;
  private RegionMaintainHandler handler;
  private TConsensusGroupId target;

  @Before
  public void setUp() {
    partitionInfo = new PartitionInfo();
    partitionManager = mock(PartitionManager.class);
    ConfigManager configManager = mock(ConfigManager.class);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(partitionManager.getRegionDatabase(any()))
        .thenAnswer(invocation -> partitionInfo.getRegionDatabase(invocation.getArgument(0)));
    when(partitionManager.getReplicaSets(anyString(), anyList()))
        .thenAnswer(
            invocation ->
                partitionInfo.getReplicaSets(invocation.getArgument(0), invocation.getArgument(1)));
    when(partitionManager.getAllReplicaSets())
        .thenAnswer(invocation -> partitionInfo.getAllReplicaSets());
    handler = new RegionMaintainHandler(configManager);
    for (int database = 0; database < 3; database++) {
      String name = "root.lookup" + database;
      partitionInfo.createDatabase(
          new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(name)));
      CreateRegionGroupsPlan plan = new CreateRegionGroupsPlan();
      for (int index = 0; index < 16; index++) {
        for (TConsensusGroupType type :
            Arrays.asList(TConsensusGroupType.DataRegion, TConsensusGroupType.SchemaRegion)) {
          CountingReplicaSet replicaSet =
              new CountingReplicaSet(new TConsensusGroupId(type, database * 16 + index));
          replicaSets.add(replicaSet);
          plan.addRegionGroup(name, replicaSet);
        }
      }
      partitionInfo.createRegionGroups(plan);
    }
    target = new TConsensusGroupId(regionType, 20);
  }

  @Test
  public void testLookupCopiesOnlyRequestedRegion() {
    Optional<TRegionReplicaSet> result = handler.getRegionReplicaSet(target);
    assertTrue(result.isPresent());
    assertEquals(target, result.get().getRegionId());
    assertEquals(1, copyCount());
    for (CountingReplicaSet replicaSet : replicaSets) {
      assertEquals(replicaSet.getRegionId().equals(target) ? 1 : 0, replicaSet.copies);
    }
  }

  @Test
  public void testReturnedReplicaSetIsAnIndependentDeepCopy() {
    TRegionReplicaSet first = handler.getRegionReplicaSet(target).get();
    first.getDataNodeLocations().get(0).getInternalEndPoint().setIp("changed");
    first.getDataNodeLocations().remove(1);
    first.getRegionId().setId(-1);
    TRegionReplicaSet second = handler.getRegionReplicaSet(target).get();
    assertEquals(target, second.getRegionId());
    assertEquals(3, second.getDataNodeLocationsSize());
    assertEquals("127.0.0.1", second.getDataNodeLocations().get(0).getInternalEndPoint().getIp());
    assertNotSame(first.getDataNodeLocations().get(0), second.getDataNodeLocations().get(0));
    assertEquals(2, copyCount());
  }

  @Test
  public void testMissingRegionDoesNotCopyReplicaSets() {
    TConsensusGroupId missing = new TConsensusGroupId(regionType, 9999);
    assertFalse(handler.getRegionReplicaSet(missing).isPresent());
    assertTrue(handler.findRegionLocations(missing).isEmpty());
    assertEquals("UNKNOWN!", handler.getRegionReplicaSetString(missing));
    assertEquals(0, copyCount());
  }

  @Test
  public void testNullRegionRemainsAbsent() {
    assertFalse(handler.getRegionReplicaSet(null).isPresent());
    assertEquals(0, copyCount());
  }

  @Test
  public void testPreDeletedDatabaseRemainsVisible() {
    partitionInfo.preDeleteDatabase(
        new PreDeleteDatabasePlan("root.lookup1", PreDeleteDatabasePlan.PreDeleteType.EXECUTE));
    assertTrue(handler.getRegionReplicaSet(target).isPresent());
    assertEquals(1, copyCount());
  }

  @Test
  public void testDatabaseDeletedBetweenLookups() {
    when(partitionManager.getRegionDatabase(target))
        .thenAnswer(
            invocation -> {
              String database = partitionInfo.getRegionDatabase(target);
              partitionInfo.deleteDatabase(new DeleteDatabasePlan(database));
              return database;
            });
    assertFalse(handler.getRegionReplicaSet(target).isPresent());
    assertEquals(0, copyCount());
  }

  private int copyCount() {
    return replicaSets.stream().mapToInt(replicaSet -> replicaSet.copies).sum();
  }

  private static class CountingReplicaSet extends TRegionReplicaSet {
    private int copies;

    private CountingReplicaSet(TConsensusGroupId regionId) {
      setRegionId(regionId);
      List<TDataNodeLocation> locations = new ArrayList<>();
      for (int nodeId = 0; nodeId < 3; nodeId++) {
        locations.add(
            new TDataNodeLocation()
                .setDataNodeId(nodeId)
                .setInternalEndPoint(new TEndPoint("127.0.0.1", 10730 + nodeId)));
      }
      setDataNodeLocations(locations);
    }

    @Override
    public TRegionReplicaSet deepCopy() {
      copies++;
      return super.deepCopy();
    }
  }
}
