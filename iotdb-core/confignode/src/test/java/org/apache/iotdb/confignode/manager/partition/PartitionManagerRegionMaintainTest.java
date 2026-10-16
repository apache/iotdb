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

package org.apache.iotdb.confignode.manager.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionCreateTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class PartitionManagerRegionMaintainTest {

  @Test
  public void testPerRegionFifoAndPartialFailureRetry() {
    TConsensusGroupId region0 = regionId(TConsensusGroupType.DataRegion, 0);
    TConsensusGroupId region1 = regionId(TConsensusGroupType.DataRegion, 1);
    RegionMaintainTask region0Head = deleteTask(region0);
    RegionMaintainTask region0Next = createTask(region0);
    RegionMaintainTask region1Head = deleteTask(region1);
    Map<TConsensusGroupId, Queue<RegionMaintainTask>> tasksByRegion =
        PartitionManager.groupRegionMaintainTasks(
            Arrays.asList(region0Head, region0Next, region1Head));

    Map<RegionMaintainType, List<RegionMaintainTask>> firstRound =
        PartitionManager.getRegionMaintainTaskHeads(tasksByRegion);
    assertEquals(
        Arrays.asList(region0Head, region1Head), firstRound.get(RegionMaintainType.DELETE));
    assertFalse(firstRound.containsKey(RegionMaintainType.CREATE));

    Set<TConsensusGroupId> deferredRegions = new HashSet<>();
    PartitionManager.deferFailedRegionMaintainTasks(
        deferredRegions, firstRound, Collections.singleton(region1));
    PartitionManager.pollCompletedRegionMaintainTaskHeads(
        tasksByRegion, Collections.singleton(region1));
    assertSame(region0Head, tasksByRegion.get(region0).peek());
    assertFalse(tasksByRegion.containsKey(region1));
    assertFalse(
        PartitionManager.getRegionMaintainTaskHeads(tasksByRegion, deferredRegions)
            .containsKey(RegionMaintainType.DELETE));

    tasksByRegion =
        PartitionManager.groupRegionMaintainTasks(
            Arrays.asList(region0Head, region0Next, region1Head));
    firstRound = PartitionManager.getRegionMaintainTaskHeads(tasksByRegion);
    deferredRegions = new HashSet<>();
    PartitionManager.deferFailedRegionMaintainTasks(
        deferredRegions, firstRound, Collections.singleton(region0));
    PartitionManager.pollCompletedRegionMaintainTaskHeads(
        tasksByRegion, Collections.singleton(region0));
    assertSame(
        region0Next,
        PartitionManager.getRegionMaintainTaskHeads(tasksByRegion, deferredRegions)
            .get(RegionMaintainType.CREATE)
            .get(0));
    assertSame(region1Head, tasksByRegion.get(region1).peek());

    deferredRegions.clear();
    assertSame(
        region1Head,
        PartitionManager.getRegionMaintainTaskHeads(tasksByRegion)
            .get(RegionMaintainType.DELETE)
            .get(0));
  }

  @Test
  public void testCompletedStatusAndRequestIndexMapping() {
    assertCompleted(RegionMaintainType.CREATE, TSStatusCode.SUCCESS_STATUS, true);
    assertCompleted(RegionMaintainType.CREATE, TSStatusCode.REGION_ALREADY_EXISTS, true);
    assertCompleted(RegionMaintainType.CREATE, TSStatusCode.REGION_NOT_EXIST, false);
    assertCompleted(RegionMaintainType.CREATE, TSStatusCode.CREATE_REGION_ERROR, false);
    assertCompleted(RegionMaintainType.DELETE, TSStatusCode.SUCCESS_STATUS, true);
    assertCompleted(RegionMaintainType.DELETE, TSStatusCode.REGION_NOT_EXIST, true);
    assertCompleted(RegionMaintainType.DELETE, TSStatusCode.REGION_ALREADY_EXISTS, false);
    assertCompleted(RegionMaintainType.DELETE, TSStatusCode.DELETE_REGION_ERROR, false);

    TConsensusGroupId schemaRegion = regionId(TConsensusGroupType.SchemaRegion, 7);
    TConsensusGroupId dataRegion = regionId(TConsensusGroupType.DataRegion, 7);
    Map<Integer, TConsensusGroupId> regionsByRequestIndex = new HashMap<>();
    regionsByRequestIndex.put(0, schemaRegion);
    regionsByRequestIndex.put(1, dataRegion);
    Map<Integer, TSStatus> responses = new HashMap<>();
    responses.put(0, status(TSStatusCode.SUCCESS_STATUS));
    responses.put(1, status(TSStatusCode.CREATE_REGION_ERROR));

    Set<TConsensusGroupId> completed =
        PartitionManager.collectCompletedRegionMaintainTasks(
            RegionMaintainType.CREATE, responses, regionsByRequestIndex);
    assertEquals(Collections.singleton(schemaRegion), completed);
  }

  private static void assertCompleted(
      RegionMaintainType type, TSStatusCode statusCode, boolean expected) {
    assertEquals(
        expected, PartitionManager.isRegionMaintainTaskCompleted(type, status(statusCode)));
  }

  private static TSStatus status(TSStatusCode statusCode) {
    return new TSStatus(statusCode.getStatusCode());
  }

  private static RegionDeleteTask deleteTask(TConsensusGroupId regionId) {
    return new RegionDeleteTask(new TDataNodeLocation(), regionId);
  }

  private static RegionCreateTask createTask(TConsensusGroupId regionId) {
    TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(regionId, Collections.singletonList(new TDataNodeLocation()));
    return new RegionCreateTask(new TDataNodeLocation(), "root.test", replicaSet);
  }

  private static TConsensusGroupId regionId(TConsensusGroupType type, int id) {
    return new TConsensusGroupId(type, id);
  }
}
