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

package org.apache.iotdb.confignode.manager.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.ConsistencyProgressInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsistencyCheckSchedulerTest {

  @Test
  public void schedulerShouldSkipRegionsWithRunningRepairProcedure() {
    ConfigManager configManager = mock(ConfigManager.class);
    ProcedureManager procedureManager = mock(ProcedureManager.class);
    PartitionManager partitionManager = mock(PartitionManager.class);

    TConsensusGroupId region1 = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    TConsensusGroupId region2 = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    Map<TConsensusGroupId, TRegionReplicaSet> replicaSets = new LinkedHashMap<>();
    replicaSets.put(region1, new TRegionReplicaSet());
    replicaSets.put(region2, new TRegionReplicaSet());

    ConsistencyProgressManager progressManager =
        new ConsistencyProgressManager(new ConsistencyProgressInfo());
    List<TConsensusGroupId> checkedRegions = new ArrayList<>();

    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(configManager.getConsistencyProgressManager()).thenReturn(progressManager);
    when(partitionManager.getAllReplicaSetsMap(TConsensusGroupType.DataRegion))
        .thenReturn(replicaSets);
    when(procedureManager.hasRunningRepairProcedure(region1)).thenReturn(false);
    when(procedureManager.hasRunningRepairProcedure(region2)).thenReturn(true);

    ConsistencyCheckScheduler scheduler =
        new ConsistencyCheckScheduler(
            configManager,
            procedureManager,
            0L,
            1L,
            (manager, consensusGroupId, progressTable) -> {
              checkedRegions.add(consensusGroupId);
              progressTable.markVerified(
                  100L, 1000L, 2000L, 3000L, 3000L, RepairProgressTable.SnapshotState.READY);
            });

    scheduler.runOneRound();

    Assert.assertEquals(1, checkedRegions.size());
    Assert.assertEquals(region1, checkedRegions.get(0));
    Assert.assertNotNull(progressManager.loadRepairProgressTable(region1).getPartition(100L));
    Assert.assertNull(progressManager.loadRepairProgressTable(region2).getPartition(100L));
  }
}
