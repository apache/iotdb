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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.procedure.impl.region.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.DatabaseLockQueue;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class PartitionManagerTest {

  @Test
  public void testCreateRegionGroupsProceduresAreSubmittedPerDatabaseBeforeWaiting()
      throws Exception {
    final IManager configManager = Mockito.mock(IManager.class);
    final LoadManager loadManager = Mockito.mock(LoadManager.class);
    final ProcedureManager procedureManager = Mockito.mock(ProcedureManager.class);
    final DatabaseLockQueue databaseLockQueue =
        new DatabaseLockQueue(Mockito.mock(ProcedureScheduler.class));
    Mockito.when(configManager.getLoadManager()).thenReturn(loadManager);
    Mockito.when(configManager.getProcedureManager()).thenReturn(procedureManager);
    Mockito.when(procedureManager.acquireDatabaseLifecycleLock(Mockito.anyString()))
        .thenAnswer(
            invocation ->
                databaseLockQueue.acquireLocks(
                    Collections.singleton((String) invocation.getArgument(0))));

    final CreateRegionGroupsPlan databaseAPlan = new CreateRegionGroupsPlan();
    final CreateRegionGroupsPlan databaseBPlan = new CreateRegionGroupsPlan();
    databaseAPlan.addRegionGroup(
        "root.a",
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1), Collections.emptyList()));
    databaseBPlan.addRegionGroup(
        "root.b",
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2), Collections.emptyList()));
    final CreateRegionGroupsProcedure databaseAProcedure =
        Mockito.mock(CreateRegionGroupsProcedure.class);
    final CreateRegionGroupsProcedure databaseBProcedure =
        Mockito.mock(CreateRegionGroupsProcedure.class);
    Mockito.when(
            loadManager.allocateRegionGroups(
                Collections.singletonMap("root.a", 1), TConsensusGroupType.DataRegion))
        .thenReturn(databaseAPlan);
    Mockito.when(
            loadManager.allocateRegionGroups(
                Collections.singletonMap("root.b", 2), TConsensusGroupType.DataRegion))
        .thenReturn(databaseBPlan);
    Mockito.when(
            procedureManager.submitCreateRegionGroups(
                TConsensusGroupType.DataRegion, databaseAPlan))
        .thenReturn(databaseAProcedure);
    Mockito.when(
            procedureManager.submitCreateRegionGroups(
                TConsensusGroupType.DataRegion, databaseBPlan))
        .thenReturn(databaseBProcedure);
    Mockito.when(procedureManager.waitCreateRegionGroups(databaseAProcedure))
        .thenReturn(new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode()));
    Mockito.when(procedureManager.waitCreateRegionGroups(databaseBProcedure))
        .thenReturn(RpcUtils.SUCCESS_STATUS);

    final PartitionManager partitionManager =
        new PartitionManager(configManager, Mockito.mock(PartitionInfo.class));
    try {
      final Map<String, Integer> allotmentMap = new LinkedHashMap<>();
      allotmentMap.put("root.b", 2);
      allotmentMap.put("root.a", 1);

      final TSStatus status =
          partitionManager.generateAndAllocateRegionGroups(
              allotmentMap, TConsensusGroupType.DataRegion);

      Assert.assertEquals(TSStatusCode.CREATE_REGION_ERROR.getStatusCode(), status.getCode());
      final InOrder inOrder = Mockito.inOrder(loadManager, procedureManager);
      inOrder.verify(procedureManager).acquireDatabaseLifecycleLock("root.a");
      inOrder
          .verify(loadManager)
          .allocateRegionGroups(
              Collections.singletonMap("root.a", 1), TConsensusGroupType.DataRegion);
      inOrder
          .verify(procedureManager)
          .submitCreateRegionGroups(TConsensusGroupType.DataRegion, databaseAPlan);
      inOrder.verify(procedureManager).acquireDatabaseLifecycleLock("root.b");
      inOrder
          .verify(loadManager)
          .allocateRegionGroups(
              Collections.singletonMap("root.b", 2), TConsensusGroupType.DataRegion);
      inOrder
          .verify(procedureManager)
          .submitCreateRegionGroups(TConsensusGroupType.DataRegion, databaseBPlan);
      inOrder.verify(procedureManager).waitCreateRegionGroups(databaseAProcedure);
      inOrder.verify(procedureManager).waitCreateRegionGroups(databaseBProcedure);
      Mockito.verify(procedureManager, Mockito.never())
          .acquireDatabaseLifecycleLocks(Mockito.anySet());
    } finally {
      partitionManager.getRegionMaintainer().shutdownNow();
    }
  }
}
