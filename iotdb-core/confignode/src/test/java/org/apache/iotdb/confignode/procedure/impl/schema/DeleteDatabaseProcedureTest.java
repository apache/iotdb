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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainType;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteStorageGroupState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeleteDatabaseProcedureTest {

  @Test
  public void serializeDeserializeTest() {

    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    DeleteDatabaseProcedure p1 = new DeleteDatabaseProcedure(new TDatabaseSchema("root.sg"), false);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      DeleteDatabaseProcedure p2 =
          (DeleteDatabaseProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(p1, p2);

    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testDataRegionDeleteTasksArePreparedBeforeMetadataCleanup() {
    TDataNodeLocation dataNode0 = new TDataNodeLocation().setDataNodeId(0);
    TDataNodeLocation dataNode1 = new TDataNodeLocation().setDataNodeId(1);
    TConsensusGroupId dataRegionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 10);
    TConsensusGroupId schemaRegionId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 20);
    List<TRegionReplicaSet> replicaSets =
        Arrays.asList(
            new TRegionReplicaSet(dataRegionId, Arrays.asList(dataNode0, dataNode1)),
            new TRegionReplicaSet(schemaRegionId, Collections.singletonList(dataNode0)));

    OfferRegionMaintainTasksPlan offerPlan =
        DeleteDatabaseProcedure.buildDataRegionDeleteTaskOfferPlan(replicaSets);
    List<RegionMaintainTask> tasks = offerPlan.getRegionMaintainTaskList();

    assertEquals(2, tasks.size());
    assertEquals(RegionMaintainType.DELETE, tasks.get(0).getType());
    assertEquals(dataRegionId, tasks.get(0).getRegionId());
    assertEquals(dataNode0, tasks.get(0).getTargetDataNode());
    assertEquals(dataNode1, tasks.get(1).getTargetDataNode());
  }

  @Test
  public void testIdempotentDeleteStatusIsCompleted() {
    assertTrue(
        DeleteDatabaseProcedure.isRegionDeleteCompleted(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    assertTrue(
        DeleteDatabaseProcedure.isRegionDeleteCompleted(
            new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode())));
    assertFalse(
        DeleteDatabaseProcedure.isRegionDeleteCompleted(
            new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode())));
  }

  @Test
  public void testFailedSynchronousSchemaRegionDeleteIsQueued() {
    TDataNodeLocation dataNode = new TDataNodeLocation().setDataNodeId(0);
    TConsensusGroupId schemaRegionId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 20);
    RegionDeleteTask failedTask = new RegionDeleteTask(dataNode, schemaRegionId);
    Map<Integer, RegionDeleteTask> failedTasks = new HashMap<>();
    failedTasks.put(0, failedTask);
    OfferRegionMaintainTasksPlan offerPlan = new OfferRegionMaintainTasksPlan();

    DeleteDatabaseProcedure.appendFailedSchemaRegionDeleteTasks(offerPlan, failedTasks);

    assertEquals(Collections.singletonList(failedTask), offerPlan.getRegionMaintainTaskList());
  }

  @Test
  public void testRegionDeleteTaskOfferMustSucceedBeforeMetadataCleanup() {
    assertTrue(
        DeleteDatabaseProcedure.isRegionDeleteTaskOfferSuccessful(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    assertFalse(
        DeleteDatabaseProcedure.isRegionDeleteTaskOfferSuccessful(
            new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())));
  }

  @Test
  public void testFailedTaskOfferPreventsPartitionMetadataCleanup() throws Exception {
    TDataNodeLocation dataNode = new TDataNodeLocation().setDataNodeId(0);
    TConsensusGroupId dataRegionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 10);
    TRegionReplicaSet dataRegion =
        new TRegionReplicaSet(dataRegionId, Collections.singletonList(dataNode));
    ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(env.getAllReplicaSets("root.sg"))
        .thenReturn(Collections.singletonList(dataRegion));
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(configManager.getLoadManager()).thenReturn(loadManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));

    DeleteDatabaseProcedure procedure =
        new DeleteDatabaseProcedure(new TDatabaseSchema("root.sg"), false);
    procedure.executeFromState(env, DeleteStorageGroupState.DELETE_DATABASE_SCHEMA);

    ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager).write(planCaptor.capture());
    assertTrue(planCaptor.getValue() instanceof OfferRegionMaintainTasksPlan);
    assertEquals(
        1,
        ((OfferRegionMaintainTasksPlan) planCaptor.getValue()).getRegionMaintainTaskList().size());
    Mockito.verify(loadManager, Mockito.never()).clearDataPartitionPolicyTable(Mockito.anyString());
    Mockito.verify(env, Mockito.never())
        .deleteDatabaseConfig(Mockito.anyString(), Mockito.anyBoolean());
  }
}
