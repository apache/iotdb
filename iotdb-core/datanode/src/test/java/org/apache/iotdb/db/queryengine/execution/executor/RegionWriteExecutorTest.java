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

package org.apache.iotdb.db.queryengine.execution.executor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.trigger.executor.TriggerFireResult;
import org.apache.iotdb.db.trigger.executor.TriggerFireVisitor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RegionWriteExecutorTest {

  @Test
  public void testInsertRowNode() {

    IConsensus dataRegionConsensus = Mockito.mock(IConsensus.class);
    IConsensus schemaRegionConsensus = Mockito.mock(IConsensus.class);
    DataNodeRegionManager regionManager = Mockito.mock(DataNodeRegionManager.class);
    SchemaEngine schemaEngine = Mockito.mock(SchemaEngine.class);
    ClusterTemplateManager clusterTemplateManager = Mockito.mock(ClusterTemplateManager.class);
    TriggerFireVisitor triggerFireVisitor = Mockito.mock(TriggerFireVisitor.class);

    RegionWriteExecutor executor =
        new RegionWriteExecutor(
            dataRegionConsensus,
            schemaRegionConsensus,
            regionManager,
            schemaEngine,
            clusterTemplateManager,
            triggerFireVisitor);

    ConsensusGroupId dataRegionGroupId = new DataRegionId(1);
    InsertRowNode planNode = null;
    try {
      planNode = getInsertRowNode();
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    Mockito.when(regionManager.getRegionLock(dataRegionGroupId))
        .thenReturn(new ReentrantReadWriteLock());

    ConsensusWriteResponse writeResponse = Mockito.mock(ConsensusWriteResponse.class);
    Mockito.when(dataRegionConsensus.write(dataRegionGroupId, planNode)).thenReturn(writeResponse);

    Mockito.when(writeResponse.getStatus()).thenReturn(null);

    RegionExecutionResult res = executor.execute(dataRegionGroupId, planNode);
    assertFalse(res.isAccepted());

    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.BEFORE_INSERT))
        .thenReturn(TriggerFireResult.TERMINATION);
    res = executor.execute(dataRegionGroupId, planNode);
    assertFalse(res.isAccepted());

    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.BEFORE_INSERT))
        .thenReturn(TriggerFireResult.SUCCESS);
    Mockito.when(writeResponse.isSuccessful()).thenReturn(true);
    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.AFTER_INSERT))
        .thenReturn(TriggerFireResult.TERMINATION);
    res = executor.execute(dataRegionGroupId, planNode);
    assertFalse(res.isAccepted());

    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.AFTER_INSERT))
        .thenReturn(TriggerFireResult.SUCCESS);
    Mockito.when(writeResponse.getStatus())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    res = executor.execute(dataRegionGroupId, planNode);
    assertTrue(res.isAccepted());
  }

  private InsertRowNode getInsertRowNode() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
        };

    Object[] columns = new Object[5];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;

    return new InsertRowNode(
        new PlanNodeId("1"),
        new PartialPath("root.isp.d1"),
        false,
        new String[] {"s1", "s2", "s3", "s4", "s5"},
        dataTypes,
        time,
        columns,
        false);
  }
}
