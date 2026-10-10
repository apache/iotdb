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
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement.AlterType;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.trigger.executor.TriggerFireResult;
import org.apache.iotdb.db.trigger.executor.TriggerFireVisitor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RegionWriteExecutorTest {

  @Test
  public void testAlterTimeSeriesRegionAvailability() throws Exception {
    // Alter validation fetches the measurement before consensus; a missing region must be
    // retryable.
    checkSchemaWriteWithRegionAvailability(
        new AlterTimeSeriesNode(
            new PlanNodeId("alter"),
            new MeasurementPath("root.sg.d.s"),
            AlterType.ADD_TAGS,
            Collections.singletonMap("tag", "value"),
            null,
            null,
            null,
            false));
  }

  @Test
  public void testCreateLogicalViewRegionAvailability() throws Exception {
    // Ratis checks target existence locally. SimpleConsensus must retain its direct write path.
    checkSchemaWriteWithRegionAvailability(
        new CreateLogicalViewNode(
            new PlanNodeId("view"),
            Collections.singletonMap(
                new MeasurementPath("root.sg.d.v"), new TimeSeriesViewOperand("root.sg.d.s"))));
  }

  @Test
  public void testCreateOrUpdateTableDeviceRegionAvailability() throws Exception {
    // Table-device quota validation has the same missing-region window as tree auto-creation.
    checkSchemaWriteWithRegionAvailability(
        new CreateOrUpdateTableDeviceNode(
            new PlanNodeId("table"),
            "db",
            "table1",
            Collections.singletonList(new Object[] {"device1"}),
            Collections.emptyList(),
            Collections.singletonList(new Object[0])));
  }

  private void checkSchemaWriteWithRegionAvailability(final WritePlanNode node) throws Exception {
    final String originalProtocol =
        IoTDBDescriptor.getInstance().getConfig().getSchemaRegionConsensusProtocolClass();
    try {
      for (String protocol :
          new String[] {ConsensusFactory.RATIS_CONSENSUS, ConsensusFactory.SIMPLE_CONSENSUS}) {
        IoTDBDescriptor.getInstance().getConfig().setSchemaRegionConsensusProtocolClass(protocol);
        for (boolean available : new boolean[] {false, true}) {
          for (boolean fromPipe : new boolean[] {false, true}) {
            final IConsensus consensus = Mockito.mock(IConsensus.class);
            final DataNodeRegionManager regionManager = Mockito.mock(DataNodeRegionManager.class);
            final SchemaEngine schemaEngine = Mockito.mock(SchemaEngine.class);
            final SchemaRegionId regionId = new SchemaRegionId(1);
            final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            // Retain the lock even when the region has already been cleared during shutdown.
            Mockito.when(regionManager.getRegionLock(regionId)).thenReturn(lock);
            final ISchemaRegion region = Mockito.mock(ISchemaRegion.class);
            Mockito.when(schemaEngine.getSchemaRegion(regionId))
                .thenReturn(available ? region : null);
            if (node instanceof AlterTimeSeriesNode alterNode) {
              Mockito.when(region.fetchMeasurementPath(alterNode.getPath()))
                  .thenReturn(new MeasurementPath("root.sg.d.s", TSDataType.INT64));
            }
            final RegionWriteExecutor executor =
                new RegionWriteExecutor(
                    Mockito.mock(IConsensus.class),
                    consensus,
                    regionManager,
                    schemaEngine,
                    Mockito.mock(ClusterTemplateManager.class),
                    Mockito.mock(TriggerFireVisitor.class));
            final WritePlanNode request = fromPipe ? new PipeEnrichedWritePlanNode(node) : node;
            Mockito.when(consensus.write(regionId, request))
                .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

            final boolean validatesRegion =
                !(node instanceof CreateLogicalViewNode)
                    || ConsensusFactory.RATIS_CONSENSUS.equals(protocol);
            final boolean accepted = available || !validatesRegion;
            final RegionExecutionResult result = executor.execute(regionId, request);
            assertEquals(accepted, result.isAccepted());
            assertEquals(
                (accepted ? TSStatusCode.SUCCESS_STATUS : TSStatusCode.NO_AVAILABLE_REGION_GROUP)
                    .getStatusCode(),
                result.getStatus().getCode());
            assertFalse(lock.isWriteLocked());
            assertEquals(0, lock.getReadLockCount());
            if (accepted) {
              Mockito.verify(consensus).write(regionId, request);
              if (node instanceof AlterTimeSeriesNode alterNode) {
                Mockito.verify(region).fetchMeasurementPath(alterNode.getPath());
              } else if (node instanceof CreateOrUpdateTableDeviceNode tableNode) {
                Mockito.verify(region)
                    .checkSchemaQuota(tableNode.getTableName(), tableNode.getDeviceIdList());
              } else if (validatesRegion) {
                Mockito.verify(region)
                    .checkMeasurementExistence(
                        new PartialPath("root.sg.d"), Collections.singletonList("v"), null);
              }
            } else {
              Mockito.verifyZeroInteractions(consensus);
              assertEquals(result.getMessage(), result.getStatus().getMessage());
            }
          }
        }
      }
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setSchemaRegionConsensusProtocolClass(originalProtocol);
    }
  }

  @Test
  public void testInternalCreateTimeSeriesAfterSchemaRegionCleared() throws Exception {
    // A shutdown can clear the region while its validation lock remains registered.
    checkInternalCreateWithRegionAvailability(false, false);
  }

  @Test
  public void testInternalCreateMultiTimeSeriesAfterSchemaRegionCleared() throws Exception {
    // Batch and Pipe auto-creation must return a retryable status for the same shutdown window.
    checkInternalCreateWithRegionAvailability(true, false);
  }

  @Test
  public void testInternalCreateWithAvailableSchemaRegion() throws Exception {
    // The guard must preserve quota checks and consensus writes for both live-region requests.
    checkInternalCreateWithRegionAvailability(false, true);
    checkInternalCreateWithRegionAvailability(true, true);
  }

  private void checkInternalCreateWithRegionAvailability(boolean batch, boolean available)
      throws Exception {
    final String originalProtocol =
        IoTDBDescriptor.getInstance().getConfig().getSchemaRegionConsensusProtocolClass();
    try {
      for (String protocol :
          new String[] {ConsensusFactory.SIMPLE_CONSENSUS, ConsensusFactory.RATIS_CONSENSUS}) {
        IoTDBDescriptor.getInstance().getConfig().setSchemaRegionConsensusProtocolClass(protocol);
        for (boolean fromPipe : new boolean[] {false, true}) {
          final IConsensus consensus = Mockito.mock(IConsensus.class);
          final DataNodeRegionManager regionManager = Mockito.mock(DataNodeRegionManager.class);
          final SchemaEngine schemaEngine = Mockito.mock(SchemaEngine.class);
          final SchemaRegionId regionId = new SchemaRegionId(1);
          final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
          Mockito.when(regionManager.getRegionLock(regionId)).thenReturn(lock);
          final ISchemaRegion region = Mockito.mock(ISchemaRegion.class);
          Mockito.when(schemaEngine.getSchemaRegion(regionId))
              .thenReturn(available ? region : null);
          final RegionWriteExecutor executor =
              new RegionWriteExecutor(
                  Mockito.mock(IConsensus.class),
                  consensus,
                  regionManager,
                  schemaEngine,
                  Mockito.mock(ClusterTemplateManager.class),
                  Mockito.mock(TriggerFireVisitor.class));
          final PartialPath device = new PartialPath("root.sg.d");
          final MeasurementGroup measurements = new MeasurementGroup();
          measurements.addMeasurement(
              "s", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
          final WritePlanNode node =
              batch
                  ? new InternalCreateMultiTimeSeriesNode(
                      new PlanNodeId("test"),
                      Collections.singletonMap(device, new Pair<>(false, measurements)))
                  : new InternalCreateTimeSeriesNode(
                      new PlanNodeId("test"), device, measurements, false);
          final WritePlanNode request = fromPipe ? new PipeEnrichedWritePlanNode(node) : node;
          Mockito.when(consensus.write(regionId, request))
              .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

          final RegionExecutionResult result = executor.execute(regionId, request);
          assertEquals(available, result.isAccepted());
          assertEquals(
              (available ? TSStatusCode.SUCCESS_STATUS : TSStatusCode.NO_AVAILABLE_REGION_GROUP)
                  .getStatusCode(),
              result.getStatus().getCode());
          assertFalse(lock.isWriteLocked());
          assertEquals(0, lock.getReadLockCount());
          if (available) {
            Mockito.verify(region).checkSchemaQuota(device, 1);
            Mockito.verify(consensus).write(regionId, request);
          } else {
            Mockito.verifyZeroInteractions(consensus);
            assertEquals(1, measurements.size());
          }
        }
      }
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setSchemaRegionConsensusProtocolClass(originalProtocol);
    }
  }

  @Test
  public void testInsertRowNode() throws ConsensusException {

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

    TSStatus writeResponse = Mockito.mock(TSStatus.class);
    Mockito.when(writeResponse.getCode()).thenReturn(TSStatusCode.SUCCESS_STATUS.getStatusCode());

    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.BEFORE_INSERT))
        .thenReturn(TriggerFireResult.TERMINATION);
    RegionExecutionResult res = executor.execute(dataRegionGroupId, planNode);
    assertFalse(res.isAccepted());

    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.BEFORE_INSERT))
        .thenReturn(TriggerFireResult.SUCCESS);
    Mockito.when(dataRegionConsensus.write(dataRegionGroupId, planNode)).thenReturn(writeResponse);
    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.AFTER_INSERT))
        .thenReturn(TriggerFireResult.TERMINATION);
    res = executor.execute(dataRegionGroupId, planNode);
    assertFalse(res.isAccepted());

    Mockito.when(triggerFireVisitor.process(planNode, TriggerEvent.AFTER_INSERT))
        .thenReturn(TriggerFireResult.SUCCESS);
    res = executor.execute(dataRegionGroupId, planNode);
    assertTrue(res.isAccepted());

    Mockito.when(dataRegionConsensus.write(dataRegionGroupId, planNode))
        .thenThrow(new ConsensusException("Error!"));
    res = executor.execute(dataRegionGroupId, planNode);
    assertFalse(res.isAccepted());
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
