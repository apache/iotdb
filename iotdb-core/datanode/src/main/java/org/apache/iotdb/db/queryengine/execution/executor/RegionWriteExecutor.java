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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.trigger.executor.TriggerFireResult;
import org.apache.iotdb.db.trigger.executor.TriggerFireVisitor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegionWriteExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionWriteExecutor.class);

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private static final String METADATA_ERROR_MSG = "Metadata error: ";

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final IConsensus dataRegionConsensus;

  private final IConsensus schemaRegionConsensus;

  private final DataNodeRegionManager regionManager;

  private final SchemaEngine schemaEngine;

  private final ClusterTemplateManager clusterTemplateManager;

  private final TriggerFireVisitor triggerFireVisitor;

  private final WritePlanNodeExecutionVisitor executionVisitor;

  private final PipeEnrichedWriteSchemaNodeExecutionVisitor pipeExecutionVisitor;

  public RegionWriteExecutor() {
    dataRegionConsensus = DataRegionConsensusImpl.getInstance();
    schemaRegionConsensus = SchemaRegionConsensusImpl.getInstance();
    regionManager = DataNodeRegionManager.getInstance();
    schemaEngine = SchemaEngine.getInstance();
    clusterTemplateManager = ClusterTemplateManager.getInstance();
    triggerFireVisitor = new TriggerFireVisitor();
    executionVisitor = new WritePlanNodeExecutionVisitor();
    pipeExecutionVisitor = new PipeEnrichedWriteSchemaNodeExecutionVisitor(executionVisitor);
  }

  @TestOnly
  public RegionWriteExecutor(
      IConsensus dataRegionConsensus,
      IConsensus schemaRegionConsensus,
      DataNodeRegionManager regionManager,
      SchemaEngine schemaEngine,
      ClusterTemplateManager clusterTemplateManager,
      TriggerFireVisitor triggerFireVisitor) {
    this.dataRegionConsensus = dataRegionConsensus;
    this.schemaRegionConsensus = schemaRegionConsensus;
    this.regionManager = regionManager;
    this.schemaEngine = schemaEngine;
    this.clusterTemplateManager = clusterTemplateManager;
    this.triggerFireVisitor = triggerFireVisitor;
    executionVisitor = new WritePlanNodeExecutionVisitor();
    pipeExecutionVisitor = new PipeEnrichedWriteSchemaNodeExecutionVisitor(executionVisitor);
  }

  @SuppressWarnings("squid:S1181")
  public RegionExecutionResult execute(ConsensusGroupId groupId, PlanNode planNode) {
    try {
      WritePlanNodeExecutionContext context =
          new WritePlanNodeExecutionContext(groupId, regionManager.getRegionLock(groupId));
      return planNode.accept(executionVisitor, context);
    } catch (Throwable e) {
      LOGGER.warn(e.getMessage(), e);
      return RegionExecutionResult.create(
          false,
          e.getMessage(),
          RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  private class WritePlanNodeExecutionVisitor
      extends PlanVisitor<RegionExecutionResult, WritePlanNodeExecutionContext> {

    @Override
    public RegionExecutionResult visitPlan(
        final PlanNode node, final WritePlanNodeExecutionContext context) {

      if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
        return RegionExecutionResult.create(
            false,
            "Fail to do non-query operations because system is read-only.",
            RpcUtils.getStatus(
                TSStatusCode.SYSTEM_READ_ONLY,
                "Fail to do non-query operations because system is read-only."));
      }

      try {
        final TSStatus status = executePlanNodeInConsensusLayer(context.getRegionId(), node);
        return RegionExecutionResult.create(
            TSStatusCode.SUCCESS_STATUS.getStatusCode() == status.getCode(),
            status.getMessage(),
            status);
      } catch (final ConsensusException e) {
        LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
        TSStatus status = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
        if (e instanceof ConsensusGroupNotExistException) {
          status.setCode(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode());
        }
        return RegionExecutionResult.create(false, e.toString(), status);
      }
    }

    private TSStatus executePlanNodeInConsensusLayer(
        final ConsensusGroupId groupId, final PlanNode planNode) throws ConsensusException {
      if (groupId instanceof DataRegionId) {
        return dataRegionConsensus.write(groupId, planNode);
      } else {
        return schemaRegionConsensus.write(groupId, planNode);
      }
    }

    @Override
    public RegionExecutionResult visitInsertRow(
        final InsertRowNode node, final WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertTablet(
        final InsertTabletNode node, final WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitRelationalInsertTablet(
        final RelationalInsertTabletNode node, final WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertRows(
        final InsertRowsNode node, final WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertMultiTablets(
        final InsertMultiTabletsNode node, final WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertRowsOfOneDevice(
        final InsertRowsOfOneDeviceNode node, final WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitPipeEnrichedInsertNode(
        final PipeEnrichedInsertNode node, final WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    private RegionExecutionResult executeDataInsert(
        final InsertNode insertNode, final WritePlanNodeExecutionContext context) {
      if (context.getRegionWriteValidationRWLock() == null) {
        final String message =
            "Failed to get the lock of the region because the region is not existed.";
        return RegionExecutionResult.create(
            false, message, RpcUtils.getStatus(TSStatusCode.NO_AVAILABLE_REGION_GROUP, message));
      }

      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        final TSStatus status = fireTriggerAndInsert(context.getRegionId(), insertNode);
        return RegionExecutionResult.create(
            TSStatusCode.SUCCESS_STATUS.getStatusCode() == status.getCode(),
            status.message,
            status);
      } catch (ConsensusException e) {
        LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
        final TSStatus status = RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_ERROR, e.toString());
        if (e instanceof ConsensusGroupNotExistException) {
          status.setCode(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode());
        }
        return RegionExecutionResult.create(false, e.toString(), status);
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    private TSStatus fireTriggerAndInsert(
        final ConsensusGroupId groupId, final InsertNode insertNode) throws ConsensusException {
      long triggerCostTime = 0;
      TSStatus status;
      long startTime = System.nanoTime();
      // fire Trigger before the insertion
      TriggerFireResult result = triggerFireVisitor.process(insertNode, TriggerEvent.BEFORE_INSERT);
      triggerCostTime += (System.nanoTime() - startTime);
      if (result.equals(TriggerFireResult.TERMINATION)) {
        status =
            RpcUtils.getStatus(
                TSStatusCode.TRIGGER_FIRE_ERROR.getStatusCode(),
                "Failed to complete the insertion because trigger error before the insertion.");
      } else {
        final long startWriteTime = System.nanoTime();
        status = dataRegionConsensus.write(groupId, insertNode);
        PERFORMANCE_OVERVIEW_METRICS.recordScheduleStorageCost(System.nanoTime() - startWriteTime);

        // fire Trigger after the insertion
        startTime = System.nanoTime();
        boolean hasFailedTriggerBeforeInsertion =
            result.equals(TriggerFireResult.FAILED_NO_TERMINATION);
        result = triggerFireVisitor.process(insertNode, TriggerEvent.AFTER_INSERT);
        if (hasFailedTriggerBeforeInsertion || !result.equals(TriggerFireResult.SUCCESS)) {
          status =
              RpcUtils.getStatus(
                  TSStatusCode.TRIGGER_FIRE_ERROR.getStatusCode(),
                  "Meet trigger error before/after the insertion, the insertion itself is completed.");
        }
        triggerCostTime += (System.nanoTime() - startTime);
      }
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleTriggerCost(triggerCostTime);
      return status;
    }

    @Override
    public RegionExecutionResult visitPipeEnrichedDeleteDataNode(
        final PipeEnrichedDeleteDataNode node, final WritePlanNodeExecutionContext context) {
      // data deletion should block data insertion, especially when executed for deleting timeseries
      context.getRegionWriteValidationRWLock().writeLock().lock();
      try {
        return super.visitPipeEnrichedDeleteDataNode(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().writeLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitDeleteData(
        final DeleteDataNode node, final WritePlanNodeExecutionContext context) {
      // data deletion don't need to block data insertion, but there are some creation operation
      // require write lock on data region.
      context.getRegionWriteValidationRWLock().writeLock().lock();
      try {
        return super.visitDeleteData(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().writeLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitDeleteData(
        final RelationalDeleteDataNode node, final WritePlanNodeExecutionContext context) {
      // data deletion don't need to block data insertion, but there are some creation operation
      // require write lock on data region.
      context.getRegionWriteValidationRWLock().writeLock().lock();
      try {
        return super.visitDeleteData(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().writeLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitCreateTimeSeries(
        final CreateTimeSeriesNode node, final WritePlanNodeExecutionContext context) {
      return executeCreateTimeSeries(node, context, false);
    }

    private RegionExecutionResult executeCreateTimeSeries(
        final CreateTimeSeriesNode node,
        final WritePlanNodeExecutionContext context,
        final boolean receivedFromPipe) {
      final ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      final RegionExecutionResult result =
          checkQuotaBeforeCreatingTimeSeries(schemaRegion, node.getPath().getDevicePath(), 1);
      if (result != null) {
        return result;
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
          && !receivedFromPipe) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          final Map<Integer, MetadataException> failingMeasurementMap =
              schemaRegion.checkMeasurementExistence(
                  node.getPath().getDevicePath(),
                  Collections.singletonList(node.getPath().getMeasurement()),
                  Collections.singletonList(node.getAlias()));
          if (failingMeasurementMap.isEmpty()) {
            return super.visitCreateTimeSeries(node, context);
          } else {
            final MetadataException metadataException = failingMeasurementMap.get(0);
            LOGGER.warn(METADATA_ERROR_MSG, metadataException);
            return RegionExecutionResult.create(
                false,
                metadataException.getMessage(),
                RpcUtils.getStatus(
                    metadataException.getErrorCode(), metadataException.getMessage()));
          }
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitCreateTimeSeries(node, context);
      }
    }

    @Override
    public RegionExecutionResult visitCreateAlignedTimeSeries(
        final CreateAlignedTimeSeriesNode node, final WritePlanNodeExecutionContext context) {
      return executeCreateAlignedTimeSeries(node, context, false);
    }

    private RegionExecutionResult executeCreateAlignedTimeSeries(
        final CreateAlignedTimeSeriesNode node,
        final WritePlanNodeExecutionContext context,
        final boolean receivedFromPipe) {
      final ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      final RegionExecutionResult result =
          checkQuotaBeforeCreatingTimeSeries(
              schemaRegion, node.getDevicePath(), node.getMeasurements().size());
      if (result != null) {
        return result;
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
          && !receivedFromPipe) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          final Map<Integer, MetadataException> failingMeasurementMap =
              schemaRegion.checkMeasurementExistence(
                  node.getDevicePath(), node.getMeasurements(), node.getAliasList());
          if (failingMeasurementMap.isEmpty()) {
            return super.visitCreateAlignedTimeSeries(node, context);
          } else {
            final MetadataException metadataException =
                failingMeasurementMap.values().iterator().next();
            LOGGER.warn(METADATA_ERROR_MSG, metadataException);
            return RegionExecutionResult.create(
                false,
                metadataException.getMessage(),
                RpcUtils.getStatus(
                    metadataException.getErrorCode(), metadataException.getMessage()));
          }
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitCreateAlignedTimeSeries(node, context);
      }
    }

    @Override
    public RegionExecutionResult visitCreateMultiTimeSeries(
        final CreateMultiTimeSeriesNode node, final WritePlanNodeExecutionContext context) {
      return executeCreateMultiTimeSeries(node, context, false);
    }

    private RegionExecutionResult executeCreateMultiTimeSeries(
        final CreateMultiTimeSeriesNode node,
        final WritePlanNodeExecutionContext context,
        final boolean receivedFromPipe) {
      final ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      RegionExecutionResult result;
      for (final Map.Entry<PartialPath, MeasurementGroup> entry :
          node.getMeasurementGroupMap().entrySet()) {
        result =
            checkQuotaBeforeCreatingTimeSeries(
                schemaRegion, entry.getKey(), entry.getValue().getMeasurements().size());
        if (result != null) {
          return result;
        }
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
          && !receivedFromPipe) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          final List<TSStatus> failingStatus = new ArrayList<>();
          final Map<PartialPath, MeasurementGroup> measurementGroupMap =
              node.getMeasurementGroupMap();
          final List<PartialPath> emptyDeviceList = new ArrayList<>();

          checkMeasurementExistence(
              measurementGroupMap, schemaRegion, failingStatus, emptyDeviceList);

          for (final PartialPath emptyDevice : emptyDeviceList) {
            measurementGroupMap.remove(emptyDevice);
          }

          final RegionExecutionResult failingResult =
              registerTimeSeries(measurementGroupMap, node, context, failingStatus);

          if (failingResult != null) {
            return failingResult;
          }

          final TSStatus status = RpcUtils.getStatus(failingStatus);
          return RegionExecutionResult.create(false, status.getMessage(), status);
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitCreateMultiTimeSeries(node, context);
      }
    }

    private void checkMeasurementExistence(
        final Map<PartialPath, MeasurementGroup> measurementGroupMap,
        final ISchemaRegion schemaRegion,
        final List<TSStatus> failingStatus,
        final List<PartialPath> emptyDeviceList) {
      for (final Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
        final Map<Integer, MetadataException> failingMeasurementMap =
            schemaRegion.checkMeasurementExistence(
                entry.getKey(),
                entry.getValue().getMeasurements(),
                entry.getValue().getAliasList());
        if (failingMeasurementMap.isEmpty()) {
          continue;
        }

        for (final Map.Entry<Integer, MetadataException> failingMeasurement :
            failingMeasurementMap.entrySet()) {
          LOGGER.warn(METADATA_ERROR_MSG, failingMeasurement.getValue());
          failingStatus.add(
              RpcUtils.getStatus(
                  failingMeasurement.getValue().getErrorCode(),
                  failingMeasurement.getValue().getMessage()));
        }
        entry.getValue().removeMeasurements(failingMeasurementMap.keySet());

        if (entry.getValue().isEmpty()) {
          emptyDeviceList.add(entry.getKey());
        }
      }
    }

    private RegionExecutionResult registerTimeSeries(
        final Map<PartialPath, MeasurementGroup> measurementGroupMap,
        final CreateMultiTimeSeriesNode node,
        final WritePlanNodeExecutionContext context,
        final List<TSStatus> failingStatus) {
      if (!measurementGroupMap.isEmpty()) {
        // try registering the rest timeseries
        final RegionExecutionResult executionResult =
            super.visitCreateMultiTimeSeries(node, context);
        if (failingStatus.isEmpty()) {
          return executionResult;
        }

        final TSStatus executionStatus = executionResult.getStatus();
        if (executionStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          failingStatus.addAll(executionStatus.getSubStatus());
        } else if (executionStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failingStatus.add(executionStatus);
        }
      }
      return null;
    }

    @Override
    public RegionExecutionResult visitInternalCreateTimeSeries(
        final InternalCreateTimeSeriesNode node, final WritePlanNodeExecutionContext context) {
      return executeInternalCreateTimeSeries(node, context, false);
    }

    private RegionExecutionResult executeInternalCreateTimeSeries(
        final InternalCreateTimeSeriesNode node,
        final WritePlanNodeExecutionContext context,
        final boolean receivedFromPipe) {
      final ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      final RegionExecutionResult result =
          checkQuotaBeforeCreatingTimeSeries(
              schemaRegion, node.getDevicePath(), node.getMeasurementGroup().size());
      if (result != null) {
        return result;
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
          && !receivedFromPipe) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          final List<TSStatus> failingStatus = new ArrayList<>();
          final List<TSStatus> alreadyExistingStatus = new ArrayList<>();
          final MeasurementGroup measurementGroup = node.getMeasurementGroup();
          final Map<Integer, MetadataException> failingMeasurementMap =
              schemaRegion.checkMeasurementExistence(
                  node.getDevicePath(),
                  measurementGroup.getMeasurements(),
                  measurementGroup.getAliasList());
          MetadataException metadataException;
          // filter failed measurement and keep the rest for execution
          for (final Map.Entry<Integer, MetadataException> failingMeasurement :
              failingMeasurementMap.entrySet()) {
            metadataException = failingMeasurement.getValue();
            if (metadataException.getErrorCode()
                == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
              // There's no need to internal create timeseries.
              alreadyExistingStatus.add(
                  RpcUtils.getStatus(
                      metadataException.getErrorCode(),
                      MeasurementPath.transformDataToString(
                          ((MeasurementAlreadyExistException) metadataException)
                              .getMeasurementPath())));
            } else {
              final int errorCode = metadataException.getErrorCode();
              if (errorCode != TSStatusCode.PATH_ALREADY_EXIST.getStatusCode()
                  || errorCode != TSStatusCode.ALIAS_ALREADY_EXIST.getStatusCode()) {
                LOGGER.warn(METADATA_ERROR_MSG, metadataException);
              }
              failingStatus.add(
                  RpcUtils.getStatus(
                      metadataException.getErrorCode(), metadataException.getMessage()));
            }
          }
          measurementGroup.removeMeasurements(failingMeasurementMap.keySet());

          return processExecutionResultOfInternalCreateSchema(
              super.visitInternalCreateTimeSeries(node, context),
              failingStatus,
              alreadyExistingStatus);
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitInternalCreateTimeSeries(node, context);
      }
    }

    @Override
    public RegionExecutionResult visitInternalCreateMultiTimeSeries(
        final InternalCreateMultiTimeSeriesNode node, final WritePlanNodeExecutionContext context) {
      return executeInternalCreateMultiTimeSeries(node, context, false);
    }

    private RegionExecutionResult executeInternalCreateMultiTimeSeries(
        final InternalCreateMultiTimeSeriesNode node,
        final WritePlanNodeExecutionContext context,
        final boolean receivedFromPipe) {
      final ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      RegionExecutionResult result;
      for (final Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> deviceEntry :
          node.getDeviceMap().entrySet()) {
        result =
            checkQuotaBeforeCreatingTimeSeries(
                schemaRegion, deviceEntry.getKey(), deviceEntry.getValue().getRight().size());
        if (result != null) {
          return result;
        }
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
          && !receivedFromPipe) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          final List<TSStatus> failingStatus = new ArrayList<>();
          final List<TSStatus> alreadyExistingStatus = new ArrayList<>();

          MeasurementGroup measurementGroup;
          Map<Integer, MetadataException> failingMeasurementMap;
          MetadataException metadataException;
          for (final Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> deviceEntry :
              node.getDeviceMap().entrySet()) {
            measurementGroup = deviceEntry.getValue().right;
            failingMeasurementMap =
                schemaRegion.checkMeasurementExistence(
                    deviceEntry.getKey(),
                    measurementGroup.getMeasurements(),
                    measurementGroup.getAliasList());
            // filter failed measurement and keep the rest for execution
            for (final Map.Entry<Integer, MetadataException> failingMeasurement :
                failingMeasurementMap.entrySet()) {
              metadataException = failingMeasurement.getValue();
              if (metadataException.getErrorCode()
                  == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
                // There's no need to internal create time series.
                alreadyExistingStatus.add(
                    RpcUtils.getStatus(
                        metadataException.getErrorCode(),
                        MeasurementPath.transformDataToString(
                            ((MeasurementAlreadyExistException) metadataException)
                                .getMeasurementPath())));
              } else {
                LOGGER.warn(METADATA_ERROR_MSG, metadataException);
                failingStatus.add(
                    RpcUtils.getStatus(
                        metadataException.getErrorCode(), metadataException.getMessage()));
              }
            }
            measurementGroup.removeMeasurements(failingMeasurementMap.keySet());
          }

          return processExecutionResultOfInternalCreateSchema(
              super.visitInternalCreateMultiTimeSeries(node, context),
              failingStatus,
              alreadyExistingStatus);
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitInternalCreateMultiTimeSeries(node, context);
      }
    }

    /**
     * Check the quota before creating time series.
     *
     * @return null if the quota is not exceeded, otherwise return the execution result.
     */
    private RegionExecutionResult checkQuotaBeforeCreatingTimeSeries(
        ISchemaRegion schemaRegion, PartialPath path, int size) {
      try {
        schemaRegion.checkSchemaQuota(path, size);
      } catch (SchemaQuotaExceededException e) {
        return RegionExecutionResult.create(
            false, e.getMessage(), RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
      return null;
    }

    private RegionExecutionResult processExecutionResultOfInternalCreateSchema(
        RegionExecutionResult executionResult,
        List<TSStatus> failingStatus,
        List<TSStatus> alreadyExistingStatus) {
      TSStatus executionStatus = executionResult.getStatus();

      separateMeasurementAlreadyExistException(
          failingStatus, executionStatus, alreadyExistingStatus);

      boolean isAccepted = true;
      TSStatus status;
      if (failingStatus.isEmpty() && alreadyExistingStatus.isEmpty()) {
        status = RpcUtils.SUCCESS_STATUS;
      } else if (failingStatus.isEmpty()) {
        status = RpcUtils.getStatus(alreadyExistingStatus);
      } else {
        status = RpcUtils.getStatus(failingStatus);
        isAccepted = false;
      }
      return RegionExecutionResult.create(isAccepted, status.getMessage(), status);
    }

    private void separateMeasurementAlreadyExistException(
        List<TSStatus> failingStatus,
        TSStatus executionStatus,
        List<TSStatus> alreadyExistingStatus) {
      // separate the measurement_already_exist exception and other exceptions process,
      // measurement_already_exist exception is acceptable due to concurrent timeseries creation
      if (failingStatus.isEmpty()) {
        if (executionStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          if (executionStatus.getSubStatus().get(0).getCode()
              == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
            // there's only measurement_already_exist exception
            alreadyExistingStatus.addAll(executionStatus.getSubStatus());
          } else {
            failingStatus.addAll(executionStatus.getSubStatus());
          }
        } else if (executionStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failingStatus.add(executionStatus);
        }
      } else {
        if (executionStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          if (executionStatus.getSubStatus().get(0).getCode()
              != TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
            failingStatus.addAll(executionStatus.getSubStatus());
          }
        } else if (executionStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          failingStatus.add(executionStatus);
        }
      }
    }

    @Override
    public RegionExecutionResult visitAlterTimeSeries(
        AlterTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      return executeAlterTimeSeries(node, context, false);
    }

    private RegionExecutionResult executeAlterTimeSeries(
        AlterTimeSeriesNode node, WritePlanNodeExecutionContext context, boolean receivedFromPipe) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      try {
        MeasurementPath measurementPath = schemaRegion.fetchMeasurementPath(node.getPath());
        if (node.isAlterView() && !measurementPath.getMeasurementSchema().isLogicalView()) {
          throw new MetadataException(
              String.format("%s is not view.", measurementPath.getFullPath()));
        }
        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitAlterTimeSeries(node, context);
      } catch (MetadataException e) {
        return RegionExecutionResult.create(
            true, e.getMessage(), RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }

    @Override
    public RegionExecutionResult visitActivateTemplate(
        ActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      return executeActivateTemplate(node, context, false);
    }

    private RegionExecutionResult executeActivateTemplate(
        ActivateTemplateNode node,
        WritePlanNodeExecutionContext context,
        boolean receivedFromPipe) {
      // activate template operation shall be blocked by unset template check
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        Pair<Template, PartialPath> templateSetInfo =
            clusterTemplateManager.checkTemplateSetInfo(node.getActivatePath());
        if (templateSetInfo == null) {
          // The activation has already been validated during analyzing.
          // That means the template is being unset during the activation plan transport.
          String message =
              String.format(
                  "Template is being unsetting from path %s. Please try activating later.",
                  node.getPathSetTemplate());
          return RegionExecutionResult.create(
              false, message, RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, message));
        }
        ISchemaRegion schemaRegion =
            schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
        RegionExecutionResult result =
            checkQuotaBeforeCreatingTimeSeries(
                schemaRegion, node.getActivatePath(), templateSetInfo.left.getMeasurementNumber());
        if (result == null) {
          return receivedFromPipe
              ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
              : super.visitActivateTemplate(node, context);
        } else {
          return result;
        }
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitBatchActivateTemplate(
        BatchActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      return executeBatchActivateTemplate(node, context, false);
    }

    private RegionExecutionResult executeBatchActivateTemplate(
        BatchActivateTemplateNode node,
        WritePlanNodeExecutionContext context,
        boolean receivedFromPipe) {
      // activate template operation shall be blocked by unset template check
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        ISchemaRegion schemaRegion =
            schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
        for (PartialPath devicePath : node.getTemplateActivationMap().keySet()) {
          Pair<Template, PartialPath> templateSetInfo =
              clusterTemplateManager.checkTemplateSetInfo(devicePath);
          if (templateSetInfo == null) {
            // The activation has already been validated during analyzing.
            // That means the template is being unset during the activation plan transport.
            String message =
                String.format(
                    "Template is being unsetting from path %s. Please try activating later.",
                    node.getPathSetTemplate(devicePath));
            return RegionExecutionResult.create(
                false, message, RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, message));
          }
          RegionExecutionResult result =
              checkQuotaBeforeCreatingTimeSeries(
                  schemaRegion, devicePath, templateSetInfo.left.getMeasurementNumber());
          if (result != null) {
            return result;
          }
        }

        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitBatchActivateTemplate(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitInternalBatchActivateTemplate(
        InternalBatchActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      return executeInternalBatchActivateTemplate(node, context, false);
    }

    private RegionExecutionResult executeInternalBatchActivateTemplate(
        InternalBatchActivateTemplateNode node,
        WritePlanNodeExecutionContext context,
        boolean receivedFromPipe) {
      // activate template operation shall be blocked by unset template check
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        ISchemaRegion schemaRegion =
            schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
        for (Map.Entry<PartialPath, Pair<Integer, Integer>> entry :
            node.getTemplateActivationMap().entrySet()) {
          Pair<Template, PartialPath> templateSetInfo =
              clusterTemplateManager.checkTemplateSetInfo(entry.getKey());
          if (templateSetInfo == null) {
            // The activation has already been validated during analyzing.
            // That means the template is being unset during the activation plan transport.
            String message =
                String.format(
                    "Template is being unsetting from prefix path of %s. Please try activating later.",
                    new PartialPath(
                            Arrays.copyOf(entry.getKey().getNodes(), entry.getValue().right + 1))
                        .getFullPath());
            return RegionExecutionResult.create(
                false, message, RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, message));
          }
          RegionExecutionResult result =
              checkQuotaBeforeCreatingTimeSeries(
                  schemaRegion, entry.getKey(), templateSetInfo.left.getMeasurementNumber());
          if (result != null) {
            return result;
          }
        }

        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitInternalBatchActivateTemplate(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitCreateLogicalView(
        CreateLogicalViewNode node, WritePlanNodeExecutionContext context) {
      return executeCreateLogicalView(node, context, false);
    }

    private RegionExecutionResult executeCreateLogicalView(
        final CreateLogicalViewNode node,
        final WritePlanNodeExecutionContext context,
        final boolean receivedFromPipe) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          // step 1. make sure all target paths do NOT exist.
          List<PartialPath> targetPaths = node.getViewPathList();
          List<MetadataException> failingMetadataException = new ArrayList<>();
          for (PartialPath thisPath : targetPaths) {
            // no alias list for a view, so the third parameter is null
            Map<Integer, MetadataException> failingMeasurementMap =
                schemaRegion.checkMeasurementExistence(
                    thisPath.getDevicePath(),
                    Collections.singletonList(thisPath.getMeasurement()),
                    null);
            // merge all exceptions into one map
            for (Map.Entry<Integer, MetadataException> entry : failingMeasurementMap.entrySet()) {
              failingMetadataException.add(entry.getValue());
            }
          }
          // if there are some exceptions, handle each exception and return first of them.
          if (!failingMetadataException.isEmpty()) {
            MetadataException metadataException = failingMetadataException.get(0);
            LOGGER.warn(METADATA_ERROR_MSG, metadataException);
            return RegionExecutionResult.create(
                false,
                metadataException.getMessage(),
                RpcUtils.getStatus(
                    metadataException.getErrorCode(), metadataException.getMessage()));
          }
          // step 2. make sure all source paths exist.
          return receivedFromPipe
              ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
              : super.visitCreateLogicalView(node, context);
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return receivedFromPipe
            ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
            : super.visitCreateLogicalView(node, context);
      }
      // end of visitCreateLogicalView
    }

    @Override
    public RegionExecutionResult visitCreateOrUpdateTableDevice(
        final CreateOrUpdateTableDeviceNode node, final WritePlanNodeExecutionContext context) {
      return executeCreateOrUpdateTableDevice(node, context, false);
    }

    private RegionExecutionResult executeCreateOrUpdateTableDevice(
        final CreateOrUpdateTableDeviceNode node,
        final WritePlanNodeExecutionContext context,
        final boolean receivedFromPipe) {
      final ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      try {
        schemaRegion.checkSchemaQuota(node.getTableName(), node.getDeviceIdList());
      } catch (final SchemaQuotaExceededException e) {
        return RegionExecutionResult.create(
            false, e.getMessage(), RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
      return receivedFromPipe
          ? super.visitPipeEnrichedWritePlanNode(new PipeEnrichedWritePlanNode(node), context)
          : super.visitCreateOrUpdateTableDevice(node, context);
    }

    @Override
    public RegionExecutionResult visitPipeEnrichedWritePlanNode(
        PipeEnrichedWritePlanNode node, WritePlanNodeExecutionContext context) {
      return node.getWritePlanNode().accept(pipeExecutionVisitor, context);
    }
  }

  private class PipeEnrichedWriteSchemaNodeExecutionVisitor
      extends PlanVisitor<RegionExecutionResult, WritePlanNodeExecutionContext> {

    WritePlanNodeExecutionVisitor visitor;

    private PipeEnrichedWriteSchemaNodeExecutionVisitor(WritePlanNodeExecutionVisitor visitor) {
      this.visitor = visitor;
    }

    @Override
    public RegionExecutionResult visitPlan(PlanNode node, WritePlanNodeExecutionContext context) {
      throw new UnsupportedOperationException(
          "PipeEnrichedWriteSchemaNodeExecutionVisitor does not support visiting general plan.");
    }

    @Override
    public RegionExecutionResult visitCreateTimeSeries(
        CreateTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeCreateTimeSeries(node, context, true);
    }

    @Override
    public RegionExecutionResult visitCreateAlignedTimeSeries(
        CreateAlignedTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeCreateAlignedTimeSeries(node, context, true);
    }

    @Override
    public RegionExecutionResult visitCreateMultiTimeSeries(
        CreateMultiTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeCreateMultiTimeSeries(node, context, true);
    }

    @Override
    public RegionExecutionResult visitInternalCreateTimeSeries(
        InternalCreateTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeInternalCreateTimeSeries(node, context, true);
    }

    @Override
    public RegionExecutionResult visitInternalCreateMultiTimeSeries(
        InternalCreateMultiTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeInternalCreateMultiTimeSeries(node, context, true);
    }

    @Override
    public RegionExecutionResult visitAlterTimeSeries(
        AlterTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeAlterTimeSeries(node, context, true);
    }

    @Override
    public RegionExecutionResult visitActivateTemplate(
        ActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeActivateTemplate(node, context, true);
    }

    @Override
    public RegionExecutionResult visitBatchActivateTemplate(
        BatchActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeBatchActivateTemplate(node, context, true);
    }

    @Override
    public RegionExecutionResult visitInternalBatchActivateTemplate(
        InternalBatchActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeInternalBatchActivateTemplate(node, context, true);
    }

    @Override
    public RegionExecutionResult visitCreateLogicalView(
        CreateLogicalViewNode node, WritePlanNodeExecutionContext context) {
      return visitor.executeCreateLogicalView(node, context, true);
    }

    @Override
    public RegionExecutionResult visitCreateOrUpdateTableDevice(
        final CreateOrUpdateTableDeviceNode node, final WritePlanNodeExecutionContext context) {
      return visitor.executeCreateOrUpdateTableDevice(node, context, true);
    }
  }

  private static class WritePlanNodeExecutionContext {

    private final ConsensusGroupId regionId;

    private final ReentrantReadWriteLock regionRWLock;

    WritePlanNodeExecutionContext(ConsensusGroupId regionId, ReentrantReadWriteLock regionRWLock) {
      this.regionId = regionId;
      this.regionRWLock = regionRWLock;
    }

    public ConsensusGroupId getRegionId() {
      return regionId;
    }

    public ReentrantReadWriteLock getRegionWriteValidationRWLock() {
      return regionRWLock;
    }
  }
}
