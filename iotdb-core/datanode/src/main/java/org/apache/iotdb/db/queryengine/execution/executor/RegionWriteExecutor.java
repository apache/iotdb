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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.PipeEnrichedInsertNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.trigger.executor.TriggerFireResult;
import org.apache.iotdb.db.trigger.executor.TriggerFireVisitor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.tsfile.utils.Pair;

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

  public RegionWriteExecutor() {
    dataRegionConsensus = DataRegionConsensusImpl.getInstance();
    schemaRegionConsensus = SchemaRegionConsensusImpl.getInstance();
    regionManager = DataNodeRegionManager.getInstance();
    schemaEngine = SchemaEngine.getInstance();
    clusterTemplateManager = ClusterTemplateManager.getInstance();
    triggerFireVisitor = new TriggerFireVisitor();
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
  }

  @SuppressWarnings("squid:S1181")
  public RegionExecutionResult execute(ConsensusGroupId groupId, PlanNode planNode) {
    try {
      WritePlanNodeExecutionContext context =
          new WritePlanNodeExecutionContext(groupId, regionManager.getRegionLock(groupId));
      WritePlanNodeExecutionVisitor executionVisitor = new WritePlanNodeExecutionVisitor();
      return planNode.accept(executionVisitor, context);
    } catch (Throwable e) {
      LOGGER.error(e.getMessage(), e);
      RegionExecutionResult result = new RegionExecutionResult();
      result.setAccepted(false);
      result.setMessage(e.getMessage());
      result.setStatus(RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
      return result;
    }
  }

  private class WritePlanNodeExecutionVisitor
      extends PlanVisitor<RegionExecutionResult, WritePlanNodeExecutionContext> {

    @Override
    public RegionExecutionResult visitPlan(PlanNode node, WritePlanNodeExecutionContext context) {
      RegionExecutionResult response = new RegionExecutionResult();

      if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
        response.setAccepted(false);
        response.setMessage("Fail to do non-query operations because system is read-only.");
        response.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.SYSTEM_READ_ONLY,
                "Fail to do non-query operations because system is read-only."));
        return response;
      }

      try {
        TSStatus status = executePlanNodeInConsensusLayer(context.getRegionId(), node);
        response.setAccepted(TSStatusCode.SUCCESS_STATUS.getStatusCode() == status.getCode());
        response.setMessage(status.getMessage());
        response.setStatus(status);
      } catch (ConsensusException e) {
        LOGGER.error("Failed in the write API executing the consensus layer due to: ", e);
        response.setAccepted(false);
        response.setMessage(e.toString());
        response.setStatus(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()));
      }
      return response;
    }

    private TSStatus executePlanNodeInConsensusLayer(ConsensusGroupId groupId, PlanNode planNode)
        throws ConsensusException {
      if (groupId instanceof DataRegionId) {
        return dataRegionConsensus.write(groupId, planNode);
      } else {
        return schemaRegionConsensus.write(groupId, planNode);
      }
    }

    @Override
    public RegionExecutionResult visitInsertRow(
        InsertRowNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertTablet(
        InsertTabletNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertRows(
        InsertRowsNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertMultiTablets(
        InsertMultiTabletsNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    @Override
    public RegionExecutionResult visitPipeEnrichedInsert(
        PipeEnrichedInsertNode node, WritePlanNodeExecutionContext context) {
      return executeDataInsert(node, context);
    }

    private RegionExecutionResult executeDataInsert(
        InsertNode insertNode, WritePlanNodeExecutionContext context) {
      RegionExecutionResult response = new RegionExecutionResult();
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        TSStatus status = fireTriggerAndInsert(context.getRegionId(), insertNode);
        response.setAccepted(TSStatusCode.SUCCESS_STATUS.getStatusCode() == status.getCode());
        response.setMessage(status.message);
        if (!response.isAccepted()) {
          response.setStatus(status);
        }
        return response;
      } catch (ConsensusException e) {
        LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
        response.setAccepted(false);
        response.setMessage(e.toString());
        response.setStatus(RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_ERROR, e.toString()));
        return response;
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    private TSStatus fireTriggerAndInsert(ConsensusGroupId groupId, PlanNode planNode)
        throws ConsensusException {
      long triggerCostTime = 0;
      TSStatus status;
      long startTime = System.nanoTime();
      // fire Trigger before the insertion
      TriggerFireResult result = triggerFireVisitor.process(planNode, TriggerEvent.BEFORE_INSERT);
      triggerCostTime += (System.nanoTime() - startTime);
      if (result.equals(TriggerFireResult.TERMINATION)) {
        status =
            RpcUtils.getStatus(
                TSStatusCode.TRIGGER_FIRE_ERROR.getStatusCode(),
                "Failed to complete the insertion because trigger error before the insertion.");
      } else {
        long startWriteTime = System.nanoTime();
        status = dataRegionConsensus.write(groupId, planNode);
        PERFORMANCE_OVERVIEW_METRICS.recordScheduleStorageCost(System.nanoTime() - startWriteTime);

        // fire Trigger after the insertion
        startTime = System.nanoTime();
        boolean hasFailedTriggerBeforeInsertion =
            result.equals(TriggerFireResult.FAILED_NO_TERMINATION);
        result = triggerFireVisitor.process(planNode, TriggerEvent.AFTER_INSERT);
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
    public RegionExecutionResult visitDeleteData(
        DeleteDataNode node, WritePlanNodeExecutionContext context) {
      // data deletion should block data insertion, especially when executed for deleting timeseries
      context.getRegionWriteValidationRWLock().writeLock().lock();
      try {
        return super.visitDeleteData(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().writeLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitCreateTimeSeries(
        CreateTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      RegionExecutionResult result =
          checkQuotaBeforeCreatingTimeSeries(schemaRegion, node.getPath().getDevicePath(), 1);
      if (result != null) {
        return result;
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          Map<Integer, MetadataException> failingMeasurementMap =
              schemaRegion.checkMeasurementExistence(
                  node.getPath().getDevicePath(),
                  Collections.singletonList(node.getPath().getMeasurement()),
                  Collections.singletonList(node.getAlias()));
          if (failingMeasurementMap.isEmpty()) {
            return super.visitCreateTimeSeries(node, context);
          } else {
            MetadataException metadataException = failingMeasurementMap.get(0);
            LOGGER.error(METADATA_ERROR_MSG, metadataException);
            result = new RegionExecutionResult();
            result.setAccepted(false);
            result.setMessage(metadataException.getMessage());
            result.setStatus(
                RpcUtils.getStatus(
                    metadataException.getErrorCode(), metadataException.getMessage()));
            return result;
          }
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return super.visitCreateTimeSeries(node, context);
      }
    }

    @Override
    public RegionExecutionResult visitCreateAlignedTimeSeries(
        CreateAlignedTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      RegionExecutionResult result =
          checkQuotaBeforeCreatingTimeSeries(
              schemaRegion, node.getDevicePath(), node.getMeasurements().size());
      if (result != null) {
        return result;
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          Map<Integer, MetadataException> failingMeasurementMap =
              schemaRegion.checkMeasurementExistence(
                  node.getDevicePath(), node.getMeasurements(), node.getAliasList());
          if (failingMeasurementMap.isEmpty()) {
            return super.visitCreateAlignedTimeSeries(node, context);
          } else {
            MetadataException metadataException = failingMeasurementMap.values().iterator().next();
            LOGGER.error(METADATA_ERROR_MSG, metadataException);
            result = new RegionExecutionResult();
            result.setAccepted(false);
            result.setMessage(metadataException.getMessage());
            result.setStatus(
                RpcUtils.getStatus(
                    metadataException.getErrorCode(), metadataException.getMessage()));
            return result;
          }
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return super.visitCreateAlignedTimeSeries(node, context);
      }
    }

    @Override
    public RegionExecutionResult visitCreateMultiTimeSeries(
        CreateMultiTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      RegionExecutionResult result;
      for (Map.Entry<PartialPath, MeasurementGroup> entry :
          node.getMeasurementGroupMap().entrySet()) {
        result =
            checkQuotaBeforeCreatingTimeSeries(
                schemaRegion, entry.getKey(), entry.getValue().getMeasurements().size());
        if (result != null) {
          return result;
        }
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          List<TSStatus> failingStatus = new ArrayList<>();
          Map<PartialPath, MeasurementGroup> measurementGroupMap = node.getMeasurementGroupMap();
          List<PartialPath> emptyDeviceList = new ArrayList<>();

          checkMeasurementExistence(
              measurementGroupMap, schemaRegion, failingStatus, emptyDeviceList);

          for (PartialPath emptyDevice : emptyDeviceList) {
            measurementGroupMap.remove(emptyDevice);
          }

          RegionExecutionResult failingResult =
              registerTimeSeries(measurementGroupMap, node, context, failingStatus);

          if (failingResult != null) {
            return failingResult;
          }

          TSStatus status = RpcUtils.getStatus(failingStatus);
          failingResult = new RegionExecutionResult();
          failingResult.setAccepted(false);
          failingResult.setMessage(status.getMessage());
          failingResult.setStatus(status);
          return failingResult;
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return super.visitCreateMultiTimeSeries(node, context);
      }
    }

    private void checkMeasurementExistence(
        Map<PartialPath, MeasurementGroup> measurementGroupMap,
        ISchemaRegion schemaRegion,
        List<TSStatus> failingStatus,
        List<PartialPath> emptyDeviceList) {
      for (Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
        Map<Integer, MetadataException> failingMeasurementMap =
            schemaRegion.checkMeasurementExistence(
                entry.getKey(),
                entry.getValue().getMeasurements(),
                entry.getValue().getAliasList());
        if (failingMeasurementMap.isEmpty()) {
          continue;
        }

        for (Map.Entry<Integer, MetadataException> failingMeasurement :
            failingMeasurementMap.entrySet()) {
          LOGGER.error(METADATA_ERROR_MSG, failingMeasurement.getValue());
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
        Map<PartialPath, MeasurementGroup> measurementGroupMap,
        CreateMultiTimeSeriesNode node,
        WritePlanNodeExecutionContext context,
        List<TSStatus> failingStatus) {
      if (!measurementGroupMap.isEmpty()) {
        // try registering the rest timeseries
        RegionExecutionResult executionResult = super.visitCreateMultiTimeSeries(node, context);
        if (failingStatus.isEmpty()) {
          return executionResult;
        }

        TSStatus executionStatus = executionResult.getStatus();
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
        InternalCreateTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      RegionExecutionResult result =
          checkQuotaBeforeCreatingTimeSeries(
              schemaRegion, node.getDevicePath(), node.getMeasurementGroup().size());
      if (result != null) {
        return result;
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          List<TSStatus> failingStatus = new ArrayList<>();
          List<TSStatus> alreadyExistingStatus = new ArrayList<>();
          MeasurementGroup measurementGroup = node.getMeasurementGroup();
          Map<Integer, MetadataException> failingMeasurementMap =
              schemaRegion.checkMeasurementExistence(
                  node.getDevicePath(),
                  measurementGroup.getMeasurements(),
                  measurementGroup.getAliasList());
          MetadataException metadataException;
          // filter failed measurement and keep the rest for execution
          for (Map.Entry<Integer, MetadataException> failingMeasurement :
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
              LOGGER.warn(METADATA_ERROR_MSG, metadataException);
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
        return super.visitInternalCreateTimeSeries(node, context);
      }
    }

    @Override
    public RegionExecutionResult visitInternalCreateMultiTimeSeries(
        InternalCreateMultiTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      RegionExecutionResult result;
      for (Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> deviceEntry :
          node.getDeviceMap().entrySet()) {
        result =
            checkQuotaBeforeCreatingTimeSeries(
                schemaRegion, deviceEntry.getKey(), deviceEntry.getValue().getRight().size());
        if (result != null) {
          return result;
        }
      }
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          List<TSStatus> failingStatus = new ArrayList<>();
          List<TSStatus> alreadyExistingStatus = new ArrayList<>();

          MeasurementGroup measurementGroup;
          Map<Integer, MetadataException> failingMeasurementMap;
          MetadataException metadataException;
          for (Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> deviceEntry :
              node.getDeviceMap().entrySet()) {
            measurementGroup = deviceEntry.getValue().right;
            failingMeasurementMap =
                schemaRegion.checkMeasurementExistence(
                    deviceEntry.getKey(),
                    measurementGroup.getMeasurements(),
                    measurementGroup.getAliasList());
            // filter failed measurement and keep the rest for execution
            for (Map.Entry<Integer, MetadataException> failingMeasurement :
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
        return super.visitInternalCreateMultiTimeSeries(node, context);
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
        RegionExecutionResult result = new RegionExecutionResult();
        result.setAccepted(false);
        result.setMessage(e.getMessage());
        result.setStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        return result;
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

      RegionExecutionResult result = new RegionExecutionResult();
      TSStatus status;
      if (failingStatus.isEmpty() && alreadyExistingStatus.isEmpty()) {
        status = RpcUtils.SUCCESS_STATUS;
        result.setAccepted(true);
      } else if (failingStatus.isEmpty()) {
        status = RpcUtils.getStatus(alreadyExistingStatus);
        result.setAccepted(true);
      } else {
        status = RpcUtils.getStatus(failingStatus);
        result.setAccepted(false);
      }

      result.setMessage(status.getMessage());
      result.setStatus(status);
      return result;
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
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      try {
        List<MeasurementPath> measurementPathList =
            schemaRegion.fetchSchema(node.getPath(), Collections.emptyMap(), false);
        if (node.isAlterView()) {
          if (measurementPathList.isEmpty()) {
            throw new PathNotExistException(node.getPath().getFullPath());
          } else if (!measurementPathList.get(0).getMeasurementSchema().isLogicalView()) {
            throw new MetadataException(
                String.format("%s is not view.", measurementPathList.get(0).getFullPath()));
          }
        }
        return super.visitAlterTimeSeries(node, context);
      } catch (MetadataException e) {
        RegionExecutionResult result = new RegionExecutionResult();
        result.setAccepted(true);
        result.setMessage(e.getMessage());
        result.setStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        return result;
      }
    }

    @Override
    public RegionExecutionResult visitActivateTemplate(
        ActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      // activate template operation shall be blocked by unset template check
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        Pair<Template, PartialPath> templateSetInfo =
            clusterTemplateManager.checkTemplateSetInfo(node.getActivatePath());
        if (templateSetInfo == null) {
          // The activation has already been validated during analyzing.
          // That means the template is being unset during the activation plan transport.
          RegionExecutionResult result = new RegionExecutionResult();
          result.setAccepted(false);
          String message =
              String.format(
                  "Template is being unsetting from path %s. Please try activating later.",
                  node.getPathSetTemplate());
          result.setMessage(message);
          result.setStatus(RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, message));
          return result;
        }
        ISchemaRegion schemaRegion =
            schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
        RegionExecutionResult result =
            checkQuotaBeforeCreatingTimeSeries(
                schemaRegion, node.getActivatePath(), templateSetInfo.left.getMeasurementNumber());
        return result == null ? super.visitActivateTemplate(node, context) : result;
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitBatchActivateTemplate(
        BatchActivateTemplateNode node, WritePlanNodeExecutionContext context) {
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
            RegionExecutionResult result = new RegionExecutionResult();
            result.setAccepted(false);
            String message =
                String.format(
                    "Template is being unsetting from path %s. Please try activating later.",
                    node.getPathSetTemplate(devicePath));
            result.setMessage(message);
            result.setStatus(RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, message));
            return result;
          }
          RegionExecutionResult result =
              checkQuotaBeforeCreatingTimeSeries(
                  schemaRegion, devicePath, templateSetInfo.left.getMeasurementNumber());
          if (result != null) {
            return result;
          }
        }

        return super.visitBatchActivateTemplate(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitInternalBatchActivateTemplate(
        InternalBatchActivateTemplateNode node, WritePlanNodeExecutionContext context) {
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
            RegionExecutionResult result = new RegionExecutionResult();
            result.setAccepted(false);
            String message =
                String.format(
                    "Template is being unsetting from prefix path of %s. Please try activating later.",
                    new PartialPath(
                            Arrays.copyOf(entry.getKey().getNodes(), entry.getValue().right + 1))
                        .getFullPath());
            result.setMessage(message);
            result.setStatus(RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, message));
            return result;
          }
          RegionExecutionResult result =
              checkQuotaBeforeCreatingTimeSeries(
                  schemaRegion, entry.getKey(), templateSetInfo.left.getMeasurementNumber());
          if (result != null) {
            return result;
          }
        }

        return super.visitInternalBatchActivateTemplate(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
    }

    @Override
    public RegionExecutionResult visitCreateLogicalView(
        CreateLogicalViewNode node, WritePlanNodeExecutionContext context) {
      ISchemaRegion schemaRegion =
          schemaEngine.getSchemaRegion((SchemaRegionId) context.getRegionId());
      if (CONFIG.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          // step 1. make sure all target paths are NOT exist.
          List<PartialPath> targetPaths = node.getViewPathList();
          List<MetadataException> failingMetadataException = new ArrayList<>();
          for (PartialPath thisPath : targetPaths) {
            // no alias list for a view, so the third parameter is null
            Map<Integer, MetadataException> failingMeasurementMap =
                schemaRegion.checkMeasurementExistence(
                    thisPath.getDevicePath(),
                    Collections.singletonList(thisPath.getMeasurement()),
                    null);
            // merge all exception into one map
            for (Map.Entry<Integer, MetadataException> entry : failingMeasurementMap.entrySet()) {
              failingMetadataException.add(entry.getValue());
            }
          }
          // if there is some exception, handle each exception and return first of them.
          if (!failingMetadataException.isEmpty()) {
            MetadataException metadataException = failingMetadataException.get(0);
            LOGGER.error(METADATA_ERROR_MSG, metadataException);
            RegionExecutionResult result = new RegionExecutionResult();
            result.setAccepted(false);
            result.setMessage(metadataException.getMessage());
            result.setStatus(
                RpcUtils.getStatus(
                    metadataException.getErrorCode(), metadataException.getMessage()));
            return result;
          }
          // step 2. make sure all source paths are existed.
          return super.visitCreateLogicalView(node, context);
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return super.visitCreateLogicalView(node, context);
      }
      // end of visitCreateLogicalView
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
