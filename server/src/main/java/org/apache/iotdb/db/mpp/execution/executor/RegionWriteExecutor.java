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

package org.apache.iotdb.db.mpp.execution.executor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.metric.PerformanceOverviewMetricsManager;
import org.apache.iotdb.db.mpp.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.service.thrift.impl.DataNodeRegionManager;
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

  private static final DataNodeRegionManager REGION_MANAGER = DataNodeRegionManager.getInstance();

  public RegionExecutionResult execute(ConsensusGroupId groupId, PlanNode planNode) {
    try {
      WritePlanNodeExecutionContext context =
          new WritePlanNodeExecutionContext(groupId, REGION_MANAGER.getRegionLock(groupId));
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

  public static ConsensusWriteResponse fireTriggerAndInsert(
      ConsensusGroupId groupId, PlanNode planNode) {
    long triggerCostTime = 0;
    ConsensusWriteResponse writeResponse;
    TriggerFireVisitor visitor = new TriggerFireVisitor();
    long startTime = System.nanoTime();
    // fire Trigger before the insertion
    TriggerFireResult result = visitor.process(planNode, TriggerEvent.BEFORE_INSERT);
    triggerCostTime += (System.nanoTime() - startTime);
    if (result.equals(TriggerFireResult.TERMINATION)) {
      TSStatus triggerError = new TSStatus(TSStatusCode.TRIGGER_FIRE_ERROR.getStatusCode());
      triggerError.setMessage(
          "Failed to complete the insertion because trigger error before the insertion.");
      writeResponse = ConsensusWriteResponse.newBuilder().setStatus(triggerError).build();
    } else {
      boolean hasFailedTriggerBeforeInsertion =
          result.equals(TriggerFireResult.FAILED_NO_TERMINATION);

      long startWriteTime = System.nanoTime();
      writeResponse = DataRegionConsensusImpl.getInstance().write(groupId, planNode);
      PerformanceOverviewMetricsManager.recordScheduleStorageCost(
          System.nanoTime() - startWriteTime);

      // fire Trigger after the insertion
      if (writeResponse.isSuccessful()) {
        startTime = System.nanoTime();
        result = visitor.process(planNode, TriggerEvent.AFTER_INSERT);
        if (hasFailedTriggerBeforeInsertion || !result.equals(TriggerFireResult.SUCCESS)) {
          TSStatus triggerError = new TSStatus(TSStatusCode.TRIGGER_FIRE_ERROR.getStatusCode());
          triggerError.setMessage(
              "Meet trigger error before/after the insertion, the insertion itself is completed.");
          writeResponse = ConsensusWriteResponse.newBuilder().setStatus(triggerError).build();
        }
        triggerCostTime += (System.nanoTime() - startTime);
      }
    }
    PerformanceOverviewMetricsManager.recordScheduleTriggerCost(triggerCostTime);
    return writeResponse;
  }

  private static class WritePlanNodeExecutionVisitor
      extends PlanVisitor<RegionExecutionResult, WritePlanNodeExecutionContext> {

    private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    @Override
    public RegionExecutionResult visitPlan(PlanNode node, WritePlanNodeExecutionContext context) {
      RegionExecutionResult response = new RegionExecutionResult();

      ConsensusWriteResponse writeResponse =
          executePlanNodeInConsensusLayer(context.getRegionId(), node);
      // TODO need consider more status
      if (writeResponse.getStatus() != null) {
        response.setAccepted(
            TSStatusCode.SUCCESS_STATUS.getStatusCode() == writeResponse.getStatus().getCode());
        response.setMessage(writeResponse.getStatus().message);
        response.setStatus(writeResponse.getStatus());
      } else {
        LOGGER.error(
            "Something wrong happened while calling consensus layer's write API.",
            writeResponse.getException());
        response.setAccepted(false);
        response.setMessage(writeResponse.getException().toString());
        response.setStatus(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR));
      }
      return response;
    }

    private ConsensusWriteResponse executePlanNodeInConsensusLayer(
        ConsensusGroupId groupId, PlanNode planNode) {
      if (groupId instanceof DataRegionId) {
        return DataRegionConsensusImpl.getInstance().write(groupId, planNode);
      } else {
        return SchemaRegionConsensusImpl.getInstance().write(groupId, planNode);
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

    private RegionExecutionResult executeDataInsert(
        InsertNode insertNode, WritePlanNodeExecutionContext context) {
      RegionExecutionResult response = new RegionExecutionResult();
      // data insertion should be blocked by data deletion, especially when deleting timeseries
      final long startTime = System.nanoTime();
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        try {
          SchemaValidator.validate(insertNode);
        } catch (SemanticException e) {
          response.setAccepted(false);
          response.setMessage(e.getMessage());
          if (e.getCause() instanceof IoTDBException) {
            IoTDBException ioTDBException = (IoTDBException) e.getCause();
            response.setStatus(
                RpcUtils.getStatus(ioTDBException.getErrorCode(), ioTDBException.getMessage()));
          } else {
            response.setStatus(RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage()));
          }
          return response;
        } finally {
          PerformanceOverviewMetricsManager.recordScheduleSchemaValidateCost(
              System.nanoTime() - startTime);
        }
        boolean hasFailedMeasurement = insertNode.hasFailedMeasurements();
        String partialInsertMessage = null;
        if (hasFailedMeasurement) {
          partialInsertMessage =
              String.format(
                  "Fail to insert measurements %s caused by %s",
                  insertNode.getFailedMeasurements(), insertNode.getFailedMessages());
          LOGGER.warn(partialInsertMessage);
        }

        ConsensusWriteResponse writeResponse =
            fireTriggerAndInsert(context.getRegionId(), insertNode);

        // TODO need consider more status
        if (writeResponse.getStatus() != null) {
          response.setAccepted(
              !hasFailedMeasurement
                  && TSStatusCode.SUCCESS_STATUS.getStatusCode()
                      == writeResponse.getStatus().getCode());
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != writeResponse.getStatus().getCode()) {
            response.setMessage(writeResponse.getStatus().message);
            response.setStatus(writeResponse.getStatus());
          } else if (hasFailedMeasurement) {
            response.setMessage(partialInsertMessage);
            response.setStatus(
                RpcUtils.getStatus(
                    TSStatusCode.METADATA_ERROR.getStatusCode(), partialInsertMessage));
          } else {
            response.setMessage(writeResponse.getStatus().message);
          }
        } else {
          LOGGER.warn(
              "Something wrong happened while calling consensus layer's write API.",
              writeResponse.getException());
          response.setAccepted(false);
          response.setMessage(writeResponse.getException().toString());
          response.setStatus(
              RpcUtils.getStatus(
                  TSStatusCode.WRITE_PROCESS_ERROR, writeResponse.getException().toString()));
        }

        return response;
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
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
          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) context.getRegionId());
      if (config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
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
            LOGGER.error("Metadata error: ", metadataException);
            RegionExecutionResult result = new RegionExecutionResult();
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
          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) context.getRegionId());
      if (config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          Map<Integer, MetadataException> failingMeasurementMap =
              schemaRegion.checkMeasurementExistence(
                  node.getDevicePath(), node.getMeasurements(), node.getAliasList());
          if (failingMeasurementMap.isEmpty()) {
            return super.visitCreateAlignedTimeSeries(node, context);
          } else {
            MetadataException metadataException = failingMeasurementMap.values().iterator().next();
            LOGGER.error("Metadata error: ", metadataException);
            RegionExecutionResult result = new RegionExecutionResult();
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
          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) context.getRegionId());
      if (config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
        context.getRegionWriteValidationRWLock().writeLock().lock();
        try {
          List<TSStatus> failingStatus = new ArrayList<>();
          Map<PartialPath, MeasurementGroup> measurementGroupMap = node.getMeasurementGroupMap();
          List<PartialPath> emptyDeviceList = new ArrayList<>();
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
              LOGGER.error("Metadata error: ", failingMeasurement.getValue());
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

          for (PartialPath emptyDevice : emptyDeviceList) {
            measurementGroupMap.remove(emptyDevice);
          }

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

          TSStatus status = RpcUtils.getStatus(failingStatus);
          RegionExecutionResult failingResult = new RegionExecutionResult();
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

    @Override
    public RegionExecutionResult visitInternalCreateTimeSeries(
        InternalCreateTimeSeriesNode node, WritePlanNodeExecutionContext context) {
      ISchemaRegion schemaRegion =
          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) context.getRegionId());
      if (config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
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
              LOGGER.warn("Metadata error: ", metadataException);
              failingStatus.add(
                  RpcUtils.getStatus(
                      metadataException.getErrorCode(), metadataException.getMessage()));
            }
          }
          measurementGroup.removeMeasurements(failingMeasurementMap.keySet());

          RegionExecutionResult executionResult =
              super.visitInternalCreateTimeSeries(node, context);

          if (failingStatus.isEmpty() && alreadyExistingStatus.isEmpty()) {
            return executionResult;
          }

          TSStatus executionStatus = executionResult.getStatus();

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

          RegionExecutionResult result = new RegionExecutionResult();
          TSStatus status;
          if (failingStatus.isEmpty()) {
            status = RpcUtils.getStatus(alreadyExistingStatus);
            result.setAccepted(true);
          } else {
            status = RpcUtils.getStatus(failingStatus);
            result.setAccepted(false);
          }

          result.setMessage(status.getMessage());
          result.setStatus(status);
          return result;
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
          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) context.getRegionId());
      if (config.getSchemaRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
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
                LOGGER.warn("Metadata error: ", metadataException);
                failingStatus.add(
                    RpcUtils.getStatus(
                        metadataException.getErrorCode(), metadataException.getMessage()));
              }
            }
            measurementGroup.removeMeasurements(failingMeasurementMap.keySet());
          }

          RegionExecutionResult executionResult =
              super.visitInternalCreateMultiTimeSeries(node, context);

          if (failingStatus.isEmpty() && alreadyExistingStatus.isEmpty()) {
            return executionResult;
          }

          TSStatus executionStatus = executionResult.getStatus();

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

          RegionExecutionResult result = new RegionExecutionResult();
          TSStatus status;
          if (failingStatus.isEmpty()) {
            status = RpcUtils.getStatus(alreadyExistingStatus);
            result.setAccepted(true);
          } else {
            status = RpcUtils.getStatus(failingStatus);
            result.setAccepted(false);
          }

          result.setMessage(status.getMessage());
          result.setStatus(status);
          return result;
        } finally {
          context.getRegionWriteValidationRWLock().writeLock().unlock();
        }
      } else {
        return super.visitInternalCreateMultiTimeSeries(node, context);
      }
    }

    @Override
    public RegionExecutionResult visitActivateTemplate(
        ActivateTemplateNode node, WritePlanNodeExecutionContext context) {
      // activate template operation shall be blocked by unset template check
      context.getRegionWriteValidationRWLock().readLock().lock();
      try {
        Pair<Template, PartialPath> templateSetInfo =
            ClusterTemplateManager.getInstance().checkTemplateSetInfo(node.getActivatePath());
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
        return super.visitActivateTemplate(node, context);
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
        for (Map.Entry<PartialPath, Pair<Integer, Integer>> entry :
            node.getTemplateActivationMap().entrySet()) {
          Pair<Template, PartialPath> templateSetInfo =
              ClusterTemplateManager.getInstance().checkTemplateSetInfo(entry.getKey());
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
        }

        return super.visitInternalBatchActivateTemplate(node, context);
      } finally {
        context.getRegionWriteValidationRWLock().readLock().unlock();
      }
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
