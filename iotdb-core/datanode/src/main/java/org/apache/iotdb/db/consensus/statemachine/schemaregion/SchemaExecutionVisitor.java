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

package org.apache.iotdb.db.consensus.statemachine.schemaregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.PreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.RollbackPreDeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.RollbackSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.ConstructLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.DeleteLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.RollbackLogicalViewBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedNonWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateAlignedTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Schema write {@link PlanNode} visitor */
public class SchemaExecutionVisitor extends PlanVisitor<TSStatus, ISchemaRegion> {
  private static final Logger logger = LoggerFactory.getLogger(SchemaExecutionVisitor.class);

  @Override
  public TSStatus visitCreateTimeSeries(CreateTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.createTimeSeries(node, -1);
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitCreateAlignedTimeSeries(
      CreateAlignedTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.createAlignedTimeSeries(node);
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesNode node, ISchemaRegion schemaRegion) {
    Map<PartialPath, MeasurementGroup> measurementGroupMap = node.getMeasurementGroupMap();
    List<TSStatus> failingStatus = new ArrayList<>();
    PartialPath devicePath;
    MeasurementGroup measurementGroup;
    int size;
    for (Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
      devicePath = entry.getKey();
      measurementGroup = entry.getValue();
      size = measurementGroup.getMeasurements().size();
      // todo implement batch creation of one device in SchemaRegion
      for (int i = 0; i < size; i++) {
        try {
          schemaRegion.createTimeSeries(
              transformToCreateTimeSeriesPlan(devicePath, measurementGroup, i), -1);
        } catch (MetadataException e) {
          logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
          failingStatus.add(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        }
      }
    }

    if (!failingStatus.isEmpty()) {
      return RpcUtils.getStatus(failingStatus);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  private ICreateTimeSeriesPlan transformToCreateTimeSeriesPlan(
      PartialPath devicePath, MeasurementGroup measurementGroup, int index) {
    return SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
        devicePath.concatNode(measurementGroup.getMeasurements().get(index)),
        measurementGroup.getDataTypes().get(index),
        measurementGroup.getEncodings().get(index),
        measurementGroup.getCompressors().get(index),
        measurementGroup.getPropsList() == null ? null : measurementGroup.getPropsList().get(index),
        measurementGroup.getTagsList() == null ? null : measurementGroup.getTagsList().get(index),
        measurementGroup.getAttributesList() == null
            ? null
            : measurementGroup.getAttributesList().get(index),
        measurementGroup.getAliasList() == null
            ? null
            : measurementGroup.getAliasList().get(index));
  }

  @Override
  public TSStatus visitInternalCreateTimeSeries(
      final InternalCreateTimeSeriesNode node, final ISchemaRegion schemaRegion) {
    final PartialPath devicePath = node.getDevicePath();
    final MeasurementGroup measurementGroup = node.getMeasurementGroup();

    final List<TSStatus> alreadyExistingTimeSeries = new ArrayList<>();
    final List<TSStatus> failingStatus = new ArrayList<>();

    if (node.isAligned()) {
      executeInternalCreateAlignedTimeSeries(
          devicePath,
          measurementGroup,
          schemaRegion,
          alreadyExistingTimeSeries,
          failingStatus,
          false);
    } else {
      executeInternalCreateTimeSeries(
          devicePath,
          measurementGroup,
          schemaRegion,
          alreadyExistingTimeSeries,
          failingStatus,
          false);
    }

    if (!failingStatus.isEmpty()) {
      return RpcUtils.getStatus(failingStatus);
    }

    if (!alreadyExistingTimeSeries.isEmpty()) {
      return RpcUtils.getStatus(alreadyExistingTimeSeries);
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesNode node, final ISchemaRegion schemaRegion) {
    PartialPath devicePath;
    MeasurementGroup measurementGroup;

    final List<TSStatus> alreadyExistingTimeSeries = new ArrayList<>();
    final List<TSStatus> failingStatus = new ArrayList<>();

    for (final Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> deviceEntry :
        node.getDeviceMap().entrySet()) {
      devicePath = deviceEntry.getKey();
      measurementGroup = deviceEntry.getValue().right;
      if (Boolean.TRUE.equals(deviceEntry.getValue().left)) {
        executeInternalCreateAlignedTimeSeries(
            devicePath,
            measurementGroup,
            schemaRegion,
            alreadyExistingTimeSeries,
            failingStatus,
            node.isGeneratedByPipe());
      } else {
        executeInternalCreateTimeSeries(
            devicePath,
            measurementGroup,
            schemaRegion,
            alreadyExistingTimeSeries,
            failingStatus,
            node.isGeneratedByPipe());
      }
    }

    if (!failingStatus.isEmpty()) {
      return RpcUtils.getStatus(failingStatus);
    }

    if (!alreadyExistingTimeSeries.isEmpty()) {
      return RpcUtils.getStatus(alreadyExistingTimeSeries);
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  private void executeInternalCreateTimeSeries(
      final PartialPath devicePath,
      final MeasurementGroup measurementGroup,
      final ISchemaRegion schemaRegion,
      final List<TSStatus> alreadyExistingTimeSeries,
      final List<TSStatus> failingStatus,
      final boolean withMerge) {
    final int size = measurementGroup.getMeasurements().size();
    // todo implement batch creation of one device in SchemaRegion
    for (int i = 0; i < size; i++) {
      try {
        final ICreateTimeSeriesPlan createTimeSeriesPlan =
            transformToCreateTimeSeriesPlan(devicePath, measurementGroup, i);
        // With merge is only true for pipe to upsert the receiver alias/tags/attributes in
        // historical transfer.
        // For normal internal creation, the alias/tags/attributes are not set
        // Thus the original ones are not altered
        ((CreateTimeSeriesPlanImpl) createTimeSeriesPlan).setWithMerge(withMerge);
        schemaRegion.createTimeSeries(createTimeSeriesPlan, -1);
      } catch (final MeasurementAlreadyExistException e) {
        // There's no need to internal create time series.
        alreadyExistingTimeSeries.add(
            RpcUtils.getStatus(
                e.getErrorCode(), MeasurementPath.transformDataToString(e.getMeasurementPath())));
      } catch (final MetadataException e) {
        logger.warn("{}: MetaData error: ", e.getMessage(), e);
        failingStatus.add(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }
  }

  private void executeInternalCreateAlignedTimeSeries(
      final PartialPath devicePath,
      final MeasurementGroup measurementGroup,
      final ISchemaRegion schemaRegion,
      final List<TSStatus> alreadyExistingTimeSeries,
      final List<TSStatus> failingStatus,
      final boolean withMerge) {
    final List<String> measurementList = measurementGroup.getMeasurements();
    final List<TSDataType> dataTypeList = measurementGroup.getDataTypes();
    final List<TSEncoding> encodingList = measurementGroup.getEncodings();
    final List<CompressionType> compressionTypeList = measurementGroup.getCompressors();
    final ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            devicePath,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypeList,
            null,
            null,
            null);
    // With merge is only true for pipe to upsert the receiver alias/tags/attributes in historical
    // transfer.
    // For normal internal creation, the alias/tags/attributes are not set
    // Thus the original ones are not altered
    ((CreateAlignedTimeSeriesPlanImpl) createAlignedTimeSeriesPlan).setWithMerge(withMerge);

    boolean shouldRetry = true;
    while (shouldRetry) {
      try {
        schemaRegion.createAlignedTimeSeries(createAlignedTimeSeriesPlan);
        shouldRetry = false;
      } catch (final MeasurementAlreadyExistException e) {
        // The existence check will be executed before truly creation
        // There's no need to internal create time series.
        final MeasurementPath measurementPath = e.getMeasurementPath();
        alreadyExistingTimeSeries.add(
            RpcUtils.getStatus(
                e.getErrorCode(), MeasurementPath.transformDataToString(e.getMeasurementPath())));

        // remove the existing time series from plan
        final int index = measurementList.indexOf(measurementPath.getMeasurement());
        measurementList.remove(index);
        dataTypeList.remove(index);
        encodingList.remove(index);
        compressionTypeList.remove(index);

        if (measurementList.isEmpty()) {
          shouldRetry = false;
        }

      } catch (final MetadataException e) {
        logger.warn("{}: MetaData error: ", e.getMessage(), e);
        failingStatus.add(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        shouldRetry = false;
      }
    }
  }

  @Override
  public TSStatus visitAlterTimeSeries(AlterTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      switch (node.getAlterType()) {
        case RENAME:
          String beforeName = node.getAlterMap().keySet().iterator().next();
          String currentName = node.getAlterMap().get(beforeName);
          schemaRegion.renameTagOrAttributeKey(beforeName, currentName, node.getPath());
          break;
        case SET:
          schemaRegion.setTagsOrAttributesValue(node.getAlterMap(), node.getPath());
          break;
        case DROP:
          schemaRegion.dropTagsOrAttributes(node.getAlterMap().keySet(), node.getPath());
          break;
        case ADD_TAGS:
          schemaRegion.addTags(node.getAlterMap(), node.getPath());
          break;
        case ADD_ATTRIBUTES:
          schemaRegion.addAttributes(node.getAlterMap(), node.getPath());
          break;
        case UPSERT:
          schemaRegion.upsertAliasAndTagsAndAttributes(
              node.getAlias(), node.getTagsMap(), node.getAttributesMap(), node.getPath());
          break;
      }
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (IOException e) {
      logger.error("{}: IO error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitActivateTemplate(ActivateTemplateNode node, ISchemaRegion schemaRegion) {
    try {
      Template template = ClusterTemplateManager.getInstance().getTemplate(node.getTemplateId());
      schemaRegion.activateSchemaTemplate(node, template);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      BatchActivateTemplateNode node, ISchemaRegion schemaRegion) {
    for (Map.Entry<PartialPath, Pair<Integer, Integer>> entry :
        node.getTemplateActivationMap().entrySet()) {
      Template template = ClusterTemplateManager.getInstance().getTemplate(entry.getValue().left);
      try {
        schemaRegion.activateSchemaTemplate(
            SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
                entry.getKey(), entry.getValue().right, entry.getValue().left),
            template);
      } catch (MetadataException e) {
        logger.error(e.getMessage(), e);
        return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
      }
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateNode node, ISchemaRegion schemaRegion) {
    for (Map.Entry<PartialPath, Pair<Integer, Integer>> entry :
        node.getTemplateActivationMap().entrySet()) {
      Template template = ClusterTemplateManager.getInstance().getTemplate(entry.getValue().left);
      try {
        schemaRegion.activateSchemaTemplate(
            SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
                entry.getKey(), entry.getValue().right, entry.getValue().left),
            template);
      } catch (TemplateIsInUseException e) {
        logger.info(
            String.format(
                "Device Template has already been activated on path %s, there's no need to activate again.",
                entry.getKey()));
      } catch (MetadataException e) {
        logger.error(e.getMessage(), e);
        return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
      }
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus visitConstructSchemaBlackList(
      final ConstructSchemaBlackListNode node, final ISchemaRegion schemaRegion) {
    try {
      final Pair<Long, Boolean> preDeletedNumAndIsAllLogicalView =
          schemaRegion.constructSchemaBlackList(node.getPatternTree());
      return RpcUtils.getStatus(
          Boolean.TRUE.equals(preDeletedNumAndIsAllLogicalView.getRight())
              ? TSStatusCode.ONLY_LOGICAL_VIEW
              : TSStatusCode.SUCCESS_STATUS,
          String.valueOf(preDeletedNumAndIsAllLogicalView.getLeft()));
    } catch (final MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitRollbackSchemaBlackList(
      RollbackSchemaBlackListNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.rollbackSchemaBlackList(node.getPatternTree());
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitDeleteTimeseries(DeleteTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.deleteTimeseriesInBlackList(node.getPatternTree());
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitPreDeactivateTemplate(
      PreDeactivateTemplateNode node, ISchemaRegion schemaRegion) {
    try {
      long preDeactivateNum = schemaRegion.constructSchemaBlackListWithTemplate(node);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, String.valueOf(preDeactivateNum));
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitRollbackPreDeactivateTemplate(
      RollbackPreDeactivateTemplateNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.rollbackSchemaBlackListWithTemplate(node);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitDeactivateTemplate(DeactivateTemplateNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.deactivateTemplateInBlackList(node);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitCreateLogicalView(CreateLogicalViewNode node, ISchemaRegion schemaRegion) {
    Map<PartialPath, ViewExpression> viewPathToSourceMap = node.getViewPathToSourceExpressionMap();
    List<TSStatus> failingStatus = new ArrayList<>();
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      try {
        schemaRegion.createLogicalView(
            SchemaRegionWritePlanFactory.getCreateLogicalViewPlan(
                entry.getKey(), entry.getValue()));
      } catch (MetadataException e) {
        logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
        failingStatus.add(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }
    if (!failingStatus.isEmpty()) {
      return RpcUtils.getStatus(failingStatus);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitAlterLogicalView(AlterLogicalViewNode node, ISchemaRegion schemaRegion) {
    Map<PartialPath, ViewExpression> viewPathToSourceMap = node.getViewPathToSourceMap();
    List<TSStatus> failingStatus = new ArrayList<>();
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      try {
        schemaRegion.alterLogicalView(
            SchemaRegionWritePlanFactory.getAlterLogicalViewPlan(entry.getKey(), entry.getValue()));
      } catch (MetadataException e) {
        logger.warn("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
        failingStatus.add(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }
    if (!failingStatus.isEmpty()) {
      return RpcUtils.getStatus(failingStatus);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitConstructLogicalViewBlackList(
      ConstructLogicalViewBlackListNode node, ISchemaRegion schemaRegion) {
    try {
      long preDeletedNum = schemaRegion.constructLogicalViewBlackList(node.getPatternTree());
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, String.valueOf(preDeletedNum));
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitRollbackLogicalViewBlackList(
      RollbackLogicalViewBlackListNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.rollbackLogicalViewBlackList(node.getPatternTree());
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitDeleteLogicalView(DeleteLogicalViewNode node, ISchemaRegion schemaRegion) {
    try {
      schemaRegion.deleteLogicalView(node.getPatternTree());
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (MetadataException e) {
      logger.error(e.getMessage(), e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    }
  }

  @Override
  public TSStatus visitPipeEnrichedWritePlanNode(
      final PipeEnrichedWritePlanNode node, final ISchemaRegion schemaRegion) {
    final WritePlanNode innerNode = node.getWritePlanNode();
    innerNode.markAsGeneratedByPipe();
    return innerNode.accept(this, schemaRegion);
  }

  @Override
  public TSStatus visitPipeEnrichedNonWritePlanNode(
      final PipeEnrichedNonWritePlanNode node, final ISchemaRegion schemaRegion) {
    final PlanNode innerNode = node.getNonWritePlanNode();
    innerNode.markAsGeneratedByPipe();
    return node.getNonWritePlanNode().accept(this, schemaRegion);
  }

  @Override
  public TSStatus visitPipeOperateSchemaQueueNode(
      final PipeOperateSchemaQueueNode node, final ISchemaRegion schemaRegion) {
    final SchemaRegionId id = schemaRegion.getSchemaRegionId();
    final SchemaRegionListeningQueue queue = PipeDataNodeAgent.runtime().schemaListener(id);
    if (node.isOpen() && !queue.isOpened()) {
      logger.info("Opened pipe listening queue on schema region {}", id);
      queue.open();
    } else if (!node.isOpen() && queue.isOpened()) {
      logger.info("Closed pipe listening queue on schema region {}", id);
      queue.close();
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitPlan(PlanNode node, ISchemaRegion context) {
    return null;
  }
}
