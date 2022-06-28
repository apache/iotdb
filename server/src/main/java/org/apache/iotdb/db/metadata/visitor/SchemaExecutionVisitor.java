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

package org.apache.iotdb.db.metadata.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Schema write PlanNode visitor */
public class SchemaExecutionVisitor extends PlanVisitor<TSStatus, ISchemaRegion> {
  private static final Logger logger = LoggerFactory.getLogger(SchemaExecutionVisitor.class);

  @Override
  public TSStatus visitCreateTimeSeries(CreateTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      PhysicalPlan plan = node.accept(new PhysicalPlanTransformer(), new TransformerContext());
      schemaRegion.createTimeseries((CreateTimeSeriesPlan) plan, -1);
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitCreateAlignedTimeSeries(
      CreateAlignedTimeSeriesNode node, ISchemaRegion schemaRegion) {
    try {
      PhysicalPlan plan = node.accept(new PhysicalPlanTransformer(), new TransformerContext());
      schemaRegion.createAlignedTimeSeries((CreateAlignedTimeSeriesPlan) plan);
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
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
          schemaRegion.createTimeseries(
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

  private CreateTimeSeriesPlan transformToCreateTimeSeriesPlan(
      PartialPath devicePath, MeasurementGroup measurementGroup, int index) {
    return new CreateTimeSeriesPlan(
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
      InternalCreateTimeSeriesNode node, ISchemaRegion schemaRegion) {
    PartialPath devicePath = node.getDevicePath();
    MeasurementGroup measurementGroup = node.getMeasurementGroup();

    List<TSStatus> alreadyExistingTimeseries = new ArrayList<>();
    List<TSStatus> failingStatus = new ArrayList<>();

    if (node.isAligned()) {
      executeInternalCreateAlignedTimeseries(
          devicePath, measurementGroup, schemaRegion, alreadyExistingTimeseries, failingStatus);
    } else {
      executeInternalCreateTimeseries(
          devicePath, measurementGroup, schemaRegion, alreadyExistingTimeseries, failingStatus);
    }

    if (!failingStatus.isEmpty()) {
      return RpcUtils.getStatus(failingStatus);
    }

    if (!alreadyExistingTimeseries.isEmpty()) {
      return RpcUtils.getStatus(alreadyExistingTimeseries);
    }

    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  private void executeInternalCreateTimeseries(
      PartialPath devicePath,
      MeasurementGroup measurementGroup,
      ISchemaRegion schemaRegion,
      List<TSStatus> alreadyExistingTimeseries,
      List<TSStatus> failingStatus) {

    int size = measurementGroup.getMeasurements().size();
    // todo implement batch creation of one device in SchemaRegion
    for (int i = 0; i < size; i++) {
      try {
        schemaRegion.createTimeseries(
            transformToCreateTimeSeriesPlan(devicePath, measurementGroup, i), -1);
      } catch (MeasurementAlreadyExistException e) {
        logger.info("There's no need to internal create timeseries. {}", e.getMessage());
        alreadyExistingTimeseries.add(
            RpcUtils.getStatus(
                e.getErrorCode(), transformExistingTimeseriesToString(e.getMeasurementPath())));
      } catch (MetadataException e) {
        logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
        failingStatus.add(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }
  }

  private String transformExistingTimeseriesToString(MeasurementPath measurementPath) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      measurementPath.serialize(dataOutputStream);
    } catch (IOException ignored) {
      // this exception won't happen.
    }
    return byteArrayOutputStream.toString();
  }

  private void executeInternalCreateAlignedTimeseries(
      PartialPath devicePath,
      MeasurementGroup measurementGroup,
      ISchemaRegion schemaRegion,
      List<TSStatus> alreadyExistingTimeseries,
      List<TSStatus> failingStatus) {
    List<String> measurementList = measurementGroup.getMeasurements();
    List<TSDataType> dataTypeList = measurementGroup.getDataTypes();
    List<TSEncoding> encodingList = measurementGroup.getEncodings();
    List<CompressionType> compressionTypeList = measurementGroup.getCompressors();
    CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
        new CreateAlignedTimeSeriesPlan(
            devicePath,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypeList,
            null,
            null,
            null);

    boolean shouldRetry = true;
    while (shouldRetry) {
      try {
        schemaRegion.createAlignedTimeSeries(createAlignedTimeSeriesPlan);
        shouldRetry = false;
      } catch (MeasurementAlreadyExistException e) {
        // the existence check will be executed before truly creation
        logger.info("There's no need to internal create timeseries. {}", e.getMessage());
        MeasurementPath measurementPath = e.getMeasurementPath();
        alreadyExistingTimeseries.add(
            RpcUtils.getStatus(
                e.getErrorCode(), transformExistingTimeseriesToString(measurementPath)));

        // remove the existing timeseries from plan
        int index = measurementList.indexOf(measurementPath.getMeasurement());
        measurementList.remove(index);
        dataTypeList.remove(index);
        encodingList.remove(index);
        compressionTypeList.remove(index);

        if (measurementList.isEmpty()) {
          shouldRetry = false;
        }

      } catch (MetadataException e) {
        logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
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
          schemaRegion.upsertTagsAndAttributes(
              node.getAlias(), node.getTagsMap(), node.getAttributesMap(), node.getPath());
          break;
      }
    } catch (MetadataException e) {
      logger.error("{}: MetaData error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    } catch (IOException e) {
      logger.error("{}: IO error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully");
  }

  @Override
  public TSStatus visitPlan(PlanNode node, ISchemaRegion context) {
    return null;
  }

  // TODO need remove
  private static class PhysicalPlanTransformer
      extends PlanVisitor<PhysicalPlan, TransformerContext> {
    @Override
    public PhysicalPlan visitPlan(PlanNode node, TransformerContext context) {
      throw new NotImplementedException();
    }

    public PhysicalPlan visitCreateTimeSeries(
        CreateTimeSeriesNode node, TransformerContext context) {
      return new CreateTimeSeriesPlan(
          node.getPath(),
          node.getDataType(),
          node.getEncoding(),
          node.getCompressor(),
          node.getProps(),
          node.getTags(),
          node.getAttributes(),
          node.getAlias());
    }

    public PhysicalPlan visitCreateAlignedTimeSeries(
        CreateAlignedTimeSeriesNode node, TransformerContext context) {
      return new CreateAlignedTimeSeriesPlan(
          node.getDevicePath(),
          node.getMeasurements(),
          node.getDataTypes(),
          node.getEncodings(),
          node.getCompressors(),
          node.getAliasList(),
          node.getTagsList(),
          node.getAttributesList());
    }
  }

  private static class TransformerContext {}
}
