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

package org.apache.iotdb.db.schemaengine.schemaregion.logfile.visitor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.IDeserializer;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IAutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IChangeAliasPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IChangeTagOffsetPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IDeleteTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IRollbackPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IAlterLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.ICreateLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IDeleteLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IPreDeleteLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IRollbackPreDeleteLogicalViewPlan;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides the ability to deserialize a SchemaRegionPlan from binary data in given
 * ByteBuffer, the format of which is compatible with that of PhysicalPlan in 0.14 snapshot
 * versions.
 */
public class SchemaRegionPlanDeserializer implements IDeserializer<ISchemaRegionPlan> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegionPlanDeserializer.class);

  @Override
  public ISchemaRegionPlan deserialize(final ByteBuffer byteBuffer) {
    final ISchemaRegionPlan schemaRegionPlan =
        SchemaRegionWritePlanFactory.getEmptyPlan(SchemaRegionPlanType.deserialize(byteBuffer));
    return schemaRegionPlan.accept(new SchemaRegionPlanDeserializeVisitor(), byteBuffer);
  }

  private static class SchemaRegionPlanDeserializeVisitor
      extends SchemaRegionPlanVisitor<ISchemaRegionPlan, ByteBuffer> {

    @Override
    public ISchemaRegionPlan visitSchemaRegionPlan(
        final ISchemaRegionPlan plan, final ByteBuffer byteBuffer) {
      throw new UnsupportedOperationException(
          String.format("%s plan doesn't support deserialization.", plan.getPlanType().name()));
    }

    @Override
    public ISchemaRegionPlan visitActivateTemplateInCluster(
        final IActivateTemplateInClusterPlan activateTemplateInClusterPlan,
        final ByteBuffer buffer) {
      try {
        activateTemplateInClusterPlan.setActivatePath(
            new PartialPath(ReadWriteIOUtils.readString(buffer)));
      } catch (IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }
      activateTemplateInClusterPlan.setTemplateSetLevel(ReadWriteIOUtils.readInt(buffer));
      activateTemplateInClusterPlan.setTemplateId(ReadWriteIOUtils.readInt(buffer));
      activateTemplateInClusterPlan.setAligned(ReadWriteIOUtils.readBool(buffer));

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();
      return activateTemplateInClusterPlan;
    }

    @Override
    public ISchemaRegionPlan visitAutoCreateDeviceMNode(
        final IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, final ByteBuffer buffer) {
      try {
        autoCreateDeviceMNodePlan.setPath(new PartialPath(ReadWriteIOUtils.readString(buffer)));
      } catch (IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();
      return autoCreateDeviceMNodePlan;
    }

    @Override
    public ISchemaRegionPlan visitChangeAlias(
        final IChangeAliasPlan changeAliasPlan, final ByteBuffer buffer) {
      try {
        changeAliasPlan.setPath(new PartialPath(ReadWriteIOUtils.readString(buffer)));
      } catch (final IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }
      changeAliasPlan.setAlias(ReadWriteIOUtils.readString(buffer));
      return changeAliasPlan;
    }

    @Override
    public ISchemaRegionPlan visitChangeTagOffset(
        final IChangeTagOffsetPlan changeTagOffsetPlan, final ByteBuffer buffer) {
      try {
        changeTagOffsetPlan.setPath(new PartialPath(ReadWriteIOUtils.readString(buffer)));
      } catch (final IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }
      changeTagOffsetPlan.setOffset(buffer.getLong());
      return changeTagOffsetPlan;
    }

    @Override
    public ISchemaRegionPlan visitCreateAlignedTimeSeries(
        final ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan, final ByteBuffer buffer) {
      // deserialize a version mark to adapt to old version
      buffer.getInt();

      final int length = buffer.getInt();
      final byte[] bytes = new byte[length];
      buffer.get(bytes);
      try {
        createAlignedTimeSeriesPlan.setDevicePath(
            new PartialPath(new String(bytes, TSFileConfig.STRING_CHARSET)));
      } catch (final IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }

      final int size = ReadWriteIOUtils.readInt(buffer);
      final List<String> measurements = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        measurements.add(ReadWriteIOUtils.readString(buffer));
      }
      createAlignedTimeSeriesPlan.setMeasurements(measurements);

      final List<TSDataType> dataTypes = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        dataTypes.add(TSDataType.deserialize(buffer.get()));
      }
      createAlignedTimeSeriesPlan.setDataTypes(dataTypes);

      final List<TSEncoding> encodings = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        encodings.add(TSEncoding.deserialize(buffer.get()));
      }
      createAlignedTimeSeriesPlan.setEncodings(encodings);

      final List<CompressionType> compressors = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        compressors.add(CompressionType.deserialize(buffer.get()));
      }
      createAlignedTimeSeriesPlan.setCompressors(compressors);

      final List<Long> tagOffsets = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        tagOffsets.add(buffer.getLong());
      }
      createAlignedTimeSeriesPlan.setTagOffsets(tagOffsets);

      // alias
      if (buffer.get() == 1) {
        List<String> aliasList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
          aliasList.add(ReadWriteIOUtils.readString(buffer));
        }
        createAlignedTimeSeriesPlan.setAliasList(aliasList);
      }

      // tags
      if (buffer.get() == 1) {
        List<Map<String, String>> tagsList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
          tagsList.add(ReadWriteIOUtils.readMap(buffer));
        }
        createAlignedTimeSeriesPlan.setTagsList(tagsList);
      }

      // attributes
      if (buffer.get() == 1) {
        List<Map<String, String>> attributesList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
          attributesList.add(ReadWriteIOUtils.readMap(buffer));
        }
        createAlignedTimeSeriesPlan.setAttributesList(attributesList);
      }

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();

      return createAlignedTimeSeriesPlan;
    }

    @Override
    public ISchemaRegionPlan visitCreateTimeSeries(
        final ICreateTimeSeriesPlan createTimeSeriesPlan, final ByteBuffer buffer) {
      final int length = buffer.getInt();
      final byte[] bytes = new byte[length];
      buffer.get(bytes);
      try {
        createTimeSeriesPlan.setPath(
            new MeasurementPath(new String(bytes, TSFileConfig.STRING_CHARSET)));
      } catch (final IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }

      createTimeSeriesPlan.setDataType(TSDataType.deserialize(buffer.get()));
      createTimeSeriesPlan.setEncoding(TSEncoding.deserialize(buffer.get()));
      createTimeSeriesPlan.setCompressor(CompressionType.deserialize(buffer.get()));
      createTimeSeriesPlan.setTagOffset(buffer.getLong());

      // alias
      if (buffer.get() == 1) {
        createTimeSeriesPlan.setAlias(ReadWriteIOUtils.readString(buffer));
      }

      // props
      if (buffer.get() == 1) {
        createTimeSeriesPlan.setProps(ReadWriteIOUtils.readMap(buffer));
      }

      // tags
      if (buffer.get() == 1) {
        createTimeSeriesPlan.setTags(ReadWriteIOUtils.readMap(buffer));
      }

      // attributes
      if (buffer.get() == 1) {
        createTimeSeriesPlan.setAttributes(ReadWriteIOUtils.readMap(buffer));
      }

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();

      return createTimeSeriesPlan;
    }

    @Override
    public ISchemaRegionPlan visitDeleteTimeSeries(
        final IDeleteTimeSeriesPlan deleteTimeSeriesPlan, final ByteBuffer buffer) {
      int pathNumber = buffer.getInt();

      try {
        List<PartialPath> deletePathList = new ArrayList<>();
        for (int i = 0; i < pathNumber; i++) {
          deletePathList.add(new PartialPath(ReadWriteIOUtils.readString(buffer)));
        }
        deleteTimeSeriesPlan.setDeletePathList(deletePathList);
      } catch (IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();

      return deleteTimeSeriesPlan;
    }

    @Override
    public ISchemaRegionPlan visitPreDeleteTimeSeries(
        final IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, final ByteBuffer buffer) {
      preDeleteTimeSeriesPlan.setPath((PartialPath) PathDeserializeUtil.deserialize(buffer));

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();

      return preDeleteTimeSeriesPlan;
    }

    @Override
    public ISchemaRegionPlan visitRollbackPreDeleteTimeSeries(
        final IRollbackPreDeleteTimeSeriesPlan rollbackPreDeleteTimeSeriesPlan,
        final ByteBuffer buffer) {
      rollbackPreDeleteTimeSeriesPlan.setPath(
          (PartialPath) PathDeserializeUtil.deserialize(buffer));

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();

      return rollbackPreDeleteTimeSeriesPlan;
    }

    @Override
    public ISchemaRegionPlan visitPreDeactivateTemplate(
        final IPreDeactivateTemplatePlan preDeactivateTemplatePlan, final ByteBuffer buffer) {
      preDeactivateTemplatePlan.setTemplateSetInfo(deserializeTemplateSetInfo(buffer));
      return preDeactivateTemplatePlan;
    }

    @Override
    public ISchemaRegionPlan visitRollbackPreDeactivateTemplate(
        final IRollbackPreDeactivateTemplatePlan rollbackPreDeactivateTemplatePlan,
        final ByteBuffer buffer) {
      rollbackPreDeactivateTemplatePlan.setTemplateSetInfo(deserializeTemplateSetInfo(buffer));
      return rollbackPreDeactivateTemplatePlan;
    }

    @Override
    public ISchemaRegionPlan visitDeactivateTemplate(
        final IDeactivateTemplatePlan deactivateTemplatePlan, final ByteBuffer buffer) {
      deactivateTemplatePlan.setTemplateSetInfo(deserializeTemplateSetInfo(buffer));
      return deactivateTemplatePlan;
    }

    private Map<PartialPath, List<Integer>> deserializeTemplateSetInfo(final ByteBuffer buffer) {
      final int size = buffer.getInt();
      final Map<PartialPath, List<Integer>> result = new HashMap<>(size);
      PartialPath pattern;
      int templateNum;
      List<Integer> templateIdList;
      for (int i = 0; i < size; i++) {
        pattern = (PartialPath) PathDeserializeUtil.deserialize(buffer);
        templateNum = buffer.getInt();
        templateIdList = new ArrayList<>(templateNum);
        for (int j = 0; j < templateNum; j++) {
          templateIdList.add(buffer.getInt());
        }
        result.put(pattern, templateIdList);
      }
      return result;
    }

    @Override
    public ISchemaRegionPlan visitCreateLogicalView(
        final ICreateLogicalViewPlan createLogicalViewPlan, final ByteBuffer buffer) {

      final int viewSize = buffer.getInt();
      final Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
      for (int i = 0; i < viewSize; i++) {
        final int byteSizeOfPath = buffer.getInt();
        final byte[] bytesOfPath = new byte[byteSizeOfPath];
        buffer.get(bytesOfPath);
        try {
          final PartialPath thisPath = new PartialPath(new String(bytesOfPath));
          ViewExpression thisExp = ViewExpression.deserialize(buffer);
          viewPathToSourceMap.put(thisPath, thisExp);
        } catch (final IllegalPathException e) {
          LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
        }
      }
      createLogicalViewPlan.setViewPathToSourceExpressionMap(viewPathToSourceMap);
      return createLogicalViewPlan;
    }

    @Override
    public ISchemaRegionPlan visitPreDeleteLogicalView(
        final IPreDeleteLogicalViewPlan preDeleteLogicalViewPlan, final ByteBuffer buffer) {
      preDeleteLogicalViewPlan.setPath((PartialPath) PathDeserializeUtil.deserialize(buffer));
      return preDeleteLogicalViewPlan;
    }

    @Override
    public ISchemaRegionPlan visitRollbackPreDeleteLogicalView(
        final IRollbackPreDeleteLogicalViewPlan rollbackPreDeleteLogicalViewPlan,
        final ByteBuffer buffer) {
      rollbackPreDeleteLogicalViewPlan.setPath(
          (PartialPath) PathDeserializeUtil.deserialize(buffer));
      return rollbackPreDeleteLogicalViewPlan;
    }

    @Override
    public ISchemaRegionPlan visitDeleteLogicalView(
        final IDeleteLogicalViewPlan deleteLogicalViewPlan, final ByteBuffer buffer) {
      deleteLogicalViewPlan.setPath((PartialPath) PathDeserializeUtil.deserialize(buffer));
      return deleteLogicalViewPlan;
    }

    @Override
    public ISchemaRegionPlan visitAlterLogicalView(
        final IAlterLogicalViewPlan alterLogicalViewPlan, final ByteBuffer buffer) {
      alterLogicalViewPlan.setViewPath((PartialPath) PathDeserializeUtil.deserialize(buffer));
      alterLogicalViewPlan.setSourceExpression(ViewExpression.deserialize(buffer));
      return alterLogicalViewPlan;
    }

    @Override
    public ISchemaRegionPlan visitCreateOrUpdateTableDevice(
        final CreateOrUpdateTableDeviceNode createOrUpdateTableDeviceNode,
        final ByteBuffer buffer) {
      return (CreateOrUpdateTableDeviceNode) PlanNodeType.deserialize(buffer);
    }

    @Override
    public ISchemaRegionPlan visitUpdateTableDeviceAttribute(
        final TableDeviceAttributeUpdateNode updateTableDeviceAttributePlan,
        final ByteBuffer buffer) {
      return (TableDeviceAttributeUpdateNode) PlanNodeType.deserialize(buffer);
    }
  }
}
