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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.metadata.logfile.IDeserializer;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IAutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IChangeAliasPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IChangeTagOffsetPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeleteTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeleteTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
  public ISchemaRegionPlan deserialize(ByteBuffer byteBuffer) {
    ISchemaRegionPlan schemaRegionPlan =
        SchemaRegionWritePlanFactory.getEmptyPlan(SchemaRegionPlanType.deserialize(byteBuffer));
    return schemaRegionPlan.accept(new SchemaRegionPlanDeserializeVisitor(), byteBuffer);
  }

  private static class SchemaRegionPlanDeserializeVisitor
      extends SchemaRegionPlanVisitor<ISchemaRegionPlan, ByteBuffer> {

    @Override
    public ISchemaRegionPlan visitSchemaRegionPlan(ISchemaRegionPlan plan, ByteBuffer byteBuffer) {
      throw new UnsupportedOperationException(
          String.format("%s plan doesn't support deserialization.", plan.getPlanType().name()));
    }

    @Override
    public ISchemaRegionPlan visitActivateTemplateInCluster(
        IActivateTemplateInClusterPlan activateTemplateInClusterPlan, ByteBuffer buffer) {
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
        IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, ByteBuffer buffer) {
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
    public ISchemaRegionPlan visitChangeAlias(IChangeAliasPlan changeAliasPlan, ByteBuffer buffer) {
      try {
        changeAliasPlan.setPath(new PartialPath(ReadWriteIOUtils.readString(buffer)));
      } catch (IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }
      changeAliasPlan.setAlias(ReadWriteIOUtils.readString(buffer));
      return changeAliasPlan;
    }

    @Override
    public ISchemaRegionPlan visitChangeTagOffset(
        IChangeTagOffsetPlan changeTagOffsetPlan, ByteBuffer buffer) {
      try {
        changeTagOffsetPlan.setPath(new PartialPath(ReadWriteIOUtils.readString(buffer)));
      } catch (IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }
      changeTagOffsetPlan.setOffset(buffer.getLong());
      return changeTagOffsetPlan;
    }

    @Override
    public ISchemaRegionPlan visitCreateAlignedTimeSeries(
        ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan, ByteBuffer buffer) {
      // deserialize a version mark to adapt to old version
      buffer.getInt();

      int length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try {
        createAlignedTimeSeriesPlan.setDevicePath(new PartialPath(new String(bytes)));
      } catch (IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }

      int size = ReadWriteIOUtils.readInt(buffer);
      List<String> measurements = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        measurements.add(ReadWriteIOUtils.readString(buffer));
      }
      createAlignedTimeSeriesPlan.setMeasurements(measurements);

      List<TSDataType> dataTypes = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        dataTypes.add(TSDataType.values()[buffer.get()]);
      }
      createAlignedTimeSeriesPlan.setDataTypes(dataTypes);

      List<TSEncoding> encodings = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        encodings.add(TSEncoding.values()[buffer.get()]);
      }
      createAlignedTimeSeriesPlan.setEncodings(encodings);

      List<CompressionType> compressors = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        compressors.add(CompressionType.deserialize(buffer.get()));
      }
      createAlignedTimeSeriesPlan.setCompressors(compressors);

      List<Long> tagOffsets = new ArrayList<>();
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
        ICreateTimeSeriesPlan createTimeSeriesPlan, ByteBuffer buffer) {
      int length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try {
        createTimeSeriesPlan.setPath(new PartialPath(new String(bytes)));
      } catch (IllegalPathException e) {
        LOGGER.error("Cannot deserialize SchemaRegionPlan from buffer", e);
      }

      createTimeSeriesPlan.setDataType(TSDataType.values()[buffer.get()]);
      createTimeSeriesPlan.setEncoding(TSEncoding.values()[buffer.get()]);
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
        IDeleteTimeSeriesPlan deleteTimeSeriesPlan, ByteBuffer buffer) {
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
        IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, ByteBuffer buffer) {
      preDeleteTimeSeriesPlan.setPath((PartialPath) PathDeserializeUtil.deserialize(buffer));

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();

      return preDeleteTimeSeriesPlan;
    }

    @Override
    public ISchemaRegionPlan visitRollbackPreDeleteTimeSeries(
        IRollbackPreDeleteTimeSeriesPlan rollbackPreDeleteTimeSeriesPlan, ByteBuffer buffer) {
      rollbackPreDeleteTimeSeriesPlan.setPath(
          (PartialPath) PathDeserializeUtil.deserialize(buffer));

      // deserialize a long to keep compatible with old version (raft index)
      buffer.getLong();

      return rollbackPreDeleteTimeSeriesPlan;
    }

    @Override
    public ISchemaRegionPlan visitPreDeactivateTemplate(
        IPreDeactivateTemplatePlan preDeactivateTemplatePlan, ByteBuffer buffer) {
      preDeactivateTemplatePlan.setTemplateSetInfo(deserializeTemplateSetInfo(buffer));
      return preDeactivateTemplatePlan;
    }

    @Override
    public ISchemaRegionPlan visitRollbackPreDeactivateTemplate(
        IRollbackPreDeactivateTemplatePlan rollbackPreDeactivateTemplatePlan, ByteBuffer buffer) {
      rollbackPreDeactivateTemplatePlan.setTemplateSetInfo(deserializeTemplateSetInfo(buffer));
      return rollbackPreDeactivateTemplatePlan;
    }

    @Override
    public ISchemaRegionPlan visitDeactivateTemplate(
        IDeactivateTemplatePlan deactivateTemplatePlan, ByteBuffer buffer) {
      deactivateTemplatePlan.setTemplateSetInfo(deserializeTemplateSetInfo(buffer));
      return deactivateTemplatePlan;
    }

    private Map<PartialPath, List<Integer>> deserializeTemplateSetInfo(ByteBuffer buffer) {
      int size = buffer.getInt();
      Map<PartialPath, List<Integer>> result = new HashMap<>(size);
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
  }
}
