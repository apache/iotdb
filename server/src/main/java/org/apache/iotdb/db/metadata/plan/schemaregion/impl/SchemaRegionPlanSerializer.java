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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.logfile.ISerializer;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanVisitor;
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

import javax.annotation.concurrent.NotThreadSafe;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * This class provides the ability to serialize the given SchemaRegionPlan into binary data, which
 * is compatible with that of PhysicalPlan in 0.14 snapshot versions.
 */
@NotThreadSafe
public class SchemaRegionPlanSerializer implements ISerializer<ISchemaRegionPlan> {

  public final ConfigurableDataOutputStream dataOutputStream =
      new ConfigurableDataOutputStream(null);

  @Override
  public void serialize(ISchemaRegionPlan plan, OutputStream outputStream) throws IOException {
    dataOutputStream.changeOutputStream(outputStream);
    // serialize plan type
    plan.getPlanType().serialize(dataOutputStream);
    // serialize plan attributes
    SchemaRegionPlanSerializationResult result =
        plan.accept(new SchemaRegionPlanSerializeVisitor(), dataOutputStream);
    if (result.isFailed()) {
      throw result.getException();
    }
  }

  private static class ConfigurableDataOutputStream extends DataOutputStream {

    private ConfigurableDataOutputStream(OutputStream out) {
      super(out);
    }

    private void changeOutputStream(OutputStream out) {
      this.out = out;
      written = 0;
    }
  }

  private static class SchemaRegionPlanSerializationResult {

    private static final SchemaRegionPlanSerializationResult SUCCESS =
        new SchemaRegionPlanSerializationResult(null);

    private final IOException exception;

    private SchemaRegionPlanSerializationResult(IOException exception) {
      this.exception = exception;
    }

    private boolean isFailed() {
      return exception != null;
    }

    private IOException getException() {
      return exception;
    }
  }

  private static class SchemaRegionPlanSerializeVisitor
      extends SchemaRegionPlanVisitor<SchemaRegionPlanSerializationResult, DataOutputStream> {
    @Override
    public SchemaRegionPlanSerializationResult visitSchemaRegionPlan(
        ISchemaRegionPlan plan, DataOutputStream dataOutputStream) {
      throw new UnsupportedOperationException(
          String.format("%s plan doesn't support serialization.", plan.getPlanType().name()));
    }

    @Override
    public SchemaRegionPlanSerializationResult visitActivateTemplateInCluster(
        IActivateTemplateInClusterPlan activateTemplateInClusterPlan,
        DataOutputStream dataOutputStream) {
      try {
        ReadWriteIOUtils.write(
            activateTemplateInClusterPlan.getActivatePath().getFullPath(), dataOutputStream);
        dataOutputStream.writeInt(activateTemplateInClusterPlan.getTemplateSetLevel());
        dataOutputStream.writeInt(activateTemplateInClusterPlan.getTemplateId());
        dataOutputStream.writeBoolean(activateTemplateInClusterPlan.isAligned());
        // serialize a long to keep compatible with old version (raft index)
        dataOutputStream.writeLong(0);
        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitAutoCreateDeviceMNode(
        IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, DataOutputStream dataOutputStream) {
      try {
        ReadWriteIOUtils.write(autoCreateDeviceMNodePlan.getPath().getFullPath(), dataOutputStream);
        // serialize a long to keep compatible with old version (raft index)
        dataOutputStream.writeLong(0);
        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitChangeAlias(
        IChangeAliasPlan changeAliasPlan, DataOutputStream dataOutputStream) {
      try {
        ReadWriteIOUtils.write(changeAliasPlan.getPath().getFullPath(), dataOutputStream);
        ReadWriteIOUtils.write(changeAliasPlan.getAlias(), dataOutputStream);
        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitChangeTagOffset(
        IChangeTagOffsetPlan changeTagOffsetPlan, DataOutputStream dataOutputStream) {
      try {
        ReadWriteIOUtils.write(changeTagOffsetPlan.getPath().getFullPath(), dataOutputStream);
        dataOutputStream.writeLong(changeTagOffsetPlan.getOffset());
        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitCreateAlignedTimeSeries(
        ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan,
        DataOutputStream dataOutputStream) {
      try {
        // serialize a int to keep compatible with old version
        dataOutputStream.writeInt(-1);

        byte[] bytes = createAlignedTimeSeriesPlan.getDevicePath().getFullPath().getBytes();
        dataOutputStream.writeInt(bytes.length);
        dataOutputStream.write(bytes);

        List<String> measurements = createAlignedTimeSeriesPlan.getMeasurements();
        dataOutputStream.writeInt(measurements.size());
        for (String measurement : measurements) {
          ReadWriteIOUtils.write(measurement, dataOutputStream);
        }

        for (TSDataType dataType : createAlignedTimeSeriesPlan.getDataTypes()) {
          dataOutputStream.writeByte(dataType.ordinal());
        }

        for (TSEncoding tsEncoding : createAlignedTimeSeriesPlan.getEncodings()) {
          dataOutputStream.writeByte(tsEncoding.ordinal());
        }

        for (CompressionType compressionType : createAlignedTimeSeriesPlan.getCompressors()) {
          dataOutputStream.writeByte(compressionType.ordinal());
        }

        for (long tagOffset : createAlignedTimeSeriesPlan.getTagOffsets()) {
          dataOutputStream.writeLong(tagOffset);
        }

        // alias
        List<String> aliasList = createAlignedTimeSeriesPlan.getAliasList();
        if (aliasList == null || aliasList.isEmpty()) {
          dataOutputStream.writeByte(0);
        } else {
          dataOutputStream.writeByte(1);
          for (String alias : aliasList) {
            ReadWriteIOUtils.write(alias, dataOutputStream);
          }
        }

        // tags
        List<Map<String, String>> tagsList = createAlignedTimeSeriesPlan.getTagsList();
        if (tagsList == null || tagsList.isEmpty()) {
          dataOutputStream.writeByte(0);
        } else {
          dataOutputStream.writeByte(1);
          for (Map<String, String> tags : tagsList) {
            ReadWriteIOUtils.write(tags, dataOutputStream);
          }
        }

        // attributes
        List<Map<String, String>> attributesList = createAlignedTimeSeriesPlan.getAttributesList();
        if (attributesList == null || attributesList.isEmpty()) {
          dataOutputStream.writeByte(0);
        } else {
          dataOutputStream.writeByte(1);
          for (Map<String, String> attributes : attributesList) {
            ReadWriteIOUtils.write(attributes, dataOutputStream);
          }
        }

        // serialize a long to keep compatible with old version (raft index)
        dataOutputStream.writeLong(0);

        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitCreateTimeSeries(
        ICreateTimeSeriesPlan createTimeSeriesPlan, DataOutputStream dataOutputStream) {
      try {

        byte[] bytes = createTimeSeriesPlan.getPath().getFullPath().getBytes();
        dataOutputStream.writeInt(bytes.length);
        dataOutputStream.write(bytes);
        dataOutputStream.writeByte(createTimeSeriesPlan.getDataType().ordinal());
        dataOutputStream.writeByte(createTimeSeriesPlan.getEncoding().ordinal());
        dataOutputStream.writeByte(createTimeSeriesPlan.getCompressor().ordinal());
        dataOutputStream.writeLong(createTimeSeriesPlan.getTagOffset());

        // alias
        if (createTimeSeriesPlan.getAlias() == null) {
          dataOutputStream.writeByte(0);
        } else {
          dataOutputStream.writeByte(1);
          ReadWriteIOUtils.write(createTimeSeriesPlan.getAlias(), dataOutputStream);
        }

        // props
        Map<String, String> props = createTimeSeriesPlan.getProps();
        if (props == null || props.isEmpty()) {
          dataOutputStream.writeByte(0);
        } else {
          dataOutputStream.writeByte(1);
          ReadWriteIOUtils.write(props, dataOutputStream);
        }

        // tags
        Map<String, String> tags = createTimeSeriesPlan.getTags();
        if (tags == null || tags.isEmpty()) {
          dataOutputStream.writeByte(0);
        } else {
          dataOutputStream.writeByte(1);
          ReadWriteIOUtils.write(tags, dataOutputStream);
        }

        // attributes
        Map<String, String> attributes = createTimeSeriesPlan.getAttributes();
        if (attributes == null || attributes.isEmpty()) {
          dataOutputStream.writeByte(0);
        } else {
          dataOutputStream.writeByte(1);
          ReadWriteIOUtils.write(attributes, dataOutputStream);
        }

        // serialize a long to keep compatible with old version (raft index)
        dataOutputStream.writeLong(0);

        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitDeleteTimeSeries(
        IDeleteTimeSeriesPlan deleteTimeSeriesPlan, DataOutputStream dataOutputStream) {
      try {
        List<PartialPath> deletePathList = deleteTimeSeriesPlan.getDeletePathList();
        dataOutputStream.writeInt(deletePathList.size());
        for (PartialPath path : deletePathList) {
          ReadWriteIOUtils.write(path.getFullPath(), dataOutputStream);
        }

        // serialize a long to keep compatible with old version (raft index)
        dataOutputStream.writeLong(0);

        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitPreDeleteTimeSeries(
        IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, DataOutputStream dataOutputStream) {
      try {
        preDeleteTimeSeriesPlan.getPath().serialize(dataOutputStream);

        // serialize a long to keep compatible with old version (raft index)
        dataOutputStream.writeLong(0);

        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitRollbackPreDeleteTimeSeries(
        IRollbackPreDeleteTimeSeriesPlan rollbackPreDeleteTimeSeriesPlan,
        DataOutputStream dataOutputStream) {
      try {
        rollbackPreDeleteTimeSeriesPlan.getPath().serialize(dataOutputStream);

        // serialize a long to keep compatible with old version (raft index)
        dataOutputStream.writeLong(0);

        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitPreDeactivateTemplate(
        IPreDeactivateTemplatePlan preDeactivateTemplatePlan, DataOutputStream dataOutputStream) {
      try {
        serializeTemplateSetInfo(preDeactivateTemplatePlan.getTemplateSetInfo(), dataOutputStream);
        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitRollbackPreDeactivateTemplate(
        IRollbackPreDeactivateTemplatePlan rollbackPreDeactivateTemplatePlan,
        DataOutputStream dataOutputStream) {
      try {
        serializeTemplateSetInfo(
            rollbackPreDeactivateTemplatePlan.getTemplateSetInfo(), dataOutputStream);
        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    @Override
    public SchemaRegionPlanSerializationResult visitDeactivateTemplate(
        IDeactivateTemplatePlan deactivateTemplatePlan, DataOutputStream dataOutputStream) {
      try {
        serializeTemplateSetInfo(deactivateTemplatePlan.getTemplateSetInfo(), dataOutputStream);
        return SchemaRegionPlanSerializationResult.SUCCESS;
      } catch (IOException e) {
        return new SchemaRegionPlanSerializationResult(e);
      }
    }

    private void serializeTemplateSetInfo(
        Map<PartialPath, List<Integer>> templateSetInfo, DataOutputStream dataOutputStream)
        throws IOException {
      dataOutputStream.writeInt(templateSetInfo.size());
      for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
        entry.getKey().serialize(dataOutputStream);
        dataOutputStream.writeInt(entry.getValue().size());
        for (int templateId : entry.getValue()) {
          dataOutputStream.writeInt(templateId);
        }
      }
    }
  }
}
