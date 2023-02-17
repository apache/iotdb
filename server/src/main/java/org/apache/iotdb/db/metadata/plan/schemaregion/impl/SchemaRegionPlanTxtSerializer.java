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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class provides the ability to serialize the given SchemaRegionPlan into binary data as txt
 * format, which is used for MLogParser.
 */
public class SchemaRegionPlanTxtSerializer implements ISerializer<ISchemaRegionPlan> {

  private static final String FIELD_SEPARATOR = ", ";
  private static final String LINE_SEPARATOR = System.lineSeparator();

  @Override
  public void serialize(ISchemaRegionPlan plan, OutputStream outputStream) throws IOException {
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    StringBuilder stringBuilder = new StringBuilder();
    // serialize plan type
    stringBuilder.append(plan.getPlanType().name()).append(FIELD_SEPARATOR);
    // serialize plan attributes
    plan.accept(new SchemaRegionPlanTxtSerializeVisitor(), stringBuilder);
    stringBuilder.append(LINE_SEPARATOR);
    dataOutputStream.write(stringBuilder.toString().getBytes());
  }

  private static class SchemaRegionPlanTxtSerializeVisitor
      extends SchemaRegionPlanVisitor<Void, StringBuilder> {
    @Override
    public Void visitSchemaRegionPlan(ISchemaRegionPlan plan, StringBuilder stringBuilder) {
      throw new UnsupportedOperationException(
          String.format("%s plan doesn't support serialization.", plan.getPlanType().name()));
    }

    @Override
    public Void visitActivateTemplateInCluster(
        IActivateTemplateInClusterPlan activateTemplateInClusterPlan, StringBuilder stringBuilder) {
      stringBuilder
          .append(activateTemplateInClusterPlan.getActivatePath().getFullPath())
          .append(FIELD_SEPARATOR)
          .append(activateTemplateInClusterPlan.getTemplateSetLevel())
          .append(FIELD_SEPARATOR)
          .append(activateTemplateInClusterPlan.isAligned());
      return null;
    }

    @Override
    public Void visitAutoCreateDeviceMNode(
        IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, StringBuilder stringBuilder) {
      stringBuilder.append(autoCreateDeviceMNodePlan.getPath().getFullPath());
      return null;
    }

    @Override
    public Void visitChangeAlias(IChangeAliasPlan changeAliasPlan, StringBuilder stringBuilder) {
      stringBuilder
          .append(changeAliasPlan.getPath().getFullPath())
          .append(FIELD_SEPARATOR)
          .append(changeAliasPlan.getAlias());
      return null;
    }

    @Override
    public Void visitChangeTagOffset(
        IChangeTagOffsetPlan changeTagOffsetPlan, StringBuilder stringBuilder) {
      stringBuilder
          .append(changeTagOffsetPlan.getPath().getFullPath())
          .append(FIELD_SEPARATOR)
          .append(changeTagOffsetPlan.getOffset());
      return null;
    }

    @Override
    public Void visitCreateAlignedTimeSeries(
        ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan, StringBuilder stringBuilder) {
      stringBuilder
          .append(createAlignedTimeSeriesPlan.getDevicePath().getFullPath())
          .append(FIELD_SEPARATOR)
          .append(createAlignedTimeSeriesPlan.getMeasurements())
          .append(FIELD_SEPARATOR)
          .append(
              createAlignedTimeSeriesPlan.getDataTypes().stream()
                  .map(TSDataType::name)
                  .collect(Collectors.toList()))
          .append(FIELD_SEPARATOR)
          .append(
              createAlignedTimeSeriesPlan.getEncodings().stream()
                  .map(TSEncoding::name)
                  .collect(Collectors.toList()))
          .append(FIELD_SEPARATOR)
          .append(
              createAlignedTimeSeriesPlan.getCompressors().stream()
                  .map(CompressionType::name)
                  .collect(Collectors.toList()))
          .append(FIELD_SEPARATOR);

      if (createAlignedTimeSeriesPlan.getAliasList() != null) {
        stringBuilder.append(createAlignedTimeSeriesPlan.getAliasList());
      }
      stringBuilder.append(FIELD_SEPARATOR);

      if (createAlignedTimeSeriesPlan.getTagsList() != null) {
        stringBuilder.append(createAlignedTimeSeriesPlan.getTagsList());
      }
      stringBuilder.append(FIELD_SEPARATOR);

      if (createAlignedTimeSeriesPlan.getAttributesList() != null) {
        stringBuilder.append(createAlignedTimeSeriesPlan.getAttributesList());
      }
      stringBuilder.append(FIELD_SEPARATOR);

      stringBuilder.append(createAlignedTimeSeriesPlan.getTagOffsets());

      return null;
    }

    @Override
    public Void visitCreateTimeSeries(
        ICreateTimeSeriesPlan createTimeSeriesPlan, StringBuilder stringBuilder) {
      stringBuilder
          .append(createTimeSeriesPlan.getPath())
          .append(FIELD_SEPARATOR)
          .append(createTimeSeriesPlan.getDataType().name())
          .append(FIELD_SEPARATOR)
          .append(createTimeSeriesPlan.getEncoding().name())
          .append(FIELD_SEPARATOR)
          .append(createTimeSeriesPlan.getCompressor().name())
          .append(FIELD_SEPARATOR);

      if (createTimeSeriesPlan.getAlias() != null) {
        stringBuilder.append(createTimeSeriesPlan.getAlias());
      }
      stringBuilder.append(FIELD_SEPARATOR);

      if (createTimeSeriesPlan.getTags() != null) {
        stringBuilder.append(createTimeSeriesPlan.getTags());
      }
      stringBuilder.append(FIELD_SEPARATOR);

      if (createTimeSeriesPlan.getAttributes() != null) {
        stringBuilder.append(createTimeSeriesPlan.getAttributes());
      }
      stringBuilder.append(FIELD_SEPARATOR);

      stringBuilder.append(createTimeSeriesPlan.getTagOffset());

      return null;
    }

    @Override
    public Void visitDeleteTimeSeries(
        IDeleteTimeSeriesPlan deleteTimeSeriesPlan, StringBuilder stringBuilder) {
      stringBuilder.append(deleteTimeSeriesPlan.getDeletePathList());
      return null;
    }

    @Override
    public Void visitPreDeleteTimeSeries(
        IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, StringBuilder stringBuilder) {
      stringBuilder.append(preDeleteTimeSeriesPlan.getPath().getFullPath());
      return null;
    }

    @Override
    public Void visitRollbackPreDeleteTimeSeries(
        IRollbackPreDeleteTimeSeriesPlan rollbackPreDeleteTimeSeriesPlan,
        StringBuilder stringBuilder) {
      stringBuilder.append(rollbackPreDeleteTimeSeriesPlan.getPath().getFullPath());
      return null;
    }

    @Override
    public Void visitPreDeactivateTemplate(
        IPreDeactivateTemplatePlan preDeactivateTemplatePlan, StringBuilder stringBuilder) {
      parseTemplateSetInfo(preDeactivateTemplatePlan.getTemplateSetInfo(), stringBuilder);
      return null;
    }

    @Override
    public Void visitRollbackPreDeactivateTemplate(
        IRollbackPreDeactivateTemplatePlan rollbackPreDeactivateTemplatePlan,
        StringBuilder stringBuilder) {
      parseTemplateSetInfo(rollbackPreDeactivateTemplatePlan.getTemplateSetInfo(), stringBuilder);
      return null;
    }

    @Override
    public Void visitDeactivateTemplate(
        IDeactivateTemplatePlan deactivateTemplatePlan, StringBuilder stringBuilder) {
      parseTemplateSetInfo(deactivateTemplatePlan.getTemplateSetInfo(), stringBuilder);
      return null;
    }

    private void parseTemplateSetInfo(
        Map<PartialPath, List<Integer>> templateSetInfo, StringBuilder stringBuilder) {
      stringBuilder.append("{");
      templateSetInfo.forEach((k, v) -> stringBuilder.append(k).append(": ").append(v).append(";"));
      stringBuilder.append("}");
    }
  }
}
