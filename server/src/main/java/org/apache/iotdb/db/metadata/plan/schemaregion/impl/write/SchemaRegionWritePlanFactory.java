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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanType;
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

import java.util.List;
import java.util.Map;

public class SchemaRegionWritePlanFactory {

  private SchemaRegionWritePlanFactory() {}

  public static ISchemaRegionPlan getEmptyPlan(SchemaRegionPlanType planType) {
    switch (planType) {
      case CREATE_TIMESERIES:
        return new CreateTimeSeriesPlanImpl();
      case DELETE_TIMESERIES:
        return new DeleteTimeSeriesPlanImpl();
      case CHANGE_TAG_OFFSET:
        return new ChangeTagOffsetPlanImpl();
      case CHANGE_ALIAS:
        return new ChangeAliasPlanImpl();
      case AUTO_CREATE_DEVICE_MNODE:
        return new AutoCreateDeviceMNodePlanImpl();
      case CREATE_ALIGNED_TIMESERIES:
        return new CreateAlignedTimeSeriesPlanImpl();
      case ACTIVATE_TEMPLATE_IN_CLUSTER:
        return new ActivateTemplateInClusterPlanImpl();
      case PRE_DELETE_TIMESERIES_IN_CLUSTER:
        return new PreDeleteTimeSeriesPlanImpl();
      case ROLLBACK_PRE_DELETE_TIMESERIES:
        return new RollbackPreDeleteTimeSeriesPlanImpl();
      case PRE_DEACTIVATE_TEMPLATE:
        return new PreDeactivateTemplatePlanImpl();
      case ROLLBACK_PRE_DEACTIVATE_TEMPLATE:
        return new RollbackPreDeactivateTemplatePlanImpl();
      case DEACTIVATE_TEMPLATE:
        return new DeactivateTemplatePlanImpl();
      default:
        throw new UnsupportedOperationException(
            String.format(
                "SchemaRegionPlan of type %s doesn't support creating empty plan.",
                planType.name()));
    }
  }

  public static IChangeAliasPlan getChangeAliasPlan(PartialPath path, String alias) {
    return new ChangeAliasPlanImpl(path, alias);
  }

  public static IChangeTagOffsetPlan getChangeTagOffsetPlan(PartialPath fullPath, long tagOffset) {
    return new ChangeTagOffsetPlanImpl(fullPath, tagOffset);
  }

  public static IAutoCreateDeviceMNodePlan getAutoCreateDeviceMNodePlan(PartialPath path) {
    return new AutoCreateDeviceMNodePlanImpl(path);
  }

  public static ICreateTimeSeriesPlan getCreateTimeSeriesPlan(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String alias) {
    return new CreateTimeSeriesPlanImpl(
        path, dataType, encoding, compressor, props, tags, attributes, alias);
  }

  public static ICreateAlignedTimeSeriesPlan getCreateAlignedTimeSeriesPlan(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList) {
    return new CreateAlignedTimeSeriesPlanImpl(
        prefixPath,
        measurements,
        dataTypes,
        encodings,
        compressors,
        aliasList,
        tagsList,
        attributesList);
  }

  public static IDeleteTimeSeriesPlan getDeleteTimeSeriesPlan(List<PartialPath> pathList) {
    return new DeleteTimeSeriesPlanImpl(pathList);
  }

  public static IPreDeleteTimeSeriesPlan getPreDeleteTimeSeriesPlan(PartialPath path) {
    return new PreDeleteTimeSeriesPlanImpl(path);
  }

  public static IRollbackPreDeleteTimeSeriesPlan getRollbackPreDeleteTimeSeriesPlan(
      PartialPath path) {
    return new RollbackPreDeleteTimeSeriesPlanImpl(path);
  }

  public static IActivateTemplateInClusterPlan getActivateTemplateInClusterPlan(
      PartialPath activatePath, int templateSetLevel, int templateId) {
    return new ActivateTemplateInClusterPlanImpl(activatePath, templateSetLevel, templateId);
  }

  public static IPreDeactivateTemplatePlan getPreDeactivateTemplatePlan(
      Map<PartialPath, List<Integer>> templateSetInfo) {
    return new PreDeactivateTemplatePlanImpl(templateSetInfo);
  }

  public static IRollbackPreDeactivateTemplatePlan getRollbackPreDeactivateTemplatePlan(
      Map<PartialPath, List<Integer>> templateSetInfo) {
    return new RollbackPreDeactivateTemplatePlanImpl(templateSetInfo);
  }

  public static IDeactivateTemplatePlan getDeactivateTemplatePlan(
      Map<PartialPath, List<Integer>> templateSetInfo) {
    return new DeactivateTemplatePlanImpl(templateSetInfo);
  }
}
