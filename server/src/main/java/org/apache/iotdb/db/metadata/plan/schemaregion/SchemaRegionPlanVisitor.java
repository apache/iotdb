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

package org.apache.iotdb.db.metadata.plan.schemaregion;

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

public abstract class SchemaRegionPlanVisitor<R, C> {

  public abstract R visitSchemaRegionPlan(ISchemaRegionPlan plan, C context);

  public R visitActivateTemplateInCluster(
      IActivateTemplateInClusterPlan activateTemplateInClusterPlan, C context) {
    return visitSchemaRegionPlan(activateTemplateInClusterPlan, context);
  }

  public R visitAutoCreateDeviceMNode(
      IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, C context) {
    return visitSchemaRegionPlan(autoCreateDeviceMNodePlan, context);
  }

  public R visitChangeAlias(IChangeAliasPlan changeAliasPlan, C context) {
    return visitSchemaRegionPlan(changeAliasPlan, context);
  }

  public R visitChangeTagOffset(IChangeTagOffsetPlan changeTagOffsetPlan, C context) {
    return visitSchemaRegionPlan(changeTagOffsetPlan, context);
  }

  public R visitCreateAlignedTimeSeries(
      ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan, C context) {
    return visitSchemaRegionPlan(createAlignedTimeSeriesPlan, context);
  }

  public R visitCreateTimeSeries(ICreateTimeSeriesPlan createTimeSeriesPlan, C context) {
    return visitSchemaRegionPlan(createTimeSeriesPlan, context);
  }

  public R visitDeleteTimeSeries(IDeleteTimeSeriesPlan deleteTimeSeriesPlan, C context) {
    return visitSchemaRegionPlan(deleteTimeSeriesPlan, context);
  }

  public R visitPreDeleteTimeSeries(IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, C context) {
    return visitSchemaRegionPlan(preDeleteTimeSeriesPlan, context);
  }

  public R visitRollbackPreDeleteTimeSeries(
      IRollbackPreDeleteTimeSeriesPlan rollbackPreDeleteTimeSeriesPlan, C context) {
    return visitSchemaRegionPlan(rollbackPreDeleteTimeSeriesPlan, context);
  }

  public R visitPreDeactivateTemplate(
      IPreDeactivateTemplatePlan preDeactivateTemplatePlan, C context) {
    return visitSchemaRegionPlan(preDeactivateTemplatePlan, context);
  }

  public R visitRollbackPreDeactivateTemplate(
      IRollbackPreDeactivateTemplatePlan rollbackPreDeactivateTemplatePlan, C context) {
    return visitSchemaRegionPlan(rollbackPreDeactivateTemplatePlan, context);
  }

  public R visitDeactivateTemplate(IDeactivateTemplatePlan deactivateTemplatePlan, C context) {
    return visitSchemaRegionPlan(deactivateTemplatePlan, context);
  }
}
