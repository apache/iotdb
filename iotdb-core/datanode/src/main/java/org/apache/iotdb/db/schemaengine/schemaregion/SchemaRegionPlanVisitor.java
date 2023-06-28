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

package org.apache.iotdb.db.schemaengine.schemaregion;

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
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IAlterLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.ICreateLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IDeleteLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IPreDeleteLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IRollbackPreDeleteLogicalViewPlan;

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

  public R visitCreateLogicalView(ICreateLogicalViewPlan createLogicalViewPlan, C context) {
    return visitSchemaRegionPlan(createLogicalViewPlan, context);
  }

  public R visitAlterLogicalView(IAlterLogicalViewPlan alterLogicalViewPlan, C context) {
    return visitSchemaRegionPlan(alterLogicalViewPlan, context);
  }

  public R visitPreDeleteLogicalView(
      IPreDeleteLogicalViewPlan preDeleteLogicalViewPlan, C context) {
    return visitSchemaRegionPlan(preDeleteLogicalViewPlan, context);
  }

  public R visitRollbackPreDeleteLogicalView(
      IRollbackPreDeleteLogicalViewPlan rollbackPreDeleteLogicalViewPlan, C context) {
    return visitSchemaRegionPlan(rollbackPreDeleteLogicalViewPlan, context);
  }

  public R visitDeleteLogicalView(IDeleteLogicalViewPlan deleteLogicalViewPlan, C context) {
    return visitSchemaRegionPlan(deleteLogicalViewPlan, context);
  }
}
