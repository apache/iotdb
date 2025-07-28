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

import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.ConstructTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.DeleteTableDevicesInBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.RollbackTableDevicesBlackListNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableAttributeColumnDropNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeCommitUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableNodeLocationAddNode;
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

  public abstract R visitSchemaRegionPlan(final ISchemaRegionPlan plan, final C context);

  public R visitActivateTemplateInCluster(
      final IActivateTemplateInClusterPlan activateTemplateInClusterPlan, final C context) {
    return visitSchemaRegionPlan(activateTemplateInClusterPlan, context);
  }

  public R visitAutoCreateDeviceMNode(
      final IAutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan, final C context) {
    return visitSchemaRegionPlan(autoCreateDeviceMNodePlan, context);
  }

  public R visitChangeAlias(final IChangeAliasPlan changeAliasPlan, final C context) {
    return visitSchemaRegionPlan(changeAliasPlan, context);
  }

  public R visitChangeTagOffset(final IChangeTagOffsetPlan changeTagOffsetPlan, final C context) {
    return visitSchemaRegionPlan(changeTagOffsetPlan, context);
  }

  public R visitCreateAlignedTimeSeries(
      final ICreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan, final C context) {
    return visitSchemaRegionPlan(createAlignedTimeSeriesPlan, context);
  }

  public R visitCreateTimeSeries(
      final ICreateTimeSeriesPlan createTimeSeriesPlan, final C context) {
    return visitSchemaRegionPlan(createTimeSeriesPlan, context);
  }

  public R visitDeleteTimeSeries(
      final IDeleteTimeSeriesPlan deleteTimeSeriesPlan, final C context) {
    return visitSchemaRegionPlan(deleteTimeSeriesPlan, context);
  }

  public R visitPreDeleteTimeSeries(
      final IPreDeleteTimeSeriesPlan preDeleteTimeSeriesPlan, final C context) {
    return visitSchemaRegionPlan(preDeleteTimeSeriesPlan, context);
  }

  public R visitRollbackPreDeleteTimeSeries(
      final IRollbackPreDeleteTimeSeriesPlan rollbackPreDeleteTimeSeriesPlan, final C context) {
    return visitSchemaRegionPlan(rollbackPreDeleteTimeSeriesPlan, context);
  }

  public R visitPreDeactivateTemplate(
      final IPreDeactivateTemplatePlan preDeactivateTemplatePlan, final C context) {
    return visitSchemaRegionPlan(preDeactivateTemplatePlan, context);
  }

  public R visitRollbackPreDeactivateTemplate(
      final IRollbackPreDeactivateTemplatePlan rollbackPreDeactivateTemplatePlan, final C context) {
    return visitSchemaRegionPlan(rollbackPreDeactivateTemplatePlan, context);
  }

  public R visitDeactivateTemplate(
      final IDeactivateTemplatePlan deactivateTemplatePlan, final C context) {
    return visitSchemaRegionPlan(deactivateTemplatePlan, context);
  }

  public R visitCreateLogicalView(
      final ICreateLogicalViewPlan createLogicalViewPlan, final C context) {
    return visitSchemaRegionPlan(createLogicalViewPlan, context);
  }

  public R visitAlterLogicalView(
      final IAlterLogicalViewPlan alterLogicalViewPlan, final C context) {
    return visitSchemaRegionPlan(alterLogicalViewPlan, context);
  }

  public R visitPreDeleteLogicalView(
      final IPreDeleteLogicalViewPlan preDeleteLogicalViewPlan, final C context) {
    return visitSchemaRegionPlan(preDeleteLogicalViewPlan, context);
  }

  public R visitRollbackPreDeleteLogicalView(
      final IRollbackPreDeleteLogicalViewPlan rollbackPreDeleteLogicalViewPlan, final C context) {
    return visitSchemaRegionPlan(rollbackPreDeleteLogicalViewPlan, context);
  }

  public R visitDeleteLogicalView(
      final IDeleteLogicalViewPlan deleteLogicalViewPlan, final C context) {
    return visitSchemaRegionPlan(deleteLogicalViewPlan, context);
  }

  public R visitCreateOrUpdateTableDevice(
      final CreateOrUpdateTableDeviceNode createOrUpdateTableDeviceNode, final C context) {
    return visitSchemaRegionPlan(createOrUpdateTableDeviceNode, context);
  }

  public R visitUpdateTableDeviceAttribute(
      final TableDeviceAttributeUpdateNode updateTableDeviceAttributePlan, final C context) {
    return visitSchemaRegionPlan(updateTableDeviceAttributePlan, context);
  }

  public R visitCommitUpdateTableDeviceAttribute(
      final TableDeviceAttributeCommitUpdateNode commitUpdateTableDeviceAttributePlan,
      final C context) {
    return visitSchemaRegionPlan(commitUpdateTableDeviceAttributePlan, context);
  }

  public R visitAddNodeLocation(
      final TableNodeLocationAddNode addNodeLocationPlan, final C context) {
    return visitSchemaRegionPlan(addNodeLocationPlan, context);
  }

  public R visitDeleteTableDevice(
      final DeleteTableDeviceNode deleteTableDevicePlan, final C context) {
    return visitSchemaRegionPlan(deleteTableDevicePlan, context);
  }

  public R visitConstructTableDevicesBlackList(
      final ConstructTableDevicesBlackListNode constructTableDevicesBlackListPlan,
      final C context) {
    return visitSchemaRegionPlan(constructTableDevicesBlackListPlan, context);
  }

  public R visitRollbackTableDevicesBlackList(
      final RollbackTableDevicesBlackListNode rollbackTableDevicesBlackListPlan, final C context) {
    return visitSchemaRegionPlan(rollbackTableDevicesBlackListPlan, context);
  }

  public R visitDeleteTableDevicesInBlackList(
      final DeleteTableDevicesInBlackListNode deleteTableDevicesInBlackListPlan, final C context) {
    return visitSchemaRegionPlan(deleteTableDevicesInBlackListPlan, context);
  }

  public R visitDropTableAttribute(
      final TableAttributeColumnDropNode dropTableAttributePlan, final C context) {
    return visitSchemaRegionPlan(dropTableAttributePlan, context);
  }
}
