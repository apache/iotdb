/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.source.schemaregion;

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.Optional;

public class PipePlanTreePrivilegeParseVisitor
    extends PlanVisitor<Optional<PlanNode>, IAuditEntity> {

  private final boolean skip;

  PipePlanTreePrivilegeParseVisitor(final boolean skip) {
    this.skip = skip;
  }

  @Override
  public Optional<PlanNode> visitPlan(final PlanNode node, final IAuditEntity context) {
    return Optional.of(node);
  }

  @Override
  public Optional<PlanNode> visitCreateTimeSeries(
      final CreateTimeSeriesNode node, final IAuditEntity auditEntity) {
    return TreeAccessCheckVisitor.checkTimeSeriesPermission(
                    auditEntity.getUsername(),
                    Collections.singletonList(node.getPath()),
                    PrivilegeType.READ_SCHEMA)
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitCreateAlignedTimeSeries(
      final CreateAlignedTimeSeriesNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitAlterTimeSeries(
      final AlterTimeSeriesNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitInternalCreateTimeSeries(
      final InternalCreateTimeSeriesNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitActivateTemplate(
      final ActivateTemplateNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitInternalBatchActivateTemplate(
      final InternalBatchActivateTemplateNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitBatchActivateTemplate(
      final BatchActivateTemplateNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitCreateLogicalView(
      final CreateLogicalViewNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitAlterLogicalView(
      final AlterLogicalViewNode node, final IAuditEntity auditEntity) {}

  @Override
  public Optional<PlanNode> visitDeleteData(
      final DeleteDataNode node, final IAuditEntity auditEntity) {}
}
