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

package org.apache.iotdb.db.pipe.receiver;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.DeleteLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.AlterLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.DeleteLogicalViewStatement;

public class PipePlanToStatementVisitor extends PlanVisitor<Statement, Void> {

  @Override
  public Statement visitPlan(PlanNode node, Void context) {
    throw new UnsupportedOperationException(
        "PipePlanToStatementVisitor does not support visiting general plan.");
  }

  @Override
  public CreateTimeSeriesStatement visitCreateTimeSeries(CreateTimeSeriesNode node, Void context) {
    CreateTimeSeriesStatement statement = new CreateTimeSeriesStatement();
    statement.setPath(node.getPath());
    statement.setDataType(node.getDataType());
    statement.setEncoding(node.getEncoding());
    statement.setCompressor(node.getCompressor());
    statement.setProps(node.getProps());
    statement.setAttributes(node.getAttributes());
    statement.setAlias(node.getAlias());
    statement.setTags(node.getTags());
    return statement;
  }

  @Override
  public CreateMultiTimeSeriesStatement visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesNode node, Void context) {
    CreateMultiTimeSeriesStatement statement = new CreateMultiTimeSeriesStatement();
    return visitPlan(node, context);
  }

  @Override
  public AlterTimeSeriesStatement visitAlterTimeSeries(AlterTimeSeriesNode node, Void context) {
    AlterTimeSeriesStatement statement = new AlterTimeSeriesStatement();
    statement.setAlterMap(node.getAlterMap());
    statement.setAlterType(node.getAlterType());
    statement.setAttributesMap(node.getAttributesMap());
    statement.setAlias(node.getAlias());
    statement.setTagsMap(node.getTagsMap());
    statement.setPath(node.getPath());
    return statement;
  }

  @Override
  public InternalCreateTimeSeriesStatement visitInternalCreateTimeSeries(
      InternalCreateTimeSeriesNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public ActivateTemplateStatement visitActivateTemplate(ActivateTemplateNode node, Void context) {
    ActivateTemplateStatement statement = new ActivateTemplateStatement();
    statement.setPath(node.getActivatePath());
    return statement;
  }

  @Override
  public DeactivateTemplateStatement visitDeactivateTemplate(
      DeactivateTemplateNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public InternalBatchActivateTemplateStatement visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public InternalCreateMultiTimeSeriesStatement visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public DeleteTimeSeriesStatement visitDeleteTimeseries(DeleteTimeSeriesNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public BatchActivateTemplateStatement visitBatchActivateTemplate(
      BatchActivateTemplateNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public CreateLogicalViewStatement visitCreateLogicalView(
      CreateLogicalViewNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public DeleteLogicalViewStatement visitDeleteLogicalView(
      DeleteLogicalViewNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public AlterLogicalViewStatement visitAlterLogicalView(AlterLogicalViewNode node, Void context) {
    return visitPlan(node, context);
  }

  @Override
  public DeleteDataStatement visitDeleteData(DeleteDataNode node, Void context) {
    return visitPlan(node, context);
  }
}
