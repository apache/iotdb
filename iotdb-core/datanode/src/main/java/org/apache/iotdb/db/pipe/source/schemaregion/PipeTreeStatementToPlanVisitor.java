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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.source.schemaregion;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;

import org.apache.tsfile.utils.Pair;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipeTreeStatementToPlanVisitor extends StatementVisitor<PlanNode, Void> {

  @Override
  public PlanNode visitNode(final StatementNode node, final Void context) {
    throw new UnsupportedOperationException(
        String.format("Unsupported statement in tree schema snapshot: %s", node));
  }

  @Override
  public InternalCreateMultiTimeSeriesNode visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesStatement statement, final Void context) {
    return new InternalCreateMultiTimeSeriesNode(new PlanNodeId(""), statement.getDeviceMap());
  }

  @Override
  public BatchActivateTemplateNode visitBatchActivateTemplate(
      final BatchActivateTemplateStatement statement, final Void context) {
    return new BatchActivateTemplateNode(
        new PlanNodeId(""),
        statement.getDevicePathList().stream()
            .collect(Collectors.toMap(path -> path, path -> new Pair<>(0, 0))));
  }

  @Override
  public CreateLogicalViewNode visitCreateLogicalView(
      final CreateLogicalViewStatement statement, final Void context) {
    final Map<PartialPath, ViewExpression> viewPaths = new LinkedHashMap<>();
    final List<PartialPath> targets = statement.getTargetPathList();
    final List<ViewExpression> expressions = statement.getViewExpressions();
    for (int index = 0; index < targets.size(); index++) {
      viewPaths.put(targets.get(index), expressions.get(index));
    }
    return new CreateLogicalViewNode(new PlanNodeId(""), viewPaths);
  }

  @Override
  public AlterTimeSeriesNode visitAlterTimeSeries(
      final AlterTimeSeriesStatement statement, final Void context) {
    return new AlterTimeSeriesNode(
        new PlanNodeId(""),
        statement.getPath(),
        statement.getAlterType(),
        statement.getAlterMap(),
        statement.getAlias(),
        statement.getTagsMap(),
        statement.getAttributesMap(),
        statement.isAlterView());
  }
}
