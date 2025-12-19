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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;

import org.apache.tsfile.utils.Pair;

import java.util.stream.Collectors;

public class PipeTreeStatementToPlanVisitor extends StatementVisitor<PlanNode, Void> {

  @Override
  public PlanNode visitNode(final StatementNode node, final Void context) {
    throw new UnsupportedOperationException(
        String.format(
            "PipeTreeStatementToPlanVisitor does not support visiting general statement, Statement: %s",
            node));
  }

  @Override
  public InternalCreateMultiTimeSeriesNode visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesStatement statement, final Void context) {
    // The value pair will not be used at the receiver
    return new InternalCreateMultiTimeSeriesNode(new PlanNodeId(""), statement.getDeviceMap());
  }

  @Override
  public BatchActivateTemplateNode visitBatchActivateTemplate(
      final BatchActivateTemplateStatement node, final Void context) {
    // The value pair will not be used at the receiver
    return new BatchActivateTemplateNode(
        new PlanNodeId(""),
        node.getDevicePathList().stream().collect(Collectors.toMap(k -> k, k -> new Pair<>(0, 0))));
  }
}
