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

package org.apache.iotdb.db.mpp.plan.execution.memory;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanGraphPrinter;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class StatementMemorySourceVisitor
    extends StatementVisitor<StatementMemorySource, StatementMemorySourceContext> {

  @Override
  public StatementMemorySource visitNode(StatementNode node, StatementMemorySourceContext context) {
    return new StatementMemorySource(new TsBlock(0), new DatasetHeader(new ArrayList<>(), false));
  }

  @Override
  public StatementMemorySource visitExplain(
      ExplainStatement node, StatementMemorySourceContext context) {
    context.getAnalysis().setStatement(node.getQueryStatement());
    LogicalQueryPlan logicalPlan =
        new LogicalPlanner(context.getQueryContext(), new ArrayList<>())
            .plan(context.getAnalysis());
    DistributionPlanner planner = new DistributionPlanner(context.getAnalysis(), logicalPlan);
    PlanNode rootWithExchange = planner.addExchangeNode(planner.rewriteSource());
    List<String> lines =
        rootWithExchange.accept(new PlanGraphPrinter(), new PlanGraphPrinter.GraphContext());

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    lines.forEach(
        line -> {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(new Binary(line));
          builder.declarePosition();
        });
    TsBlock tsBlock = builder.build();
    DatasetHeader header =
        new DatasetHeader(
            Collections.singletonList(
                new ColumnHeader(IoTDBConstant.COLUMN_DISTRIBUTION_PLAN, TSDataType.TEXT)),
            true);
    return new StatementMemorySource(tsBlock, header);
  }

  @Override
  public StatementMemorySource visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, StatementMemorySourceContext context) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.showChildPathsHeader.getRespDataTypes());
    Set<String> matchedChildPaths = new TreeSet<>(context.getAnalysis().getMatchedNodes());
    matchedChildPaths.forEach(
        path -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(path));
          tsBlockBuilder.declarePosition();
        });
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, StatementMemorySourceContext context) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.showChildNodesHeader.getRespDataTypes());
    Set<String> matchedChildNodes = new TreeSet<>(context.getAnalysis().getMatchedNodes());
    matchedChildNodes.forEach(
        node -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(node));
          tsBlockBuilder.declarePosition();
        });
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitCountNodes(
      CountNodesStatement countStatement, StatementMemorySourceContext context) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.countNodesHeader.getRespDataTypes());
    Set<String> matchedChildNodes = new TreeSet<>(context.getAnalysis().getMatchedNodes());
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeInt(matchedChildNodes.size());
    tsBlockBuilder.declarePosition();
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }
}
