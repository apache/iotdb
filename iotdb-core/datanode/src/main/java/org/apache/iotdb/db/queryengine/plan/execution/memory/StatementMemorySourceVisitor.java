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

package org.apache.iotdb.db.queryengine.plan.execution.memory;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowCurrentTimestampStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowVersionStatement;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.common.header.DatasetHeader.EMPTY_HEADER;

public class StatementMemorySourceVisitor
    extends StatementVisitor<StatementMemorySource, StatementMemorySourceContext> {

  @Override
  public StatementMemorySource visitNode(StatementNode node, StatementMemorySourceContext context) {
    DatasetHeader datasetHeader = context.getAnalysis().getRespDatasetHeader();
    return new StatementMemorySource(
        new TsBlock(0), datasetHeader == null ? EMPTY_HEADER : datasetHeader);
  }

  private boolean sourceNotExist(StatementMemorySourceContext context) {
    return (context.getAnalysis().getSourceExpressions() == null
            || context.getAnalysis().getSourceExpressions().isEmpty())
        && (context.getAnalysis().getDeviceToSourceExpressions() == null
            || context.getAnalysis().getDeviceToSourceExpressions().isEmpty())
        && context.getAnalysis().getDeviceTemplate() == null;
  }

  @Override
  public StatementMemorySource visitExplain(
      ExplainStatement node, StatementMemorySourceContext context) {
    context.getAnalysis().setRealStatement(node.getQueryStatement());
    DatasetHeader header =
        new DatasetHeader(
            Collections.singletonList(
                new ColumnHeader(IoTDBConstant.COLUMN_DISTRIBUTION_PLAN, TSDataType.TEXT)),
            true);
    if (sourceNotExist(context)) {
      return new StatementMemorySource(new TsBlock(0), header);
    }
    LogicalQueryPlan logicalPlan =
        new LogicalPlanner(context.getQueryContext()).plan(context.getAnalysis());
    DistributionPlanner planner = new DistributionPlanner(context.getAnalysis(), logicalPlan);
    PlanNode rootWithExchange = planner.addExchangeNode(planner.rewriteSource());
    PlanNode optimizedRootWithExchange = planner.optimize(rootWithExchange);

    List<String> lines =
        optimizedRootWithExchange.accept(
            new PlanGraphPrinter(),
            new PlanGraphPrinter.GraphContext(
                context.getQueryContext().getTypeProvider().getTemplatedInfo()));

    return getStatementMemorySource(header, lines);
  }

  static StatementMemorySource getStatementMemorySource(DatasetHeader header, List<String> lines) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    lines.forEach(
        line -> {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(new Binary(line, TSFileConfig.STRING_CHARSET));
          builder.declarePosition();
        });
    TsBlock tsBlock = builder.build();

    return new StatementMemorySource(tsBlock, header);
  }

  @Override
  public StatementMemorySource visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, StatementMemorySourceContext context) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showChildPathsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    Set<TSchemaNode> matchedChildPaths = context.getAnalysis().getMatchedNodes();

    // sort by node type
    Set<TSchemaNode> sortSet =
        new TreeSet<>(
            (o1, o2) -> {
              if (o1.getNodeType() == o2.getNodeType()) {
                return o1.getNodeName().compareTo(o2.getNodeName());
              }
              return o1.getNodeType() - o2.getNodeType();
            });
    sortSet.addAll(matchedChildPaths);
    sortSet.forEach(
        node -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder
              .getColumnBuilder(0)
              .writeBinary(new Binary(node.getNodeName(), TSFileConfig.STRING_CHARSET));
          tsBlockBuilder
              .getColumnBuilder(1)
              .writeBinary(
                  new Binary(
                      MNodeType.getMNodeType(node.getNodeType()).getNodeTypeName(),
                      TSFileConfig.STRING_CHARSET));
          tsBlockBuilder.declarePosition();
        });
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, StatementMemorySourceContext context) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showChildNodesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    Set<String> matchedChildNodes =
        context.getAnalysis().getMatchedNodes().stream()
            .map(TSchemaNode::getNodeName)
            .collect(Collectors.toCollection(TreeSet::new));
    matchedChildNodes.forEach(
        node -> {
          try {
            PartialPath nodePath = new PartialPath(node);
            String nodeName = nodePath.getTailNode();
            tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
            tsBlockBuilder
                .getColumnBuilder(0)
                .writeBinary(new Binary(nodeName, TSFileConfig.STRING_CHARSET));
            tsBlockBuilder.declarePosition();
          } catch (IllegalPathException ignored) {
            // definitely won't happen
          }
        });
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitShowVersion(
      ShowVersionStatement showVersionStatement, StatementMemorySourceContext context) {

    return new StatementMemorySource(
        getVersionResult(), context.getAnalysis().getRespDatasetHeader());
  }

  public static TsBlock getVersionResult() {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showVersionColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder
        .getColumnBuilder(0)
        .writeBinary(new Binary(IoTDBConstant.VERSION, TSFileConfig.STRING_CHARSET));
    tsBlockBuilder
        .getColumnBuilder(1)
        .writeBinary(new Binary(IoTDBConstant.BUILD_INFO, TSFileConfig.STRING_CHARSET));
    tsBlockBuilder.declarePosition();
    return tsBlockBuilder.build();
  }

  @Override
  public StatementMemorySource visitCountNodes(
      CountNodesStatement countStatement, StatementMemorySourceContext context) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.countNodesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    Set<String> matchedChildNodes =
        context.getAnalysis().getMatchedNodes().stream()
            .map(TSchemaNode::getNodeName)
            .collect(Collectors.toSet());
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(matchedChildNodes.size());
    tsBlockBuilder.declarePosition();
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitCountDevices(
      CountDevicesStatement countStatement, StatementMemorySourceContext context) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.countDevicesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(0);
    tsBlockBuilder.declarePosition();
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitCountTimeSeries(
      CountTimeSeriesStatement countStatement, StatementMemorySourceContext context) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.countTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(0);
    tsBlockBuilder.declarePosition();
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement,
      StatementMemorySourceContext context) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showPathsUsingTemplateHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitShowCurrentTimestamp(
      ShowCurrentTimestampStatement showCurrentTimestampStatement,
      StatementMemorySourceContext context) {

    return new StatementMemorySource(
        getCurrentTimestampResult(), context.getAnalysis().getRespDatasetHeader());
  }

  public static TsBlock getCurrentTimestampResult() {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.SHOW_CURRENT_TIMESTAMP_COLUMN_HEADERS.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(System.currentTimeMillis());
    tsBlockBuilder.declarePosition();
    return tsBlockBuilder.build();
  }
}
