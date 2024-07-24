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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.AddExchangeNodes;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.DistributedPlanGenerator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushLimitOffsetIntoTableScan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SortElimination;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.common.header.DatasetHeader.EMPTY_HEADER;
import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.apache.iotdb.db.queryengine.plan.execution.memory.StatementMemorySourceVisitor.getStatementMemorySource;

public class TableModelStatementMemorySourceVisitor
    extends AstVisitor<StatementMemorySource, TableModelStatementMemorySourceContext> {

  @Override
  public StatementMemorySource visitNode(
      Node node, TableModelStatementMemorySourceContext context) {
    DatasetHeader datasetHeader = context.getAnalysis().getRespDatasetHeader();
    return new StatementMemorySource(
        new TsBlock(0), datasetHeader == null ? EMPTY_HEADER : datasetHeader);
  }

  @Override
  public StatementMemorySource visitExplain(
      Explain node, TableModelStatementMemorySourceContext context) {
    context.getAnalysis().setStatement(node.getStatement());
    DatasetHeader header =
        new DatasetHeader(
            Collections.singletonList(
                new ColumnHeader(IoTDBConstant.COLUMN_DISTRIBUTION_PLAN, TSDataType.TEXT)),
            true);
    LogicalQueryPlan logicalPlan =
        new org.apache.iotdb.db.queryengine.plan.relational.planner.LogicalPlanner(
                context.getQueryContext(),
                LocalExecutionPlanner.getInstance().metadata,
                context.getQueryContext().getSession(),
                WarningCollector.NOOP)
            .plan(context.getAnalysis());
    if (context.getAnalysis().isEmptyDataSource()) {
      return new StatementMemorySource(new TsBlock(0), header);
    }

    // generate table model distributed plan
    DistributedPlanGenerator.PlanContext planContext = new DistributedPlanGenerator.PlanContext();
    List<PlanNode> distributedPlanResult =
        new DistributedPlanGenerator(context.getQueryContext(), context.getAnalysis())
            .genResult(logicalPlan.getRootNode(), planContext);
    checkArgument(distributedPlanResult.size() == 1, "Root node must return only one");

    // Notice: when change the optimizers in TableDistributionPlanner, these code also need to be
    // adapted
    List<PlanOptimizer> optimizers =
        Arrays.asList(new PushLimitOffsetIntoTableScan(), new SortElimination());
    // distribute plan optimize rule
    PlanNode distributedPlan = distributedPlanResult.get(0);
    for (PlanOptimizer optimizer : optimizers) {
      distributedPlan =
          optimizer.optimize(
              distributedPlan,
              new PlanOptimizer.Context(
                  null,
                  context.getAnalysis(),
                  null,
                  context.getQueryContext(),
                  context.getQueryContext().getTypeProvider(),
                  new SymbolAllocator(),
                  context.getQueryContext().getQueryId(),
                  NOOP,
                  PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));
    }

    // add exchange node for distributed plan
    PlanNode outputNodeWithExchange =
        new AddExchangeNodes(context.getQueryContext())
            .addExchangeNodes(distributedPlan, planContext);

    List<String> lines =
        outputNodeWithExchange.accept(
            new PlanGraphPrinter(),
            new PlanGraphPrinter.GraphContext(
                context.getQueryContext().getTypeProvider().getTemplatedInfo()));

    return getStatementMemorySource(header, lines);
  }
}
