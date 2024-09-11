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
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanGenerator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.common.header.DatasetHeader.EMPTY_HEADER;
import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.apache.iotdb.db.queryengine.plan.execution.memory.StatementMemorySourceVisitor.getStatementMemorySource;

public class TableModelStatementMemorySourceVisitor
    extends AstVisitor<StatementMemorySource, TableModelStatementMemorySourceContext> {

  @Override
  public StatementMemorySource visitNode(
      final Node node, final TableModelStatementMemorySourceContext context) {
    final DatasetHeader datasetHeader = context.getAnalysis().getRespDatasetHeader();
    return new StatementMemorySource(
        new TsBlock(0), datasetHeader == null ? EMPTY_HEADER : datasetHeader);
  }

  @Override
  public StatementMemorySource visitExplain(
      final Explain node, final TableModelStatementMemorySourceContext context) {
    context.getAnalysis().setStatement(node.getStatement());
    final SymbolAllocator symbolAllocator = new SymbolAllocator();
    final DatasetHeader header =
        new DatasetHeader(
            Collections.singletonList(
                new ColumnHeader(IoTDBConstant.COLUMN_DISTRIBUTION_PLAN, TSDataType.TEXT)),
            true);
    final LogicalQueryPlan logicalPlan =
        new TableLogicalPlanner(
                context.getQueryContext(),
                LocalExecutionPlanner.getInstance().metadata,
                context.getQueryContext().getSession(),
                symbolAllocator,
                NOOP)
            .plan(context.getAnalysis());
    if (context.getAnalysis().isEmptyDataSource()) {
      return new StatementMemorySource(new TsBlock(0), header);
    }

    // Generate table model distributed plan
    final TableDistributedPlanGenerator.PlanContext planContext =
        new TableDistributedPlanGenerator.PlanContext();
    final PlanNode outputNodeWithExchange =
        new TableDistributedPlanner(context.getAnalysis(), symbolAllocator, logicalPlan)
            .generateDistributedPlanWithOptimize(planContext);

    final List<String> lines =
        outputNodeWithExchange.accept(
            new PlanGraphPrinter(),
            new PlanGraphPrinter.GraphContext(
                context.getQueryContext().getTypeProvider().getTemplatedInfo()));

    return getStatementMemorySource(header, lines);
  }

  @Override
  public StatementMemorySource visitShowDevice(
      final ShowDevice node, final TableModelStatementMemorySourceContext context) {
    return new StatementMemorySource(
        node.getTsBlock(context.getAnalysis()), node.getDataSetHeader());
  }

  @Override
  public StatementMemorySource visitCountDevice(
      final CountDevice node, final TableModelStatementMemorySourceContext context) {
    return new StatementMemorySource(
        node.getTsBlock(context.getAnalysis()), node.getDataSetHeader());
  }
}
