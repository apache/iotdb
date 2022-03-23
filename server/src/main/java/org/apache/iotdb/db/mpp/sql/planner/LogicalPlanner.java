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
package org.apache.iotdb.db.mpp.sql.planner;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.tree.StatementVisitor;

import java.util.List;

/** Generate a logical plan for the statement. */
public class LogicalPlanner {

  private final MPPQueryContext context;
  private final List<PlanOptimizer> optimizers;

  public LogicalPlanner(MPPQueryContext context, List<PlanOptimizer> optimizers) {
    this.context = context;
    this.optimizers = optimizers;
  }

  public LogicalQueryPlan plan(Analysis analysis) {
    PlanNode rootNode = new LogicalPlanVisitor().process(analysis.getStatement());

    // optimize the query logical plan
    if (analysis.getStatement() instanceof QueryStatement) {
      for (PlanOptimizer optimizer : optimizers) {
        rootNode = optimizer.optimize(rootNode, context);
      }
    }

    return new LogicalQueryPlan(context, rootNode);
  }

  /**
   * This visitor is used to generate a logical plan for the statement and returns the {@link
   * PlanNode}.
   */
  private static final class LogicalPlanVisitor
      extends StatementVisitor<PlanNode, MPPQueryContext> {

    @Override
    public PlanNode visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
      // TODO: Generate logical planNode tree for query statement
      return null;
    }

    @Override
    public PlanNode visitCreateTimeseries(
        CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
      return new CreateTimeSeriesNode(
          PlanNodeIdAllocator.generateId(),
          createTimeSeriesStatement.getPath(),
          createTimeSeriesStatement.getDataType(),
          createTimeSeriesStatement.getEncoding(),
          createTimeSeriesStatement.getCompressor(),
          createTimeSeriesStatement.getProps(),
          createTimeSeriesStatement.getTags(),
          createTimeSeriesStatement.getAttributes(),
          createTimeSeriesStatement.getAlias());
    }

    @Override
    public PlanNode visitInsertTablet(
        InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
      // TODO(INSERT) change the InsertTabletStatement to InsertTabletNode
      InsertTabletNode node = new InsertTabletNode(PlanNodeIdAllocator.generateId());

      return node;
    }
  }
}
