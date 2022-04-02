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

import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.meta.TimeSeriesMetaScanOperator;
import org.apache.iotdb.db.mpp.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesMetaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;

/**
 * used to plan a fragment instance. Currently, we simply change it from PlanNode to executable
 * Operator tree, but in the future, we may split one fragment instance into multiple pipeline to
 * run a fragment instance parallel and take full advantage of multi-cores
 */
public class LocalExecutionPlanner {

  /** This Visitor is responsible for transferring PlanNode Tree to Operator Tree */
  private class Visitor extends PlanVisitor<Operator, LocalExecutionPlanContext> {

    @Override
    public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
      throw new UnsupportedOperationException("should call the concrete visitXX() method");
    }

    @Override
    public Operator visitSeriesScan(SeriesScanNode node, LocalExecutionPlanContext context) {
      PartialPath seriesPath = node.getSeriesPath();
      boolean ascending = node.getScanOrder() == OrderBy.TIMESTAMP_ASC;
      OperatorContext operatorContext =
          context.taskContext.addOperatorContext(
              context.getNextOperatorId(), node.getId(), SeriesScanOperator.class.getSimpleName());
      // TODO should create QueryDataSource in SeriesScanOperator's runtime
      QueryDataSource dataSource = null;
      return new SeriesScanOperator(
          seriesPath,
          node.getAllSensors(),
          seriesPath.getSeriesType(),
          operatorContext,
          dataSource,
          node.getTimeFilter(),
          node.getValueFilter(),
          ascending);
    }

    @Override
    public Operator visitTimeSeriesMetaScan(
        TimeSeriesMetaScanNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.taskContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getId(),
              TimeSeriesMetaScanOperator.class.getSimpleName());
      return new TimeSeriesMetaScanOperator(
          operatorContext,
          node.getSchemaRegionReplicaSet().getSchemaRegionId(),
          node.getLimit(),
          node.getOffset(),
          node.getPath(),
          node.getKey(),
          node.getValue(),
          node.isContains(),
          node.isOrderByHeat(),
          node.isPrefixPath());
    }

    @Override
    public Operator visitSeriesAggregate(
        SeriesAggregateScanNode node, LocalExecutionPlanContext context) {
      return super.visitSeriesAggregate(node, context);
    }

    @Override
    public Operator visitDeviceMerge(DeviceMergeNode node, LocalExecutionPlanContext context) {
      return super.visitDeviceMerge(node, context);
    }

    @Override
    public Operator visitFill(FillNode node, LocalExecutionPlanContext context) {
      return super.visitFill(node, context);
    }

    @Override
    public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
      PlanNode child = node.getChild();

      QueryFilter filterExpression = node.getPredicate();
      List<String> outputSymbols = node.getOutputColumnNames();
      return super.visitFilter(node, context);
    }

    @Override
    public Operator visitFilterNull(FilterNullNode node, LocalExecutionPlanContext context) {
      return super.visitFilterNull(node, context);
    }

    @Override
    public Operator visitGroupByLevel(GroupByLevelNode node, LocalExecutionPlanContext context) {
      return super.visitGroupByLevel(node, context);
    }

    @Override
    public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
      Operator child = node.getChild().accept(this, context);
      return new LimitOperator(
          context.taskContext.addOperatorContext(
              context.getNextOperatorId(), node.getId(), LimitOperator.class.getSimpleName()),
          node.getLimit(),
          child);
    }

    @Override
    public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
      return super.visitOffset(node, context);
    }

    @Override
    public Operator visitRowBasedSeriesAggregate(
        AggregateNode node, LocalExecutionPlanContext context) {
      return super.visitRowBasedSeriesAggregate(node, context);
    }

    @Override
    public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
      return super.visitSort(node, context);
    }

    @Override
    public Operator visitTimeJoin(TimeJoinNode node, LocalExecutionPlanContext context) {
      return super.visitTimeJoin(node, context);
    }
  }

  private static class LocalExecutionPlanContext {
    private final FragmentInstanceContext taskContext;
    private int nextOperatorId = 0;

    public LocalExecutionPlanContext(FragmentInstanceContext taskContext) {
      this.taskContext = taskContext;
    }

    private int getNextOperatorId() {
      return nextOperatorId++;
    }
  }
}
