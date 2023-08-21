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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * <b>Optimization phase:</b> Distributed plan planning
 *
 * <p><b>Rule:</b> The LIMIT OFFSET condition can be pushed down to the SeriesScanNode, when the
 * following conditions are met:
 * <li>Time series query (not aggregation query).
 * <li>The query expressions are all scalar expression.
 * <li>Functions that need to be calculated based on before or after values are not used, such as
 *     trend functions, FILL(previous), FILL(linear).
 * <li>Only one scan node is included in the distributed plan. That is, only one single series or a
 *     group of series under an aligned device is queried, and all queried data is in one region.
 */
public class LimitOffsetPushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    if (queryStatement.isLastQuery()
        || queryStatement.isAggregationQuery()
        || (!queryStatement.hasLimit() && !queryStatement.hasOffset())) {
      return plan;
    }
    return plan.accept(new Rewriter(), new RewriterContext(analysis));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      for (PlanNode child : node.getChildren()) {
        context.setParent(node);
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitLimit(LimitNode node, RewriterContext context) {
      PlanNode parent = context.getParent();

      context.setParent(node);
      context.setLimit(node.getLimit());
      node.getChild().accept(this, context);

      if (context.isEnablePushDown()) {
        return concatParentWithChild(parent, node.getChild());
      }
      return node;
    }

    @Override
    public PlanNode visitOffset(OffsetNode node, RewriterContext context) {
      PlanNode parent = context.getParent();

      context.setParent(node);
      context.setOffset(node.getOffset());
      node.getChild().accept(this, context);

      if (context.isEnablePushDown()) {
        return concatParentWithChild(parent, node.getChild());
      }
      return node;
    }

    private PlanNode concatParentWithChild(PlanNode parent, PlanNode child) {
      if (parent != null) {
        ((SingleChildProcessNode) parent).setChild(child);
        return parent;
      } else {
        return child;
      }
    }

    @Override
    public PlanNode visitFill(FillNode node, RewriterContext context) {
      FillPolicy fillPolicy = node.getFillDescriptor().getFillPolicy();
      if (fillPolicy == FillPolicy.VALUE) {
        node.getChild().accept(this, context);
      } else {
        context.setEnablePushDown(false);
      }
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      // Value filtering push-down occurs during the logical planning phase. If there is still a
      // FilterNode here, it means that there are read filter conditions that cannot be pushed
      // down.
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitTransform(TransformNode node, RewriterContext context) {
      Expression[] outputExpressions = node.getOutputExpressions();
      boolean enablePushDown = true;
      for (Expression expression : outputExpressions) {
        if (!ExpressionAnalyzer.checkIsScalarExpression(expression, context.getAnalysis())) {
          enablePushDown = false;
          break;
        }
      }

      if (enablePushDown) {
        node.getChild().accept(this, context);
      } else {
        context.setEnablePushDown(false);
      }
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitDeviceView(DeviceViewNode node, RewriterContext context) {
      if (node.getChildren().size() == 1) {
        node.getChildren().get(0).accept(this, context);
        return node;
      } else {
        return visitMultiChildProcess(node, context);
      }
    }

    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, RewriterContext context) {
      if (context.isEnablePushDown()) {
        node.setLimit(context.getLimit());
        node.setOffset(context.getOffset());
      }
      return node;
    }

    @Override
    public PlanNode visitAlignedSeriesScan(AlignedSeriesScanNode node, RewriterContext context) {
      if (context.isEnablePushDown()) {
        node.setLimit(context.getLimit());
        node.setOffset(context.getOffset());
      }
      return node;
    }
  }

  private static class RewriterContext {
    private long limit;
    private long offset;

    private boolean enablePushDown = true;

    private PlanNode parent;

    private final Analysis analysis;

    public RewriterContext(Analysis analysis) {
      this.analysis = analysis;
    }

    public long getLimit() {
      return limit;
    }

    public void setLimit(long limit) {
      this.limit = limit;
    }

    public long getOffset() {
      return offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public boolean isEnablePushDown() {
      return enablePushDown;
    }

    public void setEnablePushDown(boolean enablePushDown) {
      this.enablePushDown = enablePushDown;
    }

    public PlanNode getParent() {
      return parent;
    }

    public void setParent(PlanNode parent) {
      this.parent = parent;
    }

    public Analysis getAnalysis() {
      return analysis;
    }
  }

  // following methods are used to push down limit/offset in group by time

  // 1. push down limit/offset to group by time in align by time

  public static boolean canPushDownLimitOffsetToGroupByTime(QueryStatement queryStatement) {
    if (queryStatement.isGroupByTime()
        && !queryStatement.isAlignByDevice()
        && !queryStatement.hasHaving()
        && !queryStatement.hasFill()) {
      return !queryStatement.hasOrderBy() || queryStatement.isOrderByBasedOnTime();
    }
    return false;
  }

  public static void pushDownLimitOffsetToTimeParameter(QueryStatement queryStatement) {
    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    long startTime = groupByTimeComponent.getStartTime();
    long endTime = groupByTimeComponent.getEndTime();
    long step = groupByTimeComponent.getSlidingStep();
    long interval = groupByTimeComponent.getInterval();

    long size = (endTime - startTime + step - 1) / step;
    if (size > queryStatement.getRowOffset()) {
      long limitSize = queryStatement.getRowLimit();
      long offsetSize = queryStatement.getRowOffset();
      if (queryStatement.getResultTimeOrder() == Ordering.ASC) {
        startTime = startTime + offsetSize * step;
      } else {
        startTime = startTime + (size - offsetSize - limitSize) * step;
      }
      endTime =
          limitSize == 0
              ? endTime
              : Math.min(endTime, startTime + (limitSize - 1) * step + interval);
      groupByTimeComponent.setEndTime(endTime);
      groupByTimeComponent.setStartTime(startTime);
    } else {
      // finish the query, resultSet is empty
      queryStatement.setResultSetEmpty(true);
    }
    queryStatement.setRowLimit(0);
    queryStatement.setRowOffset(0);
  }

  // 2. push down limit/offset to group by time in align by device
  public static boolean canPushDownLimitOffsetInGroupByTimeForDevice(
      QueryStatement queryStatement) {
    if (!hasLimitOffset(queryStatement)) {
      return false;
    }

    if (queryStatement.isGroupByTime()
        && queryStatement.isAlignByDevice()
        && !queryStatement.hasHaving()
        && !queryStatement.hasFill()) {
      return !queryStatement.hasOrderBy() || queryStatement.isOrderByBasedOnDevice();
    }
    return false;
  }

  public static List<PartialPath> pushDownLimitOffsetInGroupByTimeForDevice(
      List<PartialPath> deviceNames, QueryStatement queryStatement) {
    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    long startTime = groupByTimeComponent.getStartTime();
    long endTime = groupByTimeComponent.getEndTime();

    long size =
        (endTime - startTime + groupByTimeComponent.getSlidingStep() - 1)
            / groupByTimeComponent.getSlidingStep();
    if (size == 0 || size * deviceNames.size() <= queryStatement.getRowOffset()) {
      // resultSet is empty
      queryStatement.setResultSetEmpty(true);
      return deviceNames;
    }

    long limitSize = queryStatement.getRowLimit();
    long offsetSize = queryStatement.getRowOffset();
    List<PartialPath> optimizedDeviceNames = new ArrayList<>();
    int startDeviceIndex = (int) (offsetSize / size);
    int endDeviceIndex =
        limitSize == 0
            ? deviceNames.size() - 1
            : (int)
                ((limitSize - ((startDeviceIndex + 1) * size - offsetSize) + size - 1) / size
                    + startDeviceIndex);

    int index = 0;
    while (index < startDeviceIndex) {
      index++;
    }
    queryStatement.setRowOffset(offsetSize - startDeviceIndex * size);

    // if only refer to one device, optimize the time parameter
    if (startDeviceIndex == endDeviceIndex) {
      optimizedDeviceNames.add(deviceNames.get(startDeviceIndex));
      if (hasLimitOffset(queryStatement) && queryStatement.isOrderByTimeInDevices()) {
        pushDownLimitOffsetToTimeParameter(queryStatement);
      }
    } else {
      while (index <= endDeviceIndex && index < deviceNames.size()) {
        optimizedDeviceNames.add(deviceNames.get(index));
        index++;
      }
    }
    return optimizedDeviceNames;
  }

  private static boolean hasLimitOffset(QueryStatement queryStatement) {
    return queryStatement.hasLimit() || queryStatement.hasOffset();
  }
}
