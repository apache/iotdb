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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.CollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowDiskUsageNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.apache.tsfile.utils.TimeDuration;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
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

  private static final BigInteger BIG_LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger BIG_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
  private static final BigInteger BIG_INTEGER_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
  private static final BigInteger BIG_INTEGER_MIN = BigInteger.valueOf(Integer.MIN_VALUE);

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    StatementType statementType = analysis.getTreeStatement().getType();
    if (statementType == StatementType.SHOW_DISK_USAGE) {
      return plan.accept(new Rewriter(), new RewriterContext(analysis));
    }
    if (statementType != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = analysis.getQueryStatement();
    if (queryStatement.isLastQuery()
        || queryStatement.isAggregationQuery()
        || (!queryStatement.hasLimit() && !queryStatement.hasOffset())) {
      return plan;
    }
    return plan.accept(new Rewriter(), new RewriterContext(analysis));
  }

  private static class Rewriter implements PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        context.setParent(node);
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitLimit(LimitNode node, RewriterContext context) {
      context.setParent(node);
      context.setLimit(node.getLimit());
      node.setChild(node.getChild().accept(this, context));

      if (context.isEnablePushDown()) {
        return node.getChild();
      }
      return node;
    }

    @Override
    public PlanNode visitOffset(OffsetNode node, RewriterContext context) {
      context.setParent(node);
      context.setOffset(node.getOffset());
      node.setChild(node.getChild().accept(this, context));

      if (context.isEnablePushDown()) {
        return node.getChild();
      }
      return node;
    }

    @Override
    public PlanNode visitFill(FillNode node, RewriterContext context) {
      FillPolicy fillPolicy = node.getFillDescriptor().getFillPolicy();
      if (fillPolicy == FillPolicy.CONSTANT) {
        node.setChild(node.getChild().accept(this, context));
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
    public PlanNode visitSort(SortNode node, RewriterContext context) {
      // Limit/Offset in ORDER BY focus the sorted result, so it should not be pushed down.
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
        node.setChild(node.getChild().accept(this, context));
      } else {
        context.setEnablePushDown(false);
      }
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      if (node.getChildren().size() == 1) {
        PlanNode child = node.getChildren().get(0).accept(this, context);
        node.setChildren(Collections.singletonList(child));
        return node;
      }
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitCollect(CollectNode node, RewriterContext context) {
      PlanNode newNode = node.clone();
      RewriterContext subContext = new RewriterContext(context.getAnalysis());
      if (context.getLimit() > 0) {
        subContext.setLimit(context.getLimit() + context.getOffset());
      }
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, subContext));
      }

      // If the CollectNode has more than one child, the final result is produced
      // by merging multiple data streams. In this case, LIMIT/OFFSET cannot be
      // fully pushed down to all children because the global ordering or final
      // row count must still be enforced at the parent level.
      // Therefore, the parent LIMIT/OFFSET node should be preserved.
      if (node.getChildren().size() > 1) {
        context.setEnablePushDown(false);
      }
      return newNode;
    }

    @Override
    public PlanNode visitTwoChildProcess(TwoChildProcessNode node, RewriterContext context) {
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitLeftOuterTimeJoin(LeftOuterTimeJoinNode node, RewriterContext context) {
      // TODO we may need to push limit and offset to left child
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, RewriterContext context) {
      if (context.isEnablePushDown()) {
        node.setPushDownLimit(context.getLimit());
        node.setPushDownOffset(context.getOffset());
      }
      return node;
    }

    @Override
    public PlanNode visitAlignedSeriesScan(AlignedSeriesScanNode node, RewriterContext context) {
      if (context.isEnablePushDown()) {
        node.setPushDownLimit(context.getLimit());
        node.setPushDownOffset(context.getOffset());
      }
      return node;
    }

    @Override
    public PlanNode visitShowDiskUsage(ShowDiskUsageNode node, RewriterContext context) {
      if (context.isEnablePushDown()) {
        node.setPushDownLimit(context.getLimit());
        node.setPushDownOffset(context.getOffset());
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

  private static void pushDownLimitOffsetToTimeParameterContainingMonth(
      QueryStatement queryStatement, ZoneId zoneId) {
    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    long startTime = groupByTimeComponent.getStartTime();
    long endTime = groupByTimeComponent.getEndTime();
    TimeDuration slidingStep = groupByTimeComponent.getSlidingStep();
    TimeDuration interval = groupByTimeComponent.getInterval();
    long limitSize = queryStatement.getRowLimit();
    long offsetSize = queryStatement.getRowOffset();

    // Evaluate the day of month as 28 days
    long totalStep = slidingStep.getMinTotalDuration(TimeUnit.MILLISECONDS);
    BigInteger size = ceilDivTimeRange(startTime, endTime, totalStep);
    if (size.compareTo(BigInteger.valueOf(offsetSize)) > 0) {
      TimeZone timeZone = TimeZone.getTimeZone(zoneId);
      // ordering in group by month must be ascending
      long newStartTime =
          DateTimeUtils.calcPositiveIntervalByMonth(
              startTime, slidingStep.multiple(offsetSize), zoneId);

      if (limitSize != 0) {
        endTime =
            Math.min(
                endTime,
                DateTimeUtils.calcPositiveIntervalByMonth(
                    startTime,
                    calculateEndTimeDuration(slidingStep, interval, limitSize, offsetSize),
                    zoneId));
      }
      groupByTimeComponent.setEndTime(endTime);
      groupByTimeComponent.setStartTime(newStartTime);
    } else {
      // finish the query, resultSet is empty
      queryStatement.setResultSetEmpty(true);
    }
    // If windows overlap, we need to keep LIMIT because the window size can be less than interval
    // which may result in more windows than we need in the target time range.
    queryStatement.setRowLimit(interval.isGreaterThan(slidingStep) ? limitSize : 0);
    queryStatement.setRowOffset(0);
  }

  private static TimeDuration calculateEndTimeDuration(
      TimeDuration slidingStep, TimeDuration interval, long limitSize, long offsetSize) {
    BigInteger length =
        BigInteger.valueOf(offsetSize).add(BigInteger.valueOf(limitSize)).subtract(BigInteger.ONE);
    // startTime + offsetSize * step + (limitSize - 1) * step + interval
    int monthDuration =
        saturateToInt(
            length
                .multiply(BigInteger.valueOf(slidingStep.monthDuration))
                .add(BigInteger.valueOf(interval.monthDuration)));
    long nonMonthDuration =
        saturateToLong(
            length
                .multiply(BigInteger.valueOf(slidingStep.nonMonthDuration))
                .add(BigInteger.valueOf(interval.nonMonthDuration)));
    return new TimeDuration(monthDuration, nonMonthDuration);
  }

  public static void pushDownLimitOffsetToTimeParameter(
      QueryStatement queryStatement, ZoneId zoneId) {
    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    // if group by time contains month, we use another push down limit/offset
    if (groupByTimeComponent.getInterval().containsMonth()
        || groupByTimeComponent.getSlidingStep().containsMonth()) {
      pushDownLimitOffsetToTimeParameterContainingMonth(queryStatement, zoneId);
      return;
    }
    long startTime = groupByTimeComponent.getStartTime();
    long endTime = groupByTimeComponent.getEndTime();
    long step = groupByTimeComponent.getSlidingStep().nonMonthDuration;
    long interval = groupByTimeComponent.getInterval().nonMonthDuration;
    long limitSize = queryStatement.getRowLimit();
    long offsetSize = queryStatement.getRowOffset();
    BigInteger size = ceilDivTimeRange(startTime, endTime, step);
    if (size.compareTo(BigInteger.valueOf(offsetSize)) > 0) {
      if (queryStatement.getResultTimeOrder() == Ordering.ASC) {
        startTime = addTimeDuration(startTime, BigInteger.valueOf(offsetSize), step, 0);
      } else {
        BigInteger startTimeInterval =
            size.subtract(BigInteger.valueOf(offsetSize)).subtract(BigInteger.valueOf(limitSize));
        startTime =
            addTimeDuration(
                startTime,
                startTimeInterval.signum() < 0 ? BigInteger.ZERO : startTimeInterval,
                step,
                0);
      }
      endTime =
          limitSize == 0
              ? endTime
              : Math.min(
                  endTime,
                  addTimeDuration(startTime, BigInteger.valueOf(limitSize - 1), step, interval));
      groupByTimeComponent.setEndTime(endTime);
      groupByTimeComponent.setStartTime(startTime);
    } else {
      // finish the query, resultSet is empty
      queryStatement.setResultSetEmpty(true);
    }
    // If windows overlap, we need to keep LIMIT because the window size can be less than interval
    // which may result in more windows than we need in the target time range.
    queryStatement.setRowLimit(interval > step ? limitSize : 0);
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
      List<PartialPath> deviceNames, QueryStatement queryStatement, ZoneId zoneId) {
    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    if (groupByTimeComponent.getInterval().containsMonth()
        || groupByTimeComponent.getSlidingStep().containsMonth()) {
      return deviceNames;
    }
    long startTime = groupByTimeComponent.getStartTime();
    long endTime = groupByTimeComponent.getEndTime();
    long slidingStep = groupByTimeComponent.getSlidingStep().nonMonthDuration;
    BigInteger size = ceilDivTimeRange(startTime, endTime, slidingStep);
    if (size.signum() == 0
        || size.multiply(BigInteger.valueOf(deviceNames.size()))
                .compareTo(BigInteger.valueOf(queryStatement.getRowOffset()))
            <= 0) {
      // resultSet is empty
      queryStatement.setResultSetEmpty(true);
      return deviceNames;
    }

    long limitSize = queryStatement.getRowLimit();
    long offsetSize = queryStatement.getRowOffset();
    List<PartialPath> optimizedDeviceNames = new ArrayList<>();
    int startDeviceIndex = saturateToInt(BigInteger.valueOf(offsetSize).divide(size));
    int endDeviceIndex =
        limitSize == 0
            ? deviceNames.size() - 1
            : calculateEndDeviceIndex(size, limitSize, offsetSize, startDeviceIndex);

    int index = 0;
    while (index < startDeviceIndex) {
      index++;
    }
    queryStatement.setRowOffset(
        saturateToLong(
            BigInteger.valueOf(offsetSize)
                .subtract(BigInteger.valueOf(startDeviceIndex).multiply(size))));

    // if only refer to one device, optimize the time parameter
    if (startDeviceIndex == endDeviceIndex) {
      optimizedDeviceNames.add(deviceNames.get(startDeviceIndex));
      if (hasLimitOffset(queryStatement) && queryStatement.isOrderByTimeInDevices()) {
        pushDownLimitOffsetToTimeParameter(queryStatement, zoneId);
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

  private static BigInteger ceilDivTimeRange(long startTime, long endTime, long divisor) {
    return BigInteger.valueOf(endTime)
        .subtract(BigInteger.valueOf(startTime))
        .add(BigInteger.valueOf(divisor).subtract(BigInteger.ONE))
        .divide(BigInteger.valueOf(divisor));
  }

  private static long addTimeDuration(
      long startTime, BigInteger stepCount, long step, long interval) {
    return saturateToLong(
        BigInteger.valueOf(startTime)
            .add(stepCount.multiply(BigInteger.valueOf(step)))
            .add(BigInteger.valueOf(interval)));
  }

  private static int calculateEndDeviceIndex(
      BigInteger size, long limitSize, long offsetSize, int startDeviceIndex) {
    BigInteger firstDeviceRemaining =
        BigInteger.valueOf(startDeviceIndex + 1L)
            .multiply(size)
            .subtract(BigInteger.valueOf(offsetSize));
    return saturateToInt(
        BigInteger.valueOf(limitSize)
            .subtract(firstDeviceRemaining)
            .add(size)
            .subtract(BigInteger.ONE)
            .divide(size)
            .add(BigInteger.valueOf(startDeviceIndex)));
  }

  private static long saturateToLong(BigInteger value) {
    if (value.compareTo(BIG_LONG_MAX) > 0) {
      return Long.MAX_VALUE;
    }
    if (value.compareTo(BIG_LONG_MIN) < 0) {
      return Long.MIN_VALUE;
    }
    return value.longValue();
  }

  private static int saturateToInt(BigInteger value) {
    if (value.compareTo(BIG_INTEGER_MAX) > 0) {
      return Integer.MAX_VALUE;
    }
    if (value.compareTo(BIG_INTEGER_MIN) < 0) {
      return Integer.MIN_VALUE;
    }
    return value.intValue();
  }
}
