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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaUtils;
import org.apache.iotdb.db.utils.SchemaUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

public class AggregationPushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = analysis.getQueryStatement();
    if (!queryStatement.isAggregationQuery()
        || (queryStatement.isGroupBy() && !queryStatement.isGroupByTime())
        || cannotUseStatistics(queryStatement, analysis)) {
      return plan;
    }
    return plan.accept(
        new Rewriter(), new RewriterContext(analysis, context, queryStatement.isAlignByDevice()));
  }

  private boolean cannotUseStatistics(QueryStatement queryStatement, Analysis analysis) {
    boolean isAlignByDevice = queryStatement.isAlignByDevice();
    if (isAlignByDevice) {
      // check any of the devices
      String device = analysis.getDeviceList().get(0).toString();
      return cannotUseStatistics(
          analysis.getDeviceToAggregationExpressions().get(device),
          analysis.getDeviceToSourceTransformExpressions().get(device));
    } else {
      return cannotUseStatistics(
          analysis.getAggregationExpressions(), analysis.getSourceTransformExpressions());
    }
  }

  private boolean cannotUseStatistics(
      Set<Expression> aggregationExpressions, Set<Expression> sourceTransformExpressions) {
    for (Expression expression : aggregationExpressions) {

      if (expression instanceof FunctionExpression) {
        FunctionExpression functionExpression = (FunctionExpression) expression;
        // Disable statistics optimization of UDAF for now
        if (functionExpression.isExternalAggregationFunctionExpression()) {
          return true;
        }

        if (COUNT_TIME.equalsIgnoreCase(functionExpression.getFunctionName())) {
          String alignedDeviceId = "";
          for (Expression countTimeExpression : sourceTransformExpressions) {
            TimeSeriesOperand ts = (TimeSeriesOperand) countTimeExpression;
            if (!(ts.getPath() instanceof AlignedPath
                || ((MeasurementPath) ts.getPath()).isUnderAlignedEntity())) {
              return true;
            }
            if (StringUtils.isEmpty(alignedDeviceId)) {
              alignedDeviceId = ts.getPath().getDevice();
            } else if (!alignedDeviceId.equalsIgnoreCase(ts.getPath().getDevice())) {
              // count_time from only one aligned device can use AlignedSeriesAggScan
              return true;
            }
          }
          return false;
        }

        if (!BuiltinAggregationFunction.canUseStatistics(functionExpression.getFunctionName())) {
          return true;
        }
      } else {
        throw new IllegalArgumentException(
            String.format("Invalid Aggregation Expression: %s", expression.getExpressionString()));
      }
    }
    return false;
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      throw new IllegalArgumentException("Unexpected plan node: " + node);
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, RewriterContext context) {
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitRawDataAggregation(RawDataAggregationNode node, RewriterContext context) {
      PlanNode child = node.getChild();
      if (child instanceof FullOuterTimeJoinNode || child instanceof SeriesScanSourceNode) {
        boolean isSingleSource = child instanceof SeriesScanSourceNode;
        if (isSingleSource) {
          // only one source, check partition
        }

        List<AggregationDescriptor> aggregationDescriptorList = node.getAggregationDescriptorList();

        boolean needCheckAscending = node.getGroupByTimeParameter() == null;
        if (isSingleSource && ((SeriesScanSourceNode) child).getPushDownPredicate() != null) {
          needCheckAscending = false;
        }

        Map<PartialPath, List<AggregationDescriptor>> sourceToAscendingAggregationsMap =
            new HashMap<>();
        Map<PartialPath, List<AggregationDescriptor>> sourceToDescendingAggregationsMap =
            new HashMap<>();
        Map<PartialPath, List<AggregationDescriptor>> sourceToCountTimeAggregationsMap =
            new HashMap<>();
        for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
          checkArgument(
              aggregationDescriptor.getInputExpressions().size() == 1
                  && aggregationDescriptor.getInputExpressions().get(0)
                      instanceof TimeSeriesOperand);
          PartialPath path =
              ((TimeSeriesOperand) aggregationDescriptor.getInputExpressions().get(0)).getPath();
          if (aggregationDescriptor.getAggregationType().equals(TAggregationType.COUNT_TIME)) {
            sourceToCountTimeAggregationsMap
                .computeIfAbsent(path, key -> new ArrayList<>())
                .add(aggregationDescriptor);
          } else if (SchemaUtils.isConsistentWithScanOrder(
              aggregationDescriptor.getAggregationType(), node.getScanOrder())) {
            sourceToAscendingAggregationsMap
                .computeIfAbsent(path, key -> new ArrayList<>())
                .add(aggregationDescriptor);
          } else {
            sourceToDescendingAggregationsMap
                .computeIfAbsent(path, key -> new ArrayList<>())
                .add(aggregationDescriptor);
          }
        }

        List<PlanNode> sourceNodeList = new ArrayList<>();
        Map<PartialPath, List<AggregationDescriptor>> groupedSourceToAscendingAggregations;
        if (!sourceToCountTimeAggregationsMap.isEmpty()) {
          groupedSourceToAscendingAggregations = sourceToCountTimeAggregationsMap;
        } else {
          groupedSourceToAscendingAggregations =
              MetaUtils.groupAlignedAggregations(sourceToAscendingAggregationsMap);
        }
        for (Map.Entry<PartialPath, List<AggregationDescriptor>> sourceAggregationsEntry :
            groupedSourceToAscendingAggregations.entrySet()) {
          sourceNodeList.add(
              createAggregationScanNode(
                  sourceAggregationsEntry.getKey(),
                  sourceAggregationsEntry.getValue(),
                  node.getScanOrder(),
                  node.getGroupByTimeParameter(),
                  context));
        }
        if (needCheckAscending) {
          Map<PartialPath, List<AggregationDescriptor>> groupedSourceToDescendingAggregations =
              MetaUtils.groupAlignedAggregations(sourceToDescendingAggregationsMap);
          for (Map.Entry<PartialPath, List<AggregationDescriptor>> sourceAggregationsEntry :
              groupedSourceToDescendingAggregations.entrySet()) {
            sourceNodeList.add(
                createAggregationScanNode(
                    sourceAggregationsEntry.getKey(),
                    sourceAggregationsEntry.getValue(),
                    node.getScanOrder().reverse(),
                    null,
                    context));
          }
        }

        if (isSingleSource && ((SeriesScanSourceNode) child).getPushDownPredicate() != null) {
          Expression pushDownPredicate = ((SeriesScanSourceNode) child).getPushDownPredicate();
          sourceNodeList.forEach(
              sourceNode -> {
                ((SeriesAggregationSourceNode) sourceNode).setPushDownPredicate(pushDownPredicate);
              });
        }

        PlanNode resultNode = convergeWithTimeJoin(sourceNodeList, node.getScanOrder(), context);
        resultNode = planProject(resultNode, node, context);
        return resultNode;
      }
      // cannot push down
      return node;
    }

    private SeriesAggregationSourceNode createAggregationScanNode(
        PartialPath selectPath,
        List<AggregationDescriptor> aggregationDescriptorList,
        Ordering scanOrder,
        GroupByTimeParameter groupByTimeParameter,
        RewriterContext context) {
      if (selectPath instanceof MeasurementPath) { // non-aligned series
        return new SeriesAggregationScanNode(
            context.genPlanNodeId(),
            (MeasurementPath) selectPath,
            aggregationDescriptorList,
            scanOrder,
            groupByTimeParameter);
      } else if (selectPath instanceof AlignedPath) { // aligned series
        return new AlignedSeriesAggregationScanNode(
            context.genPlanNodeId(),
            (AlignedPath) selectPath,
            aggregationDescriptorList,
            scanOrder,
            groupByTimeParameter);
      } else {
        throw new IllegalArgumentException("unexpected path type");
      }
    }

    private PlanNode convergeWithTimeJoin(
        List<PlanNode> sourceNodes, Ordering mergeOrder, RewriterContext context) {
      PlanNode tmpNode;
      if (sourceNodes.size() == 1) {
        tmpNode = sourceNodes.get(0);
      } else {
        tmpNode = new FullOuterTimeJoinNode(context.genPlanNodeId(), mergeOrder, sourceNodes);
      }
      return tmpNode;
    }

    private PlanNode planProject(PlanNode resultNode, PlanNode rawNode, RewriterContext context) {
      if (context.isAlignByDevice()
          && !rawNode.getOutputColumnNames().equals(resultNode.getOutputColumnNames())) {
        return new ProjectNode(context.genPlanNodeId(), resultNode, rawNode.getOutputColumnNames());
      }
      return resultNode;
    }
  }

  private static class RewriterContext {

    private final QueryId queryId;
    private final boolean isAlignByDevice;

    public RewriterContext(Analysis analysis, MPPQueryContext context, boolean isAlignByDevice) {
      this.queryId = context.getQueryId();
      this.isAlignByDevice = isAlignByDevice;
    }

    public PlanNodeId genPlanNodeId() {
      return queryId.genPlanNodeId();
    }

    public boolean isAlignByDevice() {
      return isAlignByDevice;
    }
  }
}
