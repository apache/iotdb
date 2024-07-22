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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaUtils;
import org.apache.iotdb.db.utils.SchemaUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

public class AggregationPushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getTreeStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = analysis.getQueryStatement();
    if (!queryStatement.isAggregationQuery()
        || (queryStatement.isGroupBy() && !queryStatement.isGroupByTime())
        || cannotUseStatistics(queryStatement, analysis)) {
      return plan;
    }

    RewriterContext rewriterContext =
        new RewriterContext(analysis, context, queryStatement.isAlignByDevice());
    PlanNode node;

    node = plan.accept(new Rewriter(), rewriterContext);

    return node;
  }

  private boolean cannotUseStatistics(QueryStatement queryStatement, Analysis analysis) {
    boolean isAlignByDevice = queryStatement.isAlignByDevice();
    if (isAlignByDevice) {
      if (analysis.allDevicesInOneTemplate()) {
        return cannotUseStatisticsForTemplate(analysis.getAggregationExpressions());
      }

      // check any of the devices
      IDeviceID device = analysis.getDeviceList().get(0).getIDeviceIDAsFullDevice();
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
              alignedDeviceId = ts.getPath().getDeviceString();
            } else if (!alignedDeviceId.equalsIgnoreCase(ts.getPath().getDeviceString())) {
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

  private boolean cannotUseStatisticsForTemplate(Set<Expression> aggregationExpressions) {

    for (Expression expression : aggregationExpressions) {
      if (expression instanceof FunctionExpression) {
        FunctionExpression functionExpression = (FunctionExpression) expression;
        // Disable statistics optimization of UDAF for now
        if (functionExpression.isExternalAggregationFunctionExpression()) {
          return true;
        }

        // in template align by device query, device must be aligned
        if (COUNT_TIME.equalsIgnoreCase(functionExpression.getFunctionName())) {
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
    public PlanNode visitDeviceView(DeviceViewNode node, RewriterContext context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (int i = 0; i < node.getDevices().size(); i++) {
        context.setCurDevice(node.getDevices().get(i));
        if (context.analysis.allDevicesInOneTemplate()) {
          context.setCurDevicePath(context.analysis.getDeviceList().get(i));
        }

        rewrittenChildren.add(node.getChildren().get(i).accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitSingleDeviceView(SingleDeviceViewNode node, RewriterContext context) {
      context.setCurDevice(node.getDevice());
      try {
        context.setCurDevicePath(new PartialPath(node.getDevice()));
      } catch (IllegalPathException e) {
        throw new IllegalStateException(
            String.format(
                "Illegal device path: %s in AggregationPushDown rule.", node.getDevice()));
      }
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitGroupByLevel(GroupByLevelNode node, RewriterContext context) {
      checkState(
          node.getChildren().size() == 1
              && (node.getChildren().get(0) instanceof RawDataAggregationNode
                  || node.getChildren().get(0) instanceof SlidingWindowAggregationNode));

      PlanNode child = node.getChildren().get(0);
      PlanNode rewrittenChild = child.accept(this, context);
      if (rewrittenChild instanceof FullOuterTimeJoinNode) {
        // aggregation all push down, converge with GroupByLevelNode directly
        node.setChildren(rewrittenChild.getChildren());
      } else {
        node.setChildren(Collections.singletonList(rewrittenChild));
      }
      return node;
    }

    @Override
    public PlanNode visitGroupByTag(GroupByTagNode node, RewriterContext context) {
      checkState(
          node.getChildren().size() == 1
              && (node.getChildren().get(0) instanceof RawDataAggregationNode
                  || node.getChildren().get(0) instanceof SlidingWindowAggregationNode));

      PlanNode child = node.getChildren().get(0);
      PlanNode rewrittenChild = child.accept(this, context);
      if (rewrittenChild instanceof FullOuterTimeJoinNode) {
        // aggregation all push down, converge with GroupByTagNode directly
        node.setChildren(rewrittenChild.getChildren());
      } else {
        node.setChildren(Collections.singletonList(rewrittenChild));
      }
      return node;
    }

    @Override
    public PlanNode visitRawDataAggregation(RawDataAggregationNode node, RewriterContext context) {
      if (context.analysis.allDevicesInOneTemplate()) {
        return visitRawDataAggregationTemplateCase(node, context);
      }

      PlanNode child = node.getChild();
      if (child instanceof ProjectNode) {
        // remove ProjectNode
        node.setChild(((ProjectNode) child).getChild());
        return visitRawDataAggregation(node, context);
      }
      if (child instanceof FullOuterTimeJoinNode || child instanceof SeriesScanSourceNode) {
        boolean isSingleSource = child instanceof SeriesScanSourceNode;
        boolean needCheckAscending = node.getGroupByTimeParameter() == null;
        if (isSingleSource && ((SeriesScanSourceNode) child).getPushDownPredicate() != null) {
          Expression pushDownPredicate = ((SeriesScanSourceNode) child).getPushDownPredicate();
          if (!PredicateUtils.predicateCanPushIntoScan(pushDownPredicate)) {
            // don't push down, simplify the BE side logic
            return node;
          }
        }

        Map<PartialPath, List<AggregationDescriptor>> sourceToAscendingAggregationsMap =
            new HashMap<>();
        Map<PartialPath, List<AggregationDescriptor>> sourceToDescendingAggregationsMap =
            new HashMap<>();
        Map<PartialPath, List<AggregationDescriptor>> sourceToCountTimeAggregationsMap =
            new HashMap<>();

        AggregationStep curStep = node.getAggregationDescriptorList().get(0).getStep();
        Set<Expression> aggregationExpressions = context.getAggregationExpressions();
        for (Expression aggregationExpression : aggregationExpressions) {
          createAggregationDescriptor(
              (FunctionExpression) aggregationExpression,
              curStep,
              node.getScanOrder(),
              needCheckAscending,
              sourceToAscendingAggregationsMap,
              sourceToDescendingAggregationsMap,
              sourceToCountTimeAggregationsMap);
        }

        List<PlanNode> sourceNodeList =
            constructSourceNodeFromAggregationsDescriptors(
                sourceToAscendingAggregationsMap,
                sourceToDescendingAggregationsMap,
                sourceToCountTimeAggregationsMap,
                node.getScanOrder(),
                node.getGroupByTimeParameter(),
                context);

        if (isSingleSource && ((SeriesScanSourceNode) child).getPushDownPredicate() != null) {
          Expression pushDownPredicate = ((SeriesScanSourceNode) child).getPushDownPredicate();
          sourceNodeList.forEach(
              sourceNode -> {
                SeriesAggregationSourceNode aggregationSourceNode =
                    (SeriesAggregationSourceNode) sourceNode;
                aggregationSourceNode.setPushDownPredicate(pushDownPredicate);
                if (aggregationSourceNode instanceof AlignedSeriesAggregationScanNode) {
                  ((AlignedSeriesAggregationScanNode) aggregationSourceNode)
                      .setAlignedPath(((AlignedSeriesScanNode) child).getAlignedPath());
                }
              });
        }

        PlanNode resultNode = convergeWithTimeJoin(sourceNodeList, node.getScanOrder(), context);
        resultNode = planProject(resultNode, node, context);

        // After pushing down the predicate, the original scan nodes are no longer needed, we should
        // release the memory that they occupied.
        context.releaseMemoryForFrontEnd(getRamBytesUsedOfOldScanNodes(child));
        return resultNode;
      }
      // cannot push down
      return node;
    }

    private long getRamBytesUsedOfOldScanNodes(final PlanNode node) {
      if (node == null) {
        return 0L;
      }
      if (node instanceof SeriesScanSourceNode) {
        SeriesScanSourceNode scanNode = (SeriesScanSourceNode) node;
        return scanNode.ramBytesUsed();
      } else if (node instanceof FullOuterTimeJoinNode) {
        return node.getChildren().stream().mapToLong(this::getRamBytesUsedOfOldScanNodes).sum();
      }
      return 0L;
    }

    private void createAggregationDescriptor(
        FunctionExpression sourceExpression,
        AggregationStep curStep,
        Ordering scanOrder,
        boolean needCheckAscending,
        Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations,
        Map<PartialPath, List<AggregationDescriptor>> descendingAggregations,
        Map<PartialPath, List<AggregationDescriptor>> countTimeAggregations) {
      AggregationDescriptor aggregationDescriptor =
          new AggregationDescriptor(
              sourceExpression.getFunctionName(),
              curStep,
              sourceExpression.getExpressions(),
              sourceExpression.getFunctionAttributes());

      if (COUNT_TIME.equalsIgnoreCase(sourceExpression.getFunctionName())) {
        Map<String, Pair<List<String>, List<IMeasurementSchema>>> map = new HashMap<>();
        for (Expression expression : sourceExpression.getCountTimeExpressions()) {
          TimeSeriesOperand ts = (TimeSeriesOperand) expression;
          PartialPath path = ts.getPath();
          Pair<List<String>, List<IMeasurementSchema>> pair =
              map.computeIfAbsent(
                  path.getDeviceString(), k -> new Pair<>(new ArrayList<>(), new ArrayList<>()));
          pair.left.add(path.getMeasurement());
          try {
            pair.right.add(path.getMeasurementSchema());
          } catch (MetadataException ex) {
            throw new RuntimeException(ex);
          }
        }

        for (Map.Entry<String, Pair<List<String>, List<IMeasurementSchema>>> entry :
            map.entrySet()) {
          String device = entry.getKey();
          Pair<List<String>, List<IMeasurementSchema>> pair = entry.getValue();
          AlignedPath alignedPath;
          try {
            alignedPath = new AlignedPath(device, pair.left, pair.right);
          } catch (IllegalPathException e) {
            throw new RuntimeException(e);
          }
          countTimeAggregations.put(alignedPath, Collections.singletonList(aggregationDescriptor));
        }

        return;
      }

      PartialPath selectPath =
          ((TimeSeriesOperand) sourceExpression.getExpressions().get(0)).getPath();
      if (!needCheckAscending
          || SchemaUtils.isConsistentWithScanOrder(
              aggregationDescriptor.getAggregationType(), scanOrder)) {
        ascendingAggregations
            .computeIfAbsent(selectPath, key -> new ArrayList<>())
            .add(aggregationDescriptor);
      } else {
        descendingAggregations
            .computeIfAbsent(selectPath, key -> new ArrayList<>())
            .add(aggregationDescriptor);
      }
    }

    private List<PlanNode> constructSourceNodeFromAggregationsDescriptors(
        Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations,
        Map<PartialPath, List<AggregationDescriptor>> descendingAggregations,
        Map<PartialPath, List<AggregationDescriptor>> countTimeAggregations,
        Ordering scanOrder,
        GroupByTimeParameter groupByTimeParameter,
        RewriterContext context) {
      List<PlanNode> sourceNodeList = new ArrayList<>();
      Map<PartialPath, List<AggregationDescriptor>> groupedAscendingAggregations = null;
      if (!countTimeAggregations.isEmpty()) {
        groupedAscendingAggregations = countTimeAggregations;
      } else {
        groupedAscendingAggregations = MetaUtils.groupAlignedAggregations(ascendingAggregations);
      }

      for (Map.Entry<PartialPath, List<AggregationDescriptor>> pathAggregationsEntry :
          groupedAscendingAggregations.entrySet()) {
        sourceNodeList.add(
            createAggregationScanNode(
                pathAggregationsEntry.getKey(),
                pathAggregationsEntry.getValue(),
                scanOrder,
                groupByTimeParameter,
                context,
                countTimeAggregations.isEmpty() ? (byte) 0 : (byte) 2));
      }

      boolean needCheckAscending = groupByTimeParameter == null;
      if (needCheckAscending) {
        Map<PartialPath, List<AggregationDescriptor>> groupedDescendingAggregations =
            MetaUtils.groupAlignedAggregations(descendingAggregations);
        for (Map.Entry<PartialPath, List<AggregationDescriptor>> pathAggregationsEntry :
            groupedDescendingAggregations.entrySet()) {
          sourceNodeList.add(
              createAggregationScanNode(
                  pathAggregationsEntry.getKey(),
                  pathAggregationsEntry.getValue(),
                  scanOrder.reverse(),
                  null,
                  context,
                  (byte) 1));
        }
      }
      return sourceNodeList;
    }

    public PlanNode visitRawDataAggregationTemplateCase(
        RawDataAggregationNode node, RewriterContext context) {
      PlanNode child = node.getChild();

      if (child instanceof ProjectNode) {
        node.setChild(((ProjectNode) child).getChild());
        return visitRawDataAggregation(node, context);
      }

      if (child instanceof FullOuterTimeJoinNode || child instanceof SeriesScanSourceNode) {
        boolean isSingleSource = child instanceof SeriesScanSourceNode;
        if (isSingleSource && ((SeriesScanSourceNode) child).getPushDownPredicate() != null) {
          Expression pushDownPredicate = ((SeriesScanSourceNode) child).getPushDownPredicate();
          if (!PredicateUtils.predicateCanPushIntoScan(pushDownPredicate)) {
            // don't push down, simplify the BE side logic
            return node;
          }
        }

        List<PlanNode> sourceNodeList =
            constructSourceNodeFromTemplateAggregationDescriptors(
                context
                    .getContext()
                    .getTypeProvider()
                    .getTemplatedInfo()
                    .getAscendingDescriptorList(),
                context
                    .getContext()
                    .getTypeProvider()
                    .getTemplatedInfo()
                    .getDescendingDescriptorList(),
                node.getScanOrder(),
                node.getGroupByTimeParameter(),
                context);

        if (isSingleSource && ((SeriesScanSourceNode) child).getPushDownPredicate() != null) {
          Expression pushDownPredicate = ((SeriesScanSourceNode) child).getPushDownPredicate();
          sourceNodeList.forEach(
              sourceNode -> {
                SeriesAggregationSourceNode aggregationSourceNode =
                    (SeriesAggregationSourceNode) sourceNode;
                aggregationSourceNode.setPushDownPredicate(pushDownPredicate);
                if (aggregationSourceNode instanceof AlignedSeriesAggregationScanNode) {
                  ((AlignedSeriesAggregationScanNode) aggregationSourceNode)
                      .setAlignedPath(((AlignedSeriesScanNode) child).getAlignedPath());
                }
              });
        }

        PlanNode resultNode = convergeWithTimeJoin(sourceNodeList, node.getScanOrder(), context);
        resultNode = planProject(resultNode, node, context);

        // After pushing down the predicate, the original scan nodes are no longer needed, we should
        // release the memory that they occupied.
        context.releaseMemoryForFrontEnd(getRamBytesUsedOfOldScanNodes(child));
        return resultNode;
      }
      // cannot push down
      return node;
    }

    private List<PlanNode> constructSourceNodeFromTemplateAggregationDescriptors(
        List<AggregationDescriptor> ascendingAggregations,
        List<AggregationDescriptor> descendingAggregations,
        Ordering scanOrder,
        GroupByTimeParameter groupByTimeParameter,
        RewriterContext context) {

      List<PlanNode> sourceNodeList = new ArrayList<>();
      PartialPath devicePath = context.curDevicePath;
      List<String> measurementList = context.analysis.getMeasurementList();
      List<IMeasurementSchema> measurementSchemaList = context.analysis.getMeasurementSchemaList();
      boolean needCheckAscending = groupByTimeParameter == null;

      if (!context.analysis.getDeviceTemplate().isDirectAligned()) {
        throw new IllegalStateException(
            "Aggregation descriptors with non aligned template are not supported");
      }
      AlignedPath alignedPath = new AlignedPath(devicePath);
      alignedPath.setMeasurementList(measurementList);
      alignedPath.addSchemas(measurementSchemaList);

      if (!ascendingAggregations.isEmpty()) {
        sourceNodeList.add(
            createAggregationScanNode(
                alignedPath,
                ascendingAggregations,
                scanOrder,
                groupByTimeParameter,
                context,
                (byte) 0));
      }

      if (needCheckAscending && !descendingAggregations.isEmpty()) {
        sourceNodeList.add(
            createAggregationScanNode(
                alignedPath, descendingAggregations, scanOrder, null, context, (byte) 1));
      }

      return sourceNodeList;
    }

    private SeriesAggregationSourceNode createAggregationScanNode(
        PartialPath selectPath,
        List<AggregationDescriptor> aggregationDescriptorList,
        Ordering scanOrder,
        GroupByTimeParameter groupByTimeParameter,
        RewriterContext context,
        byte descriptorType) {
      if (selectPath instanceof MeasurementPath) { // non-aligned series
        SeriesAggregationSourceNode node =
            new SeriesAggregationScanNode(
                context.genPlanNodeId(),
                (MeasurementPath) selectPath,
                aggregationDescriptorList,
                scanOrder,
                groupByTimeParameter);
        context
            .getContext()
            .reserveMemoryForFrontEnd(
                MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(node));
        return node;
      } else if (selectPath instanceof AlignedPath) { // aligned series
        AlignedSeriesAggregationScanNode aggScanNode =
            new AlignedSeriesAggregationScanNode(
                context.genPlanNodeId(),
                (AlignedPath) selectPath,
                aggregationDescriptorList,
                scanOrder,
                groupByTimeParameter);
        aggScanNode.setDescriptorType(descriptorType);
        context
            .getContext()
            .reserveMemoryForFrontEnd(
                MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(aggScanNode));
        return aggScanNode;
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
      List<String> outputColumnNames = rawNode.getOutputColumnNames();
      outputColumnNames.remove(ColumnHeaderConstant.ENDTIME);
      if (context.isAlignByDevice()
          && !outputColumnNames.equals(resultNode.getOutputColumnNames())) {
        return new ProjectNode(context.genPlanNodeId(), resultNode, outputColumnNames);
      }
      return resultNode;
    }
  }

  private static class RewriterContext {

    private final Analysis analysis;
    private final MPPQueryContext context;
    private final boolean isAlignByDevice;

    private IDeviceID curDevice;
    private PartialPath curDevicePath;

    public RewriterContext(Analysis analysis, MPPQueryContext context, boolean isAlignByDevice) {
      this.analysis = analysis;
      Validate.notNull(context, "Query context cannot be null.");
      this.context = context;
      this.isAlignByDevice = isAlignByDevice;
    }

    public PlanNodeId genPlanNodeId() {
      return context.getQueryId().genPlanNodeId();
    }

    public boolean isAlignByDevice() {
      return isAlignByDevice;
    }

    public void setCurDevice(IDeviceID curDevice) {
      this.curDevice = curDevice;
    }

    public void setCurDevicePath(PartialPath devicePath) {
      this.curDevicePath = devicePath;
    }

    public MPPQueryContext getContext() {
      return context;
    }

    public Set<Expression> getAggregationExpressions() {
      if (isAlignByDevice) {
        if (analysis.allDevicesInOneTemplate()) {
          return analysis.getAggregationExpressions();
        } else {
          return analysis.getDeviceToAggregationExpressions().get(curDevice);
        }
      }
      return analysis.getAggregationExpressions();
    }

    public void releaseMemoryForFrontEnd(final long bytes) {
      this.context.releaseMemoryReservedForFrontEnd(bytes);
    }
  }
}
