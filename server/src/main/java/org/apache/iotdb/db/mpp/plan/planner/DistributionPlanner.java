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
package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.SimplePlanNodeRewriter;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.AbstractSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MultiChildNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class DistributionPlanner {
  private Analysis analysis;
  private MPPQueryContext context;
  private LogicalQueryPlan logicalPlan;

  private int planFragmentIndex = 0;

  public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
    this.analysis = analysis;
    this.logicalPlan = logicalPlan;
    this.context = logicalPlan.getContext();
  }

  public PlanNode rewriteSource() {
    SourceRewriter rewriter = new SourceRewriter();
    return rewriter.visit(logicalPlan.getRootNode(), new DistributionPlanContext(context));
  }

  public PlanNode addExchangeNode(PlanNode root) {
    ExchangeNodeAdder adder = new ExchangeNodeAdder();
    return adder.visit(root, new NodeGroupContext(context));
  }

  public SubPlan splitFragment(PlanNode root) {
    FragmentBuilder fragmentBuilder = new FragmentBuilder(context);
    return fragmentBuilder.splitToSubPlan(root);
  }

  public DistributedQueryPlan planFragments() {
    System.out.println(PlanNodeUtil.nodeToString(logicalPlan.getRootNode()));
    PlanNode rootAfterRewrite = rewriteSource();
    System.out.println(PlanNodeUtil.nodeToString(rootAfterRewrite));
    PlanNode rootWithExchange = addExchangeNode(rootAfterRewrite);
    System.out.println(PlanNodeUtil.nodeToString(rootWithExchange));
    if (analysis.getStatement() instanceof QueryStatement) {
      analysis
          .getRespDatasetHeader()
          .setColumnToTsBlockIndexMap(rootWithExchange.getOutputColumnNames());
    }
    SubPlan subPlan = splitFragment(rootWithExchange);
    List<FragmentInstance> fragmentInstances = planFragmentInstances(subPlan);
    // Only execute this step for READ operation
    if (context.getQueryType() == QueryType.READ) {
      SetSinkForRootInstance(subPlan, fragmentInstances);
    }
    return new DistributedQueryPlan(
        logicalPlan.getContext(), subPlan, subPlan.getPlanFragmentList(), fragmentInstances);
  }

  // Convert fragment to detailed instance
  // And for parallel-able fragment, clone it into several instances with different params.
  public List<FragmentInstance> planFragmentInstances(SubPlan subPlan) {
    IFragmentParallelPlaner parallelPlaner =
        context.getQueryType() == QueryType.READ
            ? new SimpleFragmentParallelPlanner(subPlan, analysis, context)
            : new WriteFragmentParallelPlanner(subPlan, analysis, context);
    return parallelPlaner.parallelPlan();
  }

  // TODO: (xingtanzjr) Maybe we should handle ResultNode in LogicalPlanner ?
  public void SetSinkForRootInstance(SubPlan subPlan, List<FragmentInstance> instances) {
    FragmentInstance rootInstance = null;
    for (FragmentInstance instance : instances) {
      if (instance.getFragment().getId().equals(subPlan.getPlanFragment().getId())) {
        rootInstance = instance;
        break;
      }
    }
    // root should not be null during normal process
    if (rootInstance == null) {
      return;
    }

    FragmentSinkNode sinkNode = new FragmentSinkNode(context.getQueryId().genPlanNodeId());
    sinkNode.setDownStream(
        context.getLocalDataBlockEndpoint(),
        context.getResultNodeContext().getVirtualFragmentInstanceId(),
        context.getResultNodeContext().getVirtualResultNodeId());
    sinkNode.setChild(rootInstance.getFragment().getRoot());
    context
        .getResultNodeContext()
        .setUpStream(
            rootInstance.getHostDataNode().dataBlockManagerEndPoint,
            rootInstance.getId(),
            sinkNode.getPlanNodeId());
    rootInstance.getFragment().setRoot(sinkNode);
  }

  private PlanFragmentId getNextFragmentId() {
    return new PlanFragmentId(this.logicalPlan.getContext().getQueryId(), this.planFragmentIndex++);
  }

  private class SourceRewriter extends SimplePlanNodeRewriter<DistributionPlanContext> {

    // TODO: (xingtanzjr) implement the method visitDeviceMergeNode()
    public PlanNode visitDeviceMerge(TimeJoinNode node, DistributionPlanContext context) {
      return null;
    }

    @Override
    public PlanNode visitSchemaQueryMerge(
        SchemaQueryMergeNode node, DistributionPlanContext context) {
      SchemaQueryMergeNode root = (SchemaQueryMergeNode) node.clone();
      SchemaQueryScanNode seed = (SchemaQueryScanNode) node.getChildren().get(0);
      TreeSet<TRegionReplicaSet> schemaRegions =
          new TreeSet<>(Comparator.comparingInt(region -> region.getRegionId().getId()));
      analysis
          .getSchemaPartitionInfo()
          .getSchemaPartitionMap()
          .forEach(
              (storageGroup, deviceGroup) -> {
                deviceGroup.forEach(
                    (deviceGroupId, schemaRegionReplicaSet) ->
                        schemaRegions.add(schemaRegionReplicaSet));
              });
      int count = schemaRegions.size();
      schemaRegions.forEach(
          region -> {
            SchemaQueryScanNode schemaQueryScanNode = (SchemaQueryScanNode) seed.clone();
            schemaQueryScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
            schemaQueryScanNode.setRegionReplicaSet(region);
            if (count > 1) {
              schemaQueryScanNode.setLimit(
                  schemaQueryScanNode.getOffset() + schemaQueryScanNode.getLimit());
              schemaQueryScanNode.setOffset(0);
            }
            root.addChild(schemaQueryScanNode);
          });
      return root;
    }

    @Override
    public PlanNode visitCountMerge(CountSchemaMergeNode node, DistributionPlanContext context) {
      CountSchemaMergeNode root = (CountSchemaMergeNode) node.clone();
      SchemaQueryScanNode seed = (SchemaQueryScanNode) node.getChildren().get(0);
      Set<TRegionReplicaSet> schemaRegions = new HashSet<>();
      analysis
          .getSchemaPartitionInfo()
          .getSchemaPartitionMap()
          .forEach(
              (storageGroup, deviceGroup) -> {
                deviceGroup.forEach(
                    (deviceGroupId, schemaRegionReplicaSet) ->
                        schemaRegions.add(schemaRegionReplicaSet));
              });
      schemaRegions.forEach(
          region -> {
            SchemaQueryScanNode schemaQueryScanNode = (SchemaQueryScanNode) seed.clone();
            schemaQueryScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
            schemaQueryScanNode.setRegionReplicaSet(region);
            root.addChild(schemaQueryScanNode);
          });
      return root;
    }

    // TODO: (xingtanzjr) a temporary way to resolve the distribution of single SeriesScanNode issue
    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, DistributionPlanContext context) {
      List<TRegionReplicaSet> dataDistribution =
          analysis.getPartitionInfo(node.getSeriesPath(), node.getTimeFilter());
      if (dataDistribution.size() == 1) {
        node.setRegionReplicaSet(dataDistribution.get(0));
        return node;
      }
      TimeJoinNode timeJoinNode =
          new TimeJoinNode(context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
      for (TRegionReplicaSet dataRegion : dataDistribution) {
        SeriesScanNode split = (SeriesScanNode) node.clone();
        split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        split.setRegionReplicaSet(dataRegion);
        timeJoinNode.addChild(split);
      }
      return timeJoinNode;
    }

    public PlanNode visitSeriesAggregationScan(
        SeriesAggregationScanNode node, DistributionPlanContext context) {
      List<TRegionReplicaSet> dataDistribution =
          analysis.getPartitionInfo(node.getSeriesPath(), node.getTimeFilter());
      List<AggregationDescriptor> leafAggDescriptorList = new ArrayList<>();
      node.getAggregationDescriptorList()
          .forEach(
              descriptor -> {
                leafAggDescriptorList.add(
                    new AggregationDescriptor(
                        descriptor.getAggregationType(),
                        AggregationStep.PARTIAL,
                        descriptor.getInputExpressions()));
              });

      List<AggregationDescriptor> rootAggDescriptorList = new ArrayList<>();
      node.getAggregationDescriptorList()
          .forEach(
              descriptor -> {
                rootAggDescriptorList.add(
                    new AggregationDescriptor(
                        descriptor.getAggregationType(),
                        AggregationStep.FINAL,
                        descriptor.getInputExpressions()));
              });

      AggregationNode aggregationNode =
          new AggregationNode(
              context.queryContext.getQueryId().genPlanNodeId(), rootAggDescriptorList);
      for (TRegionReplicaSet dataRegion : dataDistribution) {
        SeriesAggregationScanNode split = (SeriesAggregationScanNode) node.clone();
        split.setAggregationDescriptorList(leafAggDescriptorList);
        split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        split.setRegionReplicaSet(dataRegion);
        aggregationNode.addChild(split);
      }
      return aggregationNode;
    }

    @Override
    public PlanNode visitAlignedSeriesScan(
        AlignedSeriesScanNode node, DistributionPlanContext context) {
      List<TRegionReplicaSet> dataDistribution =
          analysis.getPartitionInfo(node.getAlignedPath(), node.getTimeFilter());
      if (dataDistribution.size() == 1) {
        node.setRegionReplicaSet(dataDistribution.get(0));
        return node;
      }
      TimeJoinNode timeJoinNode =
          new TimeJoinNode(context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
      for (TRegionReplicaSet dataRegion : dataDistribution) {
        AlignedSeriesScanNode split = (AlignedSeriesScanNode) node.clone();
        split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        split.setRegionReplicaSet(dataRegion);
        timeJoinNode.addChild(split);
      }
      return timeJoinNode;
    }

    @Override
    public PlanNode visitSchemaFetchMerge(
        SchemaFetchMergeNode node, DistributionPlanContext context) {
      SchemaFetchMergeNode root = (SchemaFetchMergeNode) node.clone();
      Map<String, Set<TRegionReplicaSet>> storageGroupSchemaRegionMap = new HashMap<>();
      analysis
          .getSchemaPartitionInfo()
          .getSchemaPartitionMap()
          .forEach(
              (storageGroup, deviceGroup) -> {
                storageGroupSchemaRegionMap.put(storageGroup, new HashSet<>());
                deviceGroup.forEach(
                    (deviceGroupId, schemaRegionReplicaSet) ->
                        storageGroupSchemaRegionMap.get(storageGroup).add(schemaRegionReplicaSet));
              });

      for (PlanNode child : node.getChildren()) {
        for (TRegionReplicaSet schemaRegion :
            storageGroupSchemaRegionMap.get(
                ((SchemaFetchScanNode) child).getStorageGroup().getFullPath())) {
          SchemaFetchScanNode schemaFetchScanNode = (SchemaFetchScanNode) child.clone();
          schemaFetchScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          schemaFetchScanNode.setRegionReplicaSet(schemaRegion);
          root.addChild(schemaFetchScanNode);
        }
      }
      return root;
    }

    @Override
    public PlanNode visitTimeJoin(TimeJoinNode node, DistributionPlanContext context) {
      // Although some logic is similar between Aggregation and RawDataQuery,
      // we still use separate method to process the distribution planning now
      // to make the planning procedure more clear
      if (isAggregationQuery(node)) {
        return planAggregationWithTimeJoin(node, context);
      }

      TimeJoinNode root = (TimeJoinNode) node.clone();
      // Step 1: Get all source nodes. For the node which is not source, add it as the child of
      // current TimeJoinNode
      List<SourceNode> sources = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        if (child instanceof SeriesSourceNode) {
          // If the child is SeriesScanNode, we need to check whether this node should be seperated
          // into several splits.
          SeriesSourceNode handle = (SeriesSourceNode) child;
          List<TRegionReplicaSet> dataDistribution =
              analysis.getPartitionInfo(handle.getPartitionPath(), handle.getPartitionTimeFilter());
          // If the size of dataDistribution is m, this SeriesScanNode should be seperated into m
          // SeriesScanNode.
          for (TRegionReplicaSet dataRegion : dataDistribution) {
            SeriesSourceNode split = (SeriesSourceNode) handle.clone();
            split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
            split.setRegionReplicaSet(dataRegion);
            sources.add(split);
          }
        }
      }
      // Step 2: For the source nodes, group them by the DataRegion.
      Map<TRegionReplicaSet, List<SourceNode>> sourceGroup =
          sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));

      // Step 3: For the source nodes which belong to same data region, add a TimeJoinNode for them
      // and make the
      // new TimeJoinNode as the child of current TimeJoinNode
      // TODO: (xingtanzjr) optimize the procedure here to remove duplicated TimeJoinNode
      final boolean[] addParent = {false};
      sourceGroup.forEach(
          (dataRegion, seriesScanNodes) -> {
            if (seriesScanNodes.size() == 1) {
              root.addChild(seriesScanNodes.get(0));
            } else {
              if (!addParent[0]) {
                seriesScanNodes.forEach(root::addChild);
                addParent[0] = true;
              } else {
                // We clone a TimeJoinNode from root to make the params to be consistent.
                // But we need to assign a new ID to it
                TimeJoinNode parentOfGroup = (TimeJoinNode) root.clone();
                parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
                seriesScanNodes.forEach(parentOfGroup::addChild);
                root.addChild(parentOfGroup);
              }
            }
          });

      // Process the other children which are not SeriesSourceNode
      for (PlanNode child : node.getChildren()) {
        if (!(child instanceof SeriesSourceNode)) {
          // In a general logical query plan, the children of TimeJoinNode should only be
          // SeriesScanNode or SeriesAggregateScanNode
          // So this branch should not be touched.
          root.addChild(visit(child, context));
        }
      }

      return root;
    }

    private boolean isAggregationQuery(TimeJoinNode node) {
      for (PlanNode child : node.getChildren()) {
        if (child instanceof SeriesAggregationScanNode
            || child instanceof AlignedSeriesAggregationScanNode) {
          return true;
        }
      }
      return false;
    }

    private PlanNode planAggregationWithTimeJoin(
        TimeJoinNode root, DistributionPlanContext context) {

      List<SeriesAggregationSourceNode> sources = splitAggregationSourceByPartition(root, context);
      Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup =
          sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));

      // construct AggregationDescriptor for AggregationNode
      List<AggregationDescriptor> rootAggDescriptorList = new ArrayList<>();
      for (PlanNode child : root.getChildren()) {
        SeriesAggregationSourceNode handle = (SeriesAggregationSourceNode) child;
        handle
            .getAggregationDescriptorList()
            .forEach(
                descriptor -> {
                  rootAggDescriptorList.add(
                      new AggregationDescriptor(
                          descriptor.getAggregationType(),
                          AggregationStep.FINAL,
                          descriptor.getInputExpressions()));
                });
      }
      AggregationNode aggregationNode =
          new AggregationNode(
              context.queryContext.getQueryId().genPlanNodeId(), rootAggDescriptorList);

      final boolean[] addParent = {false};
      sourceGroup.forEach(
          (dataRegion, sourceNodes) -> {
            if (sourceNodes.size() == 1) {
              aggregationNode.addChild(sourceNodes.get(0));
            } else {
              if (!addParent[0]) {
                sourceNodes.forEach(aggregationNode::addChild);
                addParent[0] = true;
              } else {
                // We clone a TimeJoinNode from root to make the params to be consistent.
                // But we need to assign a new ID to it
                TimeJoinNode parentOfGroup = (TimeJoinNode) root.clone();
                parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
                sourceNodes.forEach(parentOfGroup::addChild);
                aggregationNode.addChild(parentOfGroup);
              }
            }
          });

      return aggregationNode;
    }

    public PlanNode visitGroupByLevel(GroupByLevelNode root, DistributionPlanContext context) {
      // Firstly, we build the tree structure for GroupByLevelNode
      List<SeriesAggregationSourceNode> sources = splitAggregationSourceByPartition(root, context);
      Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup =
          sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));

      GroupByLevelNode newRoot = (GroupByLevelNode) root.clone();
      final boolean[] addParent = {false};
      sourceGroup.forEach(
          (dataRegion, sourceNodes) -> {
            if (sourceNodes.size() == 1) {
              newRoot.addChild(sourceNodes.get(0));
            } else {
              if (!addParent[0]) {
                sourceNodes.forEach(newRoot::addChild);
                addParent[0] = true;
              } else {
                // We clone a TimeJoinNode from root to make the params to be consistent.
                // But we need to assign a new ID to it
                GroupByLevelNode parentOfGroup = (GroupByLevelNode) root.clone();
                parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
                sourceNodes.forEach(parentOfGroup::addChild);
                newRoot.addChild(parentOfGroup);
              }
            }
          });

      // Then, we calculate the attributes for GroupByLevelNode in each level
      calculateGroupByLevelNodeAttributes(newRoot, 0);
      return newRoot;
    }

    private void calculateGroupByLevelNodeAttributes(PlanNode node, int level) {
      if (node == null) {
        return;
      }
      node.getChildren().forEach(child -> calculateGroupByLevelNodeAttributes(child, level + 1));
      if (!(node instanceof GroupByLevelNode)) {
        return;
      }
      GroupByLevelNode handle = (GroupByLevelNode) node;

      // Construct all outputColumns from children
      List<String> childrenOutputColumns = new ArrayList<>();
      handle
          .getChildren()
          .forEach(child -> childrenOutputColumns.addAll(child.getOutputColumnNames()));

      // Check every OutputColumn of GroupByLevelNode and set the Expression of corresponding
      // AggregationDescriptor
      List<String> outputColumnList = new ArrayList<>();
      List<AggregationDescriptor> descriptorList = new ArrayList<>();
      for (int i = 0; i < handle.getOutputColumnNames().size(); i++) {
        String column = handle.getOutputColumnNames().get(i);
        Set<Expression> originalExpressions =
            analysis.getAggregationExpressions().getOrDefault(column, new HashSet<>());
        List<Expression> descriptorExpression = new ArrayList<>();
        for (String childColumn : childrenOutputColumns) {
          if (childColumn.equals(column)) {
            try {
              descriptorExpression.add(new TimeSeriesOperand(new PartialPath(childColumn)));
            } catch (IllegalPathException e) {
              throw new RuntimeException("error when plan distribution aggregation query", e);
            }
            continue;
          }
          for (Expression exp : originalExpressions) {
            if (exp.getExpressionString().equals(childColumn)) {
              descriptorExpression.add(exp);
            }
          }
        }
        if (descriptorExpression.size() == 0) {
          continue;
        }
        AggregationDescriptor descriptor = handle.getAggregationDescriptorList().get(i).deepClone();
        descriptor.setStep(level == 0 ? AggregationStep.FINAL : AggregationStep.PARTIAL);
        descriptor.setInputExpressions(descriptorExpression);

        outputColumnList.add(column);
        descriptorList.add(descriptor);
      }
      handle.setOutputColumnNames(outputColumnList);
      handle.setAggregationDescriptorList(descriptorList);
    }

    private List<SeriesAggregationSourceNode> splitAggregationSourceByPartition(
        MultiChildNode root, DistributionPlanContext context) {
      // Step 1: split SeriesAggregationSourceNode according to data partition
      List<SeriesAggregationSourceNode> sources = new ArrayList<>();
      Map<PartialPath, Integer> regionCountPerSeries = new HashMap<>();
      for (PlanNode child : root.getChildren()) {
        SeriesAggregationSourceNode handle = (SeriesAggregationSourceNode) child;
        List<TRegionReplicaSet> dataDistribution =
            analysis.getPartitionInfo(handle.getPartitionPath(), handle.getPartitionTimeFilter());
        for (TRegionReplicaSet dataRegion : dataDistribution) {
          SeriesAggregationSourceNode split = (SeriesAggregationSourceNode) handle.clone();
          split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          split.setRegionReplicaSet(dataRegion);
          // Let each split reference different object of AggregationDescriptorList
          split.setAggregationDescriptorList(
              handle.getAggregationDescriptorList().stream()
                  .map(AggregationDescriptor::deepClone)
                  .collect(Collectors.toList()));
          sources.add(split);
        }
        regionCountPerSeries.put(handle.getPartitionPath(), dataDistribution.size());
      }

      // Step 2: change the step for each SeriesAggregationSourceNode according to its split count
      for (SeriesAggregationSourceNode source : sources) {
        boolean isFinal = regionCountPerSeries.get(source.getPartitionPath()) == 1;
        source
            .getAggregationDescriptorList()
            .forEach(d -> d.setStep(isFinal ? AggregationStep.FINAL : AggregationStep.PARTIAL));
      }
      return sources;
    }

    public PlanNode visit(PlanNode node, DistributionPlanContext context) {
      return node.accept(this, context);
    }
  }

  private class DistributionPlanContext {
    private MPPQueryContext queryContext;

    public DistributionPlanContext(MPPQueryContext queryContext) {
      this.queryContext = queryContext;
    }
  }

  private class ExchangeNodeAdder extends PlanVisitor<PlanNode, NodeGroupContext> {
    @Override
    public PlanNode visitPlan(PlanNode node, NodeGroupContext context) {
      // TODO: (xingtanzjr) we apply no action for IWritePlanNode currently
      if (node instanceof WritePlanNode) {
        return node;
      }
      // Visit all the children of current node
      List<PlanNode> children =
          node.getChildren().stream()
              .map(child -> child.accept(this, context))
              .collect(toImmutableList());

      // Calculate the node distribution info according to its children

      // Put the node distribution info into context
      // NOTICE: we will only process the PlanNode which has only 1 child here. For the other
      // PlanNode, we need to process
      // them with special method
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.SAME_WITH_ALL_CHILDREN, null));

      return node.cloneWithChildren(children);
    }

    @Override
    public PlanNode visitSchemaQueryMerge(SchemaQueryMergeNode node, NodeGroupContext context) {
      return internalVisitSchemaMerge(node, context);
    }

    private PlanNode internalVisitSchemaMerge(
        AbstractSchemaMergeNode node, NodeGroupContext context) {
      node.getChildren()
          .forEach(
              child -> {
                visit(child, context);
              });
      NodeDistribution nodeDistribution =
          new NodeDistribution(NodeDistributionType.DIFFERENT_FROM_ALL_CHILDREN);
      PlanNode newNode = node.clone();
      nodeDistribution.region = calculateSchemaRegionByChildren(node.getChildren(), context);
      context.putNodeDistribution(newNode.getPlanNodeId(), nodeDistribution);
      node.getChildren()
          .forEach(
              child -> {
                if (!nodeDistribution.region.equals(
                    context.getNodeDistribution(child.getPlanNodeId()).region)) {
                  ExchangeNode exchangeNode =
                      new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
                  exchangeNode.setChild(child);
                  exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
                  newNode.addChild(exchangeNode);
                } else {
                  newNode.addChild(child);
                }
              });
      return newNode;
    }

    @Override
    public PlanNode visitCountMerge(CountSchemaMergeNode node, NodeGroupContext context) {
      return internalVisitSchemaMerge(node, context);
    }

    @Override
    public PlanNode visitSchemaQueryScan(SchemaQueryScanNode node, NodeGroupContext context) {
      NodeDistribution nodeDistribution = new NodeDistribution(NodeDistributionType.NO_CHILD);
      nodeDistribution.region = node.getRegionReplicaSet();
      context.putNodeDistribution(node.getPlanNodeId(), nodeDistribution);
      return node;
    }

    @Override
    public PlanNode visitSchemaFetchMerge(SchemaFetchMergeNode node, NodeGroupContext context) {
      return internalVisitSchemaMerge(node, context);
    }

    @Override
    public PlanNode visitSchemaFetchScan(SchemaFetchScanNode node, NodeGroupContext context) {
      NodeDistribution nodeDistribution = new NodeDistribution(NodeDistributionType.NO_CHILD);
      nodeDistribution.region = node.getRegionReplicaSet();
      context.putNodeDistribution(node.getPlanNodeId(), nodeDistribution);
      return node;
    }

    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, NodeGroupContext context) {
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.NO_CHILD, node.getRegionReplicaSet()));
      return node.clone();
    }

    @Override
    public PlanNode visitAlignedSeriesScan(AlignedSeriesScanNode node, NodeGroupContext context) {
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.NO_CHILD, node.getRegionReplicaSet()));
      return node.clone();
    }

    public PlanNode visitSeriesAggregationScan(
        SeriesAggregationScanNode node, NodeGroupContext context) {
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.NO_CHILD, node.getRegionReplicaSet()));
      return node.clone();
    }

    @Override
    public PlanNode visitTimeJoin(TimeJoinNode node, NodeGroupContext context) {
      return processMultiChildNode(node, context);
    }

    public PlanNode visitRowBasedSeriesAggregate(AggregationNode node, NodeGroupContext context) {
      return processMultiChildNode(node, context);
    }

    @Override
    public PlanNode visitGroupByLevel(GroupByLevelNode node, NodeGroupContext context) {
      return processMultiChildNode(node, context);
    }

    private PlanNode processMultiChildNode(MultiChildNode node, NodeGroupContext context) {
      MultiChildNode newNode = (MultiChildNode) node.clone();
      List<PlanNode> visitedChildren = new ArrayList<>();
      node.getChildren()
          .forEach(
              child -> {
                visitedChildren.add(visit(child, context));
              });

      TRegionReplicaSet dataRegion = calculateDataRegionByChildren(visitedChildren, context);
      NodeDistributionType distributionType =
          nodeDistributionIsSame(visitedChildren, context)
              ? NodeDistributionType.SAME_WITH_ALL_CHILDREN
              : NodeDistributionType.SAME_WITH_SOME_CHILD;
      context.putNodeDistribution(
          newNode.getPlanNodeId(), new NodeDistribution(distributionType, dataRegion));

      // If the distributionType of all the children are same, no ExchangeNode need to be added.
      if (distributionType == NodeDistributionType.SAME_WITH_ALL_CHILDREN) {
        newNode.setChildren(visitedChildren);
        return newNode;
      }

      // Otherwise, we need to add ExchangeNode for the child whose DataRegion is different from the
      // parent.
      visitedChildren.forEach(
          child -> {
            if (!dataRegion.equals(context.getNodeDistribution(child.getPlanNodeId()).region)) {
              ExchangeNode exchangeNode =
                  new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
              exchangeNode.setChild(child);
              exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
              newNode.addChild(exchangeNode);
            } else {
              newNode.addChild(child);
            }
          });
      return newNode;
    }

    private TRegionReplicaSet calculateDataRegionByChildren(
        List<PlanNode> children, NodeGroupContext context) {
      // Step 1: calculate the count of children group by DataRegion.
      Map<TRegionReplicaSet, Long> groupByRegion =
          children.stream()
              .collect(
                  Collectors.groupingBy(
                      child -> context.getNodeDistribution(child.getPlanNodeId()).region,
                      Collectors.counting()));
      // Step 2: return the RegionReplicaSet with max count
      return Collections.max(groupByRegion.entrySet(), Map.Entry.comparingByValue()).getKey();
    }

    private TRegionReplicaSet calculateSchemaRegionByChildren(
        List<PlanNode> children, NodeGroupContext context) {
      // We always make the schemaRegion of MetaMergeNode to be the same as its first child.
      return context.getNodeDistribution(children.get(0).getPlanNodeId()).region;
    }

    private boolean nodeDistributionIsSame(List<PlanNode> children, NodeGroupContext context) {
      // The size of children here should always be larger than 0, or our code has Bug.
      NodeDistribution first = context.getNodeDistribution(children.get(0).getPlanNodeId());
      for (int i = 1; i < children.size(); i++) {
        NodeDistribution next = context.getNodeDistribution(children.get(i).getPlanNodeId());
        if (first.region == null || !first.region.equals(next.region)) {
          return false;
        }
      }
      return true;
    }

    public PlanNode visit(PlanNode node, NodeGroupContext context) {
      return node.accept(this, context);
    }
  }

  private class NodeGroupContext {
    private MPPQueryContext queryContext;
    private Map<PlanNodeId, NodeDistribution> nodeDistributionMap;

    public NodeGroupContext(MPPQueryContext queryContext) {
      this.queryContext = queryContext;
      this.nodeDistributionMap = new HashMap<>();
    }

    public void putNodeDistribution(PlanNodeId nodeId, NodeDistribution distribution) {
      this.nodeDistributionMap.put(nodeId, distribution);
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistributionMap.get(nodeId);
    }
  }

  private enum NodeDistributionType {
    SAME_WITH_ALL_CHILDREN,
    SAME_WITH_SOME_CHILD,
    DIFFERENT_FROM_ALL_CHILDREN,
    NO_CHILD,
  }

  private class NodeDistribution {
    private NodeDistributionType type;
    private TRegionReplicaSet region;

    private NodeDistribution(NodeDistributionType type, TRegionReplicaSet region) {
      this.type = type;
      this.region = region;
    }

    private NodeDistribution(NodeDistributionType type) {
      this.type = type;
    }
  }

  private class FragmentBuilder {
    private MPPQueryContext context;

    public FragmentBuilder(MPPQueryContext context) {
      this.context = context;
    }

    public SubPlan splitToSubPlan(PlanNode root) {
      SubPlan rootSubPlan = createSubPlan(root);
      splitToSubPlan(root, rootSubPlan);
      return rootSubPlan;
    }

    private void splitToSubPlan(PlanNode root, SubPlan subPlan) {
      // TODO: (xingtanzjr) we apply no action for IWritePlanNode currently
      if (root instanceof WritePlanNode) {
        return;
      }
      if (root instanceof ExchangeNode) {
        // We add a FragmentSinkNode for newly created PlanFragment
        ExchangeNode exchangeNode = (ExchangeNode) root;
        FragmentSinkNode sinkNode = new FragmentSinkNode(context.getQueryId().genPlanNodeId());
        sinkNode.setChild(exchangeNode.getChild());
        sinkNode.setDownStreamPlanNodeId(exchangeNode.getPlanNodeId());

        // Record the source node info in the ExchangeNode so that we can keep the connection of
        // these nodes/fragments
        exchangeNode.setRemoteSourceNode(sinkNode);
        // We cut off the subtree to make the ExchangeNode as the leaf node of current PlanFragment
        exchangeNode.cleanChildren();

        // Build the child SubPlan Tree
        SubPlan childSubPlan = createSubPlan(sinkNode);
        splitToSubPlan(sinkNode, childSubPlan);

        subPlan.addChild(childSubPlan);
        return;
      }
      for (PlanNode child : root.getChildren()) {
        splitToSubPlan(child, subPlan);
      }
    }

    private SubPlan createSubPlan(PlanNode root) {
      PlanFragment fragment = new PlanFragment(getNextFragmentId(), root);
      return new SubPlan(fragment);
    }
  }
}
