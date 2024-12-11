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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateCombineIntoTableScanChecker;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoMetadataChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.EqualityInference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ReplaceSymbolInExpression;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.PARTITION_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.SCHEMA_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.TABLE_TYPE;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor.extractUnique;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator.isDeterministic;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.extractGlobalTimeFilter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.filterDeterministicConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.extractJoinPredicate;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.joinEqualityExpression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.processInnerJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>After the optimized rule {@link
 * org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.SimplifyExpressions}
 * finished, predicate expression in FilterNode has been transformed to conjunctive normal
 * forms(CNF).
 *
 * <p>In this class, we examine each expression in CNFs, determine how to use it, in metadata query,
 * or pushed down into ScanOperators, or it can only be used in FilterNode above with
 * DeviceTableScanNode.
 *
 * <ul>
 *   <li>For metadata query expressions, it will be used in {@code tableIndexScan} method to
 *       generate the deviceEntries and DataPartition used for DeviceTableScanNode.
 *   <li>For expressions which can be pushed into DeviceTableScanNode, we will execute {@code
 *       extractGlobalTimeFilter}, to extract the timePredicate and pushDownValuePredicate.
 *   <li>Expression which can not be pushed down into DeviceTableScanNode, will be used in the
 *       FilterNode above of DeviceTableScanNode.
 * </ul>
 *
 * <p>Notice that, when aggregation, multi-table, join are introduced, this optimization rule need
 * to be adapted.
 */
public class PushPredicateIntoTableScan implements PlanOptimizer {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    return plan.accept(
        new Rewriter(
            context.getQueryContext(),
            context.getAnalysis(),
            context.getMetadata(),
            context.getSymbolAllocator()),
        new RewriteContext(TRUE_LITERAL));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriteContext> {
    private final MPPQueryContext queryContext;
    private final Analysis analysis;
    private final Metadata metadata;
    private final SymbolAllocator symbolAllocator;
    private final QueryId queryId;

    Rewriter(
        MPPQueryContext queryContext,
        Analysis analysis,
        Metadata metadata,
        SymbolAllocator symbolAllocator) {
      this.queryContext = queryContext;
      this.analysis = analysis;
      this.metadata = metadata;
      this.symbolAllocator = symbolAllocator;
      this.queryId = queryContext.getQueryId();
    }

    @Override
    public PlanNode visitPlan(PlanNode node, RewriteContext context) {
      PlanNode rewrittenNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        RewriteContext subContext = new RewriteContext();
        PlanNode rewrittenChild = child.accept(this, subContext);
        rewrittenNode.addChild(rewrittenChild);
      }
      if (!TRUE_LITERAL.equals(context.inheritedPredicate)) {
        FilterNode filterNode =
            new FilterNode(queryId.genPlanNodeId(), rewrittenNode, context.inheritedPredicate);
        context.inheritedPredicate = TRUE_LITERAL;
        return filterNode;
      } else {
        return rewrittenNode;
      }
    }

    @Override
    public PlanNode visitProject(ProjectNode node, RewriteContext context) {
      for (Expression expression : node.getAssignments().getMap().values()) {
        if (containsDiffFunction(expression)) {
          node.setChild(node.getChild().accept(this, new RewriteContext()));
          if (!TRUE_LITERAL.equals(context.inheritedPredicate)) {
            FilterNode filterNode =
                new FilterNode(queryId.genPlanNodeId(), node, context.inheritedPredicate);
            context.inheritedPredicate = TRUE_LITERAL;
            return filterNode;
          } else {
            return node;
          }
        }
      }

      // TODO(beyyes) in some situation, predicate can not be pushed down below ProjectNode
      node.setChild(node.getChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriteContext context) {
      checkArgument(node.getPredicate() != null, "Filter predicate of FilterNode is null");

      Expression predicate = combineConjuncts(node.getPredicate(), context.inheritedPredicate);

      // when exist diff function, predicate can not be pushed down into DeviceTableScanNode
      if (containsDiffFunction(predicate)) {
        node.setChild(node.getChild().accept(this, new RewriteContext()));
        node.setPredicate(predicate);
        context.inheritedPredicate = TRUE_LITERAL;
        return node;
      }

      // FilterNode may get from having, subquery or join
      PlanNode rewrittenPlan = node.getChild().accept(this, new RewriteContext(predicate));
      if (!(rewrittenPlan instanceof FilterNode)) {
        return rewrittenPlan;
      }

      FilterNode rewrittenFilterNode = (FilterNode) rewrittenPlan;
      // TODO(beyyes) use areExpressionsEquivalent method
      if (!rewrittenFilterNode.getPredicate().equals(node.getPredicate())
          || node.getChild() != rewrittenFilterNode.getChild()) {
        return rewrittenPlan;
      }
      return node;
    }

    //    private boolean areExpressionsEquivalent(
    //        Expression leftExpression, Expression rightExpression) {
    //      return false;
    //    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, RewriteContext context) {
      if (node.hasEmptyGroupingSet()) {
        // TODO: in case of grouping sets, we should be able to push the filters over grouping keys
        // below the aggregation
        // and also preserve the filter above the aggregation if it has an empty grouping set
        return visitPlan(node, context);
      }

      Expression inheritedPredicate = context.inheritedPredicate;

      EqualityInference equalityInference = new EqualityInference(metadata, inheritedPredicate);

      List<Expression> pushdownConjuncts = new ArrayList<>();
      List<Expression> postAggregationConjuncts = new ArrayList<>();

      // Strip out non-deterministic conjuncts
      extractConjuncts(inheritedPredicate).stream()
          .filter(expression -> !isDeterministic(expression))
          .forEach(postAggregationConjuncts::add);
      inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

      // Sort non-equality predicates by those that can be pushed down and those that cannot
      Set<Symbol> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
      EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate)
          .forEach(
              conjunct -> {
                if (node.getGroupIdSymbol().isPresent()
                    && extractUnique(conjunct).contains(node.getGroupIdSymbol().get())) {
                  // aggregation operator synthesizes outputs for group ids corresponding to the
                  // global grouping set (i.e., ()), so we
                  // need to preserve any predicates that evaluate the group id to run after the
                  // aggregation
                  // TODO: we should be able to infer if conditions on grouping() correspond to
                  // global grouping sets to determine whether
                  // we need to do this for each specific case
                  postAggregationConjuncts.add(conjunct);
                } else {
                  Expression rewrittenConjunct = equalityInference.rewrite(conjunct, groupingKeys);
                  if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                  } else {
                    postAggregationConjuncts.add(conjunct);
                  }
                }
              });

      // Add the equality predicates back in
      EqualityInference.EqualityPartition equalityPartition =
          equalityInference.generateEqualitiesPartitionedBy(groupingKeys);
      pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
      postAggregationConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
      postAggregationConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

      // PlanNode rewrittenSource = context.rewrite(node.getSource(),
      // combineConjuncts(pushdownConjuncts));

      // if (rewrittenSource != node.getChild()) {
      context.inheritedPredicate = combineConjuncts(pushdownConjuncts);
      PlanNode output =
          AggregationNode.builderFrom(node)
              .setSource(node.getChild().accept(this, context))
              .setPreGroupedSymbols(ImmutableList.of())
              .build();
      if (!postAggregationConjuncts.isEmpty()) {
        output =
            new FilterNode(
                queryId.genPlanNodeId(), output, combineConjuncts(postAggregationConjuncts));
      }
      return output;
    }

    @Override
    public PlanNode visitDeviceTableScan(
        DeviceTableScanNode tableScanNode, RewriteContext context) {

      // no predicate, just scan all matched deviceEntries
      if (TRUE_LITERAL.equals(context.inheritedPredicate)) {
        getDeviceEntriesWithDataPartitions(tableScanNode, Collections.emptyList(), null);
        return tableScanNode;
      }

      // has predicate, deal with split predicate
      return combineFilterAndScan(tableScanNode, context.inheritedPredicate);
    }

    public PlanNode combineFilterAndScan(DeviceTableScanNode tableScanNode, Expression predicate) {
      SplitExpression splitExpression = splitPredicate(tableScanNode, predicate);

      // exist expressions can push down to scan operator
      if (!splitExpression.getExpressionsCanPushDown().isEmpty()) {
        List<Expression> expressions = splitExpression.getExpressionsCanPushDown();
        Expression pushDownPredicate =
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions);

        // extract global time filter and set it to DeviceTableScanNode
        Pair<Expression, Boolean> resultPair =
            extractGlobalTimeFilter(pushDownPredicate, splitExpression.getTimeColumnName());
        if (resultPair.left != null) {
          tableScanNode.setTimePredicate(resultPair.left);
        }
        if (Boolean.TRUE.equals(resultPair.right)) {
          if (pushDownPredicate instanceof LogicalExpression
              && ((LogicalExpression) pushDownPredicate).getTerms().size() == 1) {
            tableScanNode.setPushDownPredicate(
                ((LogicalExpression) pushDownPredicate).getTerms().get(0));
          } else {
            tableScanNode.setPushDownPredicate(pushDownPredicate);
          }
        }
      } else {
        tableScanNode.setPushDownPredicate(null);
      }

      // do index scan after expressionCanPushDown is processed
      getDeviceEntriesWithDataPartitions(
          tableScanNode,
          splitExpression.getMetadataExpressions(),
          splitExpression.getTimeColumnName());

      // exist expressions can not push down to scan operator
      if (!splitExpression.getExpressionsCannotPushDown().isEmpty()) {
        List<Expression> expressions = splitExpression.getExpressionsCannotPushDown();
        return new FilterNode(
            queryId.genPlanNodeId(),
            tableScanNode,
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions));
      }

      return tableScanNode;
    }

    private SplitExpression splitPredicate(DeviceTableScanNode node, Expression predicate) {
      Set<String> idOrAttributeColumnNames = new HashSet<>(node.getAssignments().size());
      Set<String> measurementColumnNames = new HashSet<>(node.getAssignments().size());
      String timeColumnName = null;
      for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
        Symbol columnSymbol = entry.getKey();
        ColumnSchema columnSchema = entry.getValue();
        if (TIME.equals(columnSchema.getColumnCategory())) {
          measurementColumnNames.add(columnSymbol.getName());
          timeColumnName = columnSymbol.getName();
        } else if (MEASUREMENT.equals(columnSchema.getColumnCategory())) {
          measurementColumnNames.add(columnSymbol.getName());
        } else {
          idOrAttributeColumnNames.add(columnSymbol.getName());
        }
      }

      List<Expression> metadataExpressions = new ArrayList<>();
      List<Expression> expressionsCanPushDown = new ArrayList<>();
      List<Expression> expressionsCannotPushDown = new ArrayList<>();

      if (predicate instanceof LogicalExpression
          && ((LogicalExpression) predicate).getOperator() == LogicalExpression.Operator.AND) {

        for (Expression expression : ((LogicalExpression) predicate).getTerms()) {
          if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, expression)) {
            metadataExpressions.add(expression);
          } else if (PredicateCombineIntoTableScanChecker.check(
              measurementColumnNames, expression)) {
            expressionsCanPushDown.add(expression);
          } else {
            expressionsCannotPushDown.add(expression);
          }
        }

        return new SplitExpression(
            metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown, timeColumnName);
      }

      if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, predicate)) {
        metadataExpressions.add(predicate);
      } else if (PredicateCombineIntoTableScanChecker.check(measurementColumnNames, predicate)) {
        expressionsCanPushDown.add(predicate);
      } else {
        expressionsCannotPushDown.add(predicate);
      }

      return new SplitExpression(
          metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown, timeColumnName);
    }

    private void getDeviceEntriesWithDataPartitions(
        DeviceTableScanNode tableScanNode,
        List<Expression> metadataExpressions,
        String timeColumnName) {

      List<String> attributeColumns = new ArrayList<>();
      int attributeIndex = 0;
      for (Map.Entry<Symbol, ColumnSchema> entry : tableScanNode.getAssignments().entrySet()) {
        Symbol columnSymbol = entry.getKey();
        ColumnSchema columnSchema = entry.getValue();
        if (ATTRIBUTE.equals(columnSchema.getColumnCategory())) {
          attributeColumns.add(columnSchema.getName());
          tableScanNode.getIdAndAttributeIndexMap().put(columnSymbol, attributeIndex++);
        }
      }

      long startTime = System.nanoTime();
      List<DeviceEntry> deviceEntries =
          metadata.indexScan(
              tableScanNode.getQualifiedObjectName(),
              metadataExpressions.stream()
                  .map(
                      expression ->
                          ReplaceSymbolInExpression.transform(
                              expression, tableScanNode.getAssignments()))
                  .collect(Collectors.toList()),
              attributeColumns,
              queryContext);
      tableScanNode.setDeviceEntries(deviceEntries);

      long schemaFetchCost = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TABLE_TYPE, SCHEMA_FETCHER, schemaFetchCost);
      queryContext.setFetchSchemaCost(schemaFetchCost);

      if (deviceEntries.isEmpty()) {
        if (analysis.noAggregates() && !analysis.hasJoinNode()) {
          // no device entries, queries(except aggregation and join) can be finished
          analysis.setEmptyDataSource(true);
          analysis.setFinishQueryAfterAnalyze();
        }
      } else {
        Filter timeFilter =
            tableScanNode
                .getTimePredicate()
                .map(value -> value.accept(new ConvertPredicateToTimeFilterVisitor(), null))
                .orElse(null);

        tableScanNode.setTimeFilter(timeFilter);
        String treeModelDatabase =
            "root." + tableScanNode.getQualifiedObjectName().getDatabaseName();

        startTime = System.nanoTime();
        DataPartition dataPartition =
            fetchDataPartitionByDevices(treeModelDatabase, deviceEntries, timeFilter);

        if (dataPartition.getDataPartitionMap().size() > 1) {
          throw new IllegalStateException(
              "Table model can only process data only in one database yet!");
        }

        if (dataPartition.getDataPartitionMap().isEmpty()) {
          if (analysis.noAggregates() && !analysis.hasJoinNode()) {
            // no data partitions, queries(except aggregation and join) can be finished
            analysis.setEmptyDataSource(true);
            analysis.setFinishQueryAfterAnalyze();
          }
        } else {
          analysis.upsertDataPartition(dataPartition);
        }

        long fetchPartitionCost = System.nanoTime() - startTime;
        QueryPlanCostMetricSet.getInstance()
            .recordPlanCost(TABLE_TYPE, PARTITION_FETCHER, fetchPartitionCost);
        queryContext.setFetchPartitionCost(fetchPartitionCost);
      }
    }

    @Override
    public PlanNode visitJoin(JoinNode node, RewriteContext context) {
      Expression inheritedPredicate =
          context.inheritedPredicate != null ? context.inheritedPredicate : TRUE_LITERAL;

      // See if we can rewrite outer joins in terms of a plain inner join
      // node = tryNormalizeToOuterToInnerJoin(node, inheritedPredicate);

      Expression leftEffectivePredicate = TRUE_LITERAL;
      // effectivePredicateExtractor.extract(session, node.getLeftChild(), types, typeAnalyzer);
      Expression rightEffectivePredicate = TRUE_LITERAL;
      // effectivePredicateExtractor.extract(session, node.getRightChild(), types, typeAnalyzer);
      Expression joinPredicate = extractJoinPredicate(node);

      Expression leftPredicate;
      Expression rightPredicate;
      Expression postJoinPredicate;
      Expression newJoinPredicate;

      switch (node.getJoinType()) {
        case INNER:
          JoinUtils.InnerJoinPushDownResult innerJoinPushDownResult =
              processInnerJoin(
                  metadata,
                  inheritedPredicate,
                  leftEffectivePredicate,
                  rightEffectivePredicate,
                  joinPredicate,
                  node.getLeftChild().getOutputSymbols(),
                  node.getRightChild().getOutputSymbols());
          leftPredicate = innerJoinPushDownResult.getLeftPredicate();
          rightPredicate = innerJoinPushDownResult.getRightPredicate();
          postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
          newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
          break;
        case FULL:
          leftPredicate = TRUE_LITERAL;
          rightPredicate = TRUE_LITERAL;
          postJoinPredicate = inheritedPredicate;
          newJoinPredicate = joinPredicate;
          break;
        default:
          throw new IllegalStateException("Only support INNER JOIN in current version");
      }

      // newJoinPredicate = simplifyExpression(newJoinPredicate);

      // Create identity projections for all existing symbols
      Assignments.Builder leftProjections = Assignments.builder();
      leftProjections.putAll(
          node.getLeftChild().getOutputSymbols().stream()
              .collect(toImmutableMap(key -> key, Symbol::toSymbolReference)));

      Assignments.Builder rightProjections = Assignments.builder();
      rightProjections.putAll(
          node.getRightChild().getOutputSymbols().stream()
              .collect(toImmutableMap(key -> key, Symbol::toSymbolReference)));

      // Create new projections for the new join clauses
      List<JoinNode.EquiJoinClause> equiJoinClauses = new ArrayList<>();
      ImmutableList.Builder<Expression> joinFilterBuilder = ImmutableList.builder();
      boolean hasFilter = false;
      Expression lastEquiJoinConjunct = null;
      for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
        if (joinEqualityExpressionOnTimeColumn(conjunct, node)) {
          lastEquiJoinConjunct = conjunct;
          ComparisonExpression equality = (ComparisonExpression) conjunct;

          boolean alignedComparison =
              node.getLeftChild().getOutputSymbols().containsAll(extractUnique(equality.getLeft()));
          Expression leftExpression = alignedComparison ? equality.getLeft() : equality.getRight();
          Expression rightExpression = alignedComparison ? equality.getRight() : equality.getLeft();

          Symbol leftSymbol = symbolForExpression(leftExpression);
          if (!node.getLeftChild().getOutputSymbols().contains(leftSymbol)) {
            leftProjections.put(leftSymbol, leftExpression);
          }

          Symbol rightSymbol = symbolForExpression(rightExpression);
          if (!node.getRightChild().getOutputSymbols().contains(rightSymbol)) {
            rightProjections.put(rightSymbol, rightExpression);
          }

          equiJoinClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
        } else {
          joinFilterBuilder.add(conjunct);
          hasFilter = true;
        }
      }

      // todo: Remove this check after supporting join on multiple columns.
      checkArgument(equiJoinClauses.size() <= 1, "Only support Join on one column for now.");
      if (!equiJoinClauses.isEmpty() && hasFilter) {
        equiJoinClauses.clear();
        joinFilterBuilder.add(lastEquiJoinConjunct);
      }

      List<Expression> joinFilter = joinFilterBuilder.build();
      //      DynamicFiltersResult dynamicFiltersResult = createDynamicFilters(node,
      // equiJoinClauses, joinFilter, session, idAllocator);
      //      Map<DynamicFilterId, Symbol> dynamicFilters =
      // dynamicFiltersResult.getDynamicFilters();
      // leftPredicate = combineConjuncts(metadata, leftPredicate, combineConjuncts(metadata,
      // dynamicFiltersResult.getPredicates()));

      PlanNode leftSource;
      PlanNode rightSource;
      boolean equiJoinClausesUnmodified =
          ImmutableSet.copyOf(equiJoinClauses).equals(ImmutableSet.copyOf(node.getCriteria()));
      if (!equiJoinClausesUnmodified) {
        leftSource =
            new ProjectNode(queryId.genPlanNodeId(), node.getLeftChild(), leftProjections.build())
                .accept(this, new RewriteContext(leftPredicate));
        rightSource =
            new ProjectNode(queryId.genPlanNodeId(), node.getRightChild(), rightProjections.build())
                .accept(this, new RewriteContext(rightPredicate));
      } else {
        leftSource = node.getLeftChild().accept(this, new RewriteContext(leftPredicate));
        rightSource = node.getRightChild().accept(this, new RewriteContext(rightPredicate));
      }

      Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(joinFilter));
      if (newJoinFilter.get().equals(TRUE_LITERAL)) {
        newJoinFilter = Optional.empty();
      }

      if (node.getJoinType() == INNER && newJoinFilter.isPresent() && equiJoinClauses.isEmpty()) {
        // if we do not have any equi conjunct we do not pushdown non-equality condition into
        // inner join, so we plan execution as nested-loops-join followed by filter instead
        // hash join.
        postJoinPredicate = combineConjuncts(postJoinPredicate, newJoinFilter.get());
        newJoinFilter = Optional.empty();
      }

      boolean filtersEquivalent =
          newJoinFilter.isPresent() == node.getFilter().isPresent() && (!newJoinFilter.isPresent());
      // areExpressionsEquivalent(newJoinFilter.get(), node.getFilter().get());

      PlanNode output = node;
      if (leftSource != node.getLeftChild()
          || rightSource != node.getRightChild()
          || !filtersEquivalent
          // !dynamicFilters.equals(node.getDynamicFilters()) ||
          || !equiJoinClausesUnmodified) {
        leftSource =
            new ProjectNode(
                queryContext.getQueryId().genPlanNodeId(), leftSource, leftProjections.build());
        rightSource =
            new ProjectNode(
                queryContext.getQueryId().genPlanNodeId(), rightSource, rightProjections.build());

        output =
            new JoinNode(
                node.getPlanNodeId(),
                node.getJoinType(),
                leftSource,
                rightSource,
                equiJoinClauses,
                leftSource.getOutputSymbols(),
                rightSource.getOutputSymbols(),
                newJoinFilter,
                node.isSpillable());
      }

      // sort the left and right child of join node if it is not a cross join
      if (!((JoinNode) output).isCrossJoin()) {
        JoinNode.EquiJoinClause joinCriteria = ((JoinNode) output).getCriteria().get(0);
        OrderingScheme leftOrderingScheme =
            new OrderingScheme(
                Collections.singletonList(joinCriteria.getLeft()),
                Collections.singletonMap(joinCriteria.getLeft(), ASC_NULLS_LAST));
        OrderingScheme rightOrderingScheme =
            new OrderingScheme(
                Collections.singletonList(joinCriteria.getRight()),
                Collections.singletonMap(joinCriteria.getRight(), ASC_NULLS_LAST));
        SortNode leftSortNode =
            new SortNode(
                queryId.genPlanNodeId(),
                ((JoinNode) output).getLeftChild(),
                leftOrderingScheme,
                false,
                false);
        SortNode rightSortNode =
            new SortNode(
                queryId.genPlanNodeId(),
                ((JoinNode) output).getRightChild(),
                rightOrderingScheme,
                false,
                false);
        ((JoinNode) output).setLeftChild(leftSortNode);
        ((JoinNode) output).setRightChild(rightSortNode);
      }

      if (!postJoinPredicate.equals(TRUE_LITERAL)) {
        output =
            new FilterNode(queryContext.getQueryId().genPlanNodeId(), output, postJoinPredicate);
      }

      if (!node.getOutputSymbols().equals(output.getOutputSymbols())) {
        output =
            new ProjectNode(
                queryContext.getQueryId().genPlanNodeId(),
                output,
                Assignments.identity(node.getOutputSymbols()));
      }

      return output;
    }

    private boolean joinEqualityExpressionOnTimeColumn(Expression conjunct, JoinNode node) {
      if (!joinEqualityExpression(
          conjunct,
          node.getLeftChild().getOutputSymbols(),
          node.getRightChild().getOutputSymbols())) {
        return false;
      }
      // conjunct must be a comparison expression
      ComparisonExpression equality = (ComparisonExpression) conjunct;
      // After Optimization, some subqueries are transformed into Join.
      // For now, Users can only use join on time. And the join is implemented using MergeSortJoin.
      // However, it's assumed that use Filter + NestedLoopJoin is better than MergeSortJoin
      // for scalar subquery, since sorting the left table is not necessary.
      // So, we want to find out whether the join is on time column. If it is not on time column, we
      // will use Filter + NestedLoopJoin instead.
      // Attention: For now, join on time column is assumed to hold the following condition: left
      // and right both contains the substring time.
      // todo: after supporting join on other columns for the user, we need to remove the following
      // code, since the condition does not hold anymore.
      //  This is temporary workaround.
      Expression left = equality.getLeft();
      Expression right = equality.getRight();
      return (left instanceof SymbolReference
              && ((SymbolReference) left).getName().contains(IoTDBConstant.TIME))
          && (right instanceof SymbolReference
              && ((SymbolReference) right).getName().contains(IoTDBConstant.TIME));
    }

    private Symbol symbolForExpression(Expression expression) {
      if (expression instanceof SymbolReference) {
        return Symbol.from(expression);
      }

      // TODO(beyyes) verify the rightness of type
      return symbolAllocator.newSymbol(expression, analysis.getType(expression));
    }

    @Override
    public PlanNode visitInsertTablet(InsertTabletNode node, RewriteContext context) {
      return node;
    }

    @Override
    public PlanNode visitRelationalInsertTablet(
        RelationalInsertTabletNode node, RewriteContext context) {
      return node;
    }

    private DataPartition fetchDataPartitionByDevices(
        String database, List<DeviceEntry> deviceEntries, Filter globalTimeFilter) {
      Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
          getTimePartitionSlotList(globalTimeFilter, queryContext);

      // there is no satisfied time range
      if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
        return new DataPartition(
            Collections.emptyMap(),
            CONFIG.getSeriesPartitionExecutorClass(),
            CONFIG.getSeriesPartitionSlotNum());
      }

      List<DataPartitionQueryParam> dataPartitionQueryParams =
          deviceEntries.stream()
              .map(
                  deviceEntry ->
                      new DataPartitionQueryParam(
                          deviceEntry.getDeviceID(), res.left, res.right.left, res.right.right))
              .collect(Collectors.toList());

      if (res.right.left || res.right.right) {
        return metadata.getDataPartitionWithUnclosedTimeRange(database, dataPartitionQueryParams);
      } else {
        return metadata.getDataPartition(database, dataPartitionQueryParams);
      }
    }
  }

  public static boolean containsDiffFunction(Expression expression) {
    if (expression instanceof FunctionCall
        && "diff".equalsIgnoreCase(((FunctionCall) expression).getName().toString())) {
      return true;
    }

    if (!expression.getChildren().isEmpty()) {
      for (Node node : expression.getChildren()) {
        if (containsDiffFunction((Expression) node)) {
          return true;
        }
      }
    }

    return false;
  }

  private static class RewriteContext {
    Expression inheritedPredicate;

    RewriteContext(Expression inheritedPredicate) {
      this.inheritedPredicate = inheritedPredicate;
    }

    RewriteContext() {
      this.inheritedPredicate = TRUE_LITERAL;
    }
  }

  private static class SplitExpression {
    // indexed tag expressions, such as `tag1 = 'A'`
    List<Expression> metadataExpressions;
    // expressions can push down into TableScan, such as `time > 1 and s_1 = 1`
    List<Expression> expressionsCanPushDown;
    // expressions can not push down into TableScan, such as `s_1 is null`
    List<Expression> expressionsCannotPushDown;

    @Nullable String timeColumnName;

    public SplitExpression(
        List<Expression> metadataExpressions,
        List<Expression> expressionsCanPushDown,
        List<Expression> expressionsCannotPushDown,
        @Nullable String timeColumnName) {
      this.metadataExpressions = requireNonNull(metadataExpressions, "metadataExpressions is null");
      this.expressionsCanPushDown =
          requireNonNull(expressionsCanPushDown, "expressionsCanPushDown is null");
      this.expressionsCannotPushDown =
          requireNonNull(expressionsCannotPushDown, "expressionsCannotPushDown is null");
      this.timeColumnName = timeColumnName;
    }

    public List<Expression> getMetadataExpressions() {
      return this.metadataExpressions;
    }

    public List<Expression> getExpressionsCanPushDown() {
      return this.expressionsCanPushDown;
    }

    public List<Expression> getExpressionsCannotPushDown() {
      return this.expressionsCannotPushDown;
    }

    @Nullable
    public String getTimeColumnName() {
      return timeColumnName;
    }
  }
}
