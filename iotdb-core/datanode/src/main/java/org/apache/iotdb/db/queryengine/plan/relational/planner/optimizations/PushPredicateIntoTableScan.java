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
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateCombineIntoTableScanChecker;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoMetadataChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.EqualityInference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrExpressionInterpreter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ReplaceSymbolInExpression;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.FIELD;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.PARTITION_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.SCHEMA_FETCHER;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ExpressionSymbolInliner.inlineSymbols;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_FIRST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor.extractUnique;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator.isDeterministic;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.extractGlobalTimeFilter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.filterDeterministicConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.isEffectivelyLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.ChildReplacer.replaceChildren;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.FULL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.RIGHT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.UNSUPPORTED_JOIN_CRITERIA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.extractJoinPredicate;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.joinEqualityExpression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.processInnerJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.JoinUtils.processLimitedOuterJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;

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

  private final PlannerContext plannerContext;

  private final IrTypeAnalyzer typeAnalyzer;

  public PushPredicateIntoTableScan(PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer) {
    this.plannerContext = plannerContext;
    this.typeAnalyzer = typeAnalyzer;
  }

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    return plan.accept(
        new Rewriter(
            context.getQueryContext(),
            context.getAnalysis(),
            context.getMetadata(),
            context.getSymbolAllocator(),
            plannerContext,
            typeAnalyzer),
        new RewriteContext(TRUE_LITERAL));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriteContext> {
    private final MPPQueryContext queryContext;
    private final Analysis analysis;
    private final Metadata metadata;
    private final SymbolAllocator symbolAllocator;
    private final QueryId queryId;
    private final PlannerContext plannerContext;
    private final IrTypeAnalyzer typeAnalyzer;

    Rewriter(
        MPPQueryContext queryContext,
        Analysis analysis,
        Metadata metadata,
        SymbolAllocator symbolAllocator,
        PlannerContext plannerContext,
        IrTypeAnalyzer typeAnalyzer) {
      this.queryContext = queryContext;
      this.analysis = analysis;
      this.metadata = metadata;
      this.symbolAllocator = symbolAllocator;
      this.queryId = queryContext.getQueryId();
      this.plannerContext = plannerContext;
      this.typeAnalyzer = typeAnalyzer;
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

      Set<Symbol> deterministicSymbols =
          node.getAssignments().entrySet().stream()
              .filter(entry -> isDeterministic(entry.getValue()))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());

      Predicate<Expression> deterministic =
          conjunct -> deterministicSymbols.containsAll(extractUnique(conjunct));

      Expression inheritedPredicate =
          context.inheritedPredicate != null ? context.inheritedPredicate : TRUE_LITERAL;
      Map<Boolean, List<Expression>> conjuncts =
          extractConjuncts(inheritedPredicate).stream()
              .collect(Collectors.partitioningBy(deterministic));

      // Push down conjuncts from the inherited predicate that only depend on deterministic
      // assignments with
      // certain limitations.
      List<Expression> deterministicConjuncts = conjuncts.get(true);

      // We partition the expressions in the deterministicConjuncts into two lists, and only inline
      // the
      // expressions that are in the inlining targets list.
      Map<Boolean, List<Expression>> inlineConjuncts =
          deterministicConjuncts.stream()
              .collect(
                  Collectors.partitioningBy(expression -> isInliningCandidate(expression, node)));

      List<Expression> inlinedDeterministicConjuncts =
          inlineConjuncts.get(true).stream()
              .map(entry -> inlineSymbols(node.getAssignments().getMap(), entry))
              .map(
                  conjunct ->
                      canonicalizeExpression(
                          conjunct,
                          typeAnalyzer,
                          queryContext.getTypeProvider(),
                          plannerContext,
                          queryContext
                              .getSession())) // normalize expressions to a form that unwrapCasts
              // understands
              // no need for now
              // .map(conjunct -> unwrapCasts(session, plannerContext, typeAnalyzer, types,
              // conjunct))
              .collect(Collectors.toList());

      PlanNode rewrittenChild =
          node.getChild()
              .accept(this, new RewriteContext(combineConjuncts(inlinedDeterministicConjuncts)));

      PlanNode rewrittenNode = replaceChildren(node, ImmutableList.of(rewrittenChild));

      // All deterministic conjuncts that contains non-inlining targets, and non-deterministic
      // conjuncts,
      // if any, will be in the filter node.
      List<Expression> nonInliningConjuncts = inlineConjuncts.get(false);
      nonInliningConjuncts.addAll(conjuncts.get(false));

      if (!nonInliningConjuncts.isEmpty()) {
        rewrittenNode =
            new FilterNode(
                queryId.genPlanNodeId(), rewrittenNode, combineConjuncts(nonInliningConjuncts));
      }

      return rewrittenNode;
    }

    private boolean isInliningCandidate(Expression expression, ProjectNode node) {
      // TryExpressions should not be pushed down. However they are now being handled as lambda
      // passed to a FunctionCall now and should not affect predicate push down. So we want to make
      // sure the conjuncts are not TryExpressions.
      // verify(AstUtils.preOrder(expression).noneMatch(TryExpression.class::isInstance));

      // candidate symbols for inlining are
      //   1. references to simple constants or symbol references
      //   2. references to complex expressions that appear only once
      // which come from the node, as opposed to an enclosing scope.
      Set<Symbol> childOutputSet = ImmutableSet.copyOf(node.getOutputSymbols());
      Map<Symbol, Long> dependencies =
          SymbolsExtractor.extractAll(expression).stream()
              .filter(childOutputSet::contains)
              .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

      return dependencies.entrySet().stream()
          .allMatch(
              entry ->
                  entry.getValue() == 1
                      || isEffectivelyLiteral(
                          node.getAssignments().get(entry.getKey()),
                          plannerContext,
                          queryContext.getSession())
                      || node.getAssignments().get(entry.getKey()) instanceof SymbolReference);
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
        } else if (FIELD.equals(columnSchema.getColumnCategory())) {
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
        final DeviceTableScanNode tableScanNode,
        final List<Expression> metadataExpressions,
        String timeColumnName) {

      final List<String> attributeColumns = new ArrayList<>();
      int attributeIndex = 0;
      for (final Map.Entry<Symbol, ColumnSchema> entry :
          tableScanNode.getAssignments().entrySet()) {
        final Symbol columnSymbol = entry.getKey();
        final ColumnSchema columnSchema = entry.getValue();
        if (ATTRIBUTE.equals(columnSchema.getColumnCategory())) {
          attributeColumns.add(columnSchema.getName());
          tableScanNode.getTagAndAttributeIndexMap().put(columnSymbol, attributeIndex++);
        }
      }

      long startTime = System.nanoTime();
      final Map<String, List<DeviceEntry>> deviceEntriesMap =
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
      if (deviceEntriesMap.size() > 1) {
        throw new SemanticException("Tree device view with multiple databases is unsupported yet.");
      }
      final String deviceDatabase =
          !deviceEntriesMap.isEmpty() ? deviceEntriesMap.keySet().iterator().next() : null;
      final List<DeviceEntry> deviceEntries =
          Objects.nonNull(deviceDatabase)
              ? deviceEntriesMap.get(deviceDatabase)
              : Collections.emptyList();

      tableScanNode.setDeviceEntries(deviceEntries);
      if (deviceEntries.stream()
          .anyMatch(deviceEntry -> deviceEntry instanceof NonAlignedDeviceEntry)) {
        tableScanNode.setContainsNonAlignedDevice();
      }

      if (tableScanNode instanceof TreeDeviceViewScanNode) {
        ((TreeDeviceViewScanNode) tableScanNode).setTreeDBName(deviceDatabase);
      }

      final long schemaFetchCost = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance().recordTablePlanCost(SCHEMA_FETCHER, schemaFetchCost);
      queryContext.setFetchSchemaCost(schemaFetchCost);

      if (deviceEntries.isEmpty()) {
        if (analysis.noAggregates() && !analysis.hasJoinNode()) {
          // no device entries, queries(except aggregation and join) can be finished
          analysis.setEmptyDataSource(true);
          analysis.setFinishQueryAfterAnalyze();
        }
      } else {
        final Filter timeFilter =
            tableScanNode
                .getTimePredicate()
                .map(value -> value.accept(new ConvertPredicateToTimeFilterVisitor(), null))
                .orElse(null);

        tableScanNode.setTimeFilter(timeFilter);

        startTime = System.nanoTime();
        final DataPartition dataPartition =
            fetchDataPartitionByDevices(
                // for tree view, we need to pass actual tree db name to this method
                tableScanNode instanceof TreeDeviceViewScanNode
                    ? deviceDatabase
                    : tableScanNode.getQualifiedObjectName().getDatabaseName(),
                deviceEntries,
                timeFilter);

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

        final long fetchPartitionCost = System.nanoTime() - startTime;
        QueryPlanCostMetricSet.getInstance()
            .recordTablePlanCost(PARTITION_FETCHER, fetchPartitionCost);
        queryContext.setFetchPartitionCost(fetchPartitionCost);
      }
    }

    @Override
    public PlanNode visitJoin(JoinNode node, RewriteContext context) {
      Expression inheritedPredicate =
          context.inheritedPredicate != null ? context.inheritedPredicate : TRUE_LITERAL;

      // See if we can rewrite outer joins in terms of a plain inner join
      node = tryNormalizeToOuterToInnerJoin(node, inheritedPredicate);

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
        case LEFT:
          JoinUtils.OuterJoinPushDownResult leftOuterJoinPushDownResult =
              processLimitedOuterJoin(
                  metadata,
                  inheritedPredicate,
                  leftEffectivePredicate,
                  rightEffectivePredicate,
                  joinPredicate,
                  node.getLeftChild().getOutputSymbols(),
                  node.getRightChild().getOutputSymbols());
          leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
          rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
          postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
          newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
          break;
        case FULL:
          leftPredicate = TRUE_LITERAL;
          rightPredicate = TRUE_LITERAL;
          postJoinPredicate = inheritedPredicate;
          newJoinPredicate = joinPredicate;
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported join type in predicate push down: " + node.getJoinType().name());
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
      for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
        if (joinEqualityExpressionOnOneColumn(conjunct, node)) {
          ComparisonExpression equality = (ComparisonExpression) conjunct;

          boolean alignedComparison =
              new HashSet<>(node.getLeftChild().getOutputSymbols())
                  .containsAll(extractUnique(equality.getLeft()));
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
          if (conjunct.equals(TRUE_LITERAL) && node.getAsofCriteria().isPresent()) {
            continue;
          }
          if (node.getJoinType() != INNER) {
            throw new SemanticException(String.format(UNSUPPORTED_JOIN_CRITERIA, conjunct));
          }
          joinFilterBuilder.add(conjunct);
        }
      }

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

      Cardinality leftCardinality = extractCardinality(leftSource);
      Cardinality rightCardinality = extractCardinality(rightSource);
      if (leftCardinality.isAtMostScalar() || rightCardinality.isAtMostScalar()) {
        // if cardinality of left or right equals to 1, use NestedLoopJoin
        equiJoinClauses.forEach(
            equiJoinClause -> joinFilterBuilder.add(equiJoinClause.toExpression()));
        equiJoinClauses.clear();
      }

      List<Expression> joinFilter = joinFilterBuilder.build();
      Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(joinFilter));
      if (TRUE_LITERAL.equals(newJoinFilter.get())) {
        newJoinFilter = Optional.empty();
      }

      if (node.getJoinType() == INNER && newJoinFilter.isPresent()
      // && equiJoinClauses.isEmpty()
      ) {
        // if we do not have any equi conjunct we do not pushdown non-equality condition into
        // inner join, so we plan execution as nested-loops-join followed by filter instead
        // hash join.
        postJoinPredicate = combineConjuncts(postJoinPredicate, newJoinFilter.get());
        newJoinFilter = Optional.empty();
      }

      boolean filtersEquivalent =
          newJoinFilter.isPresent() == node.getFilter().isPresent()
              && (!newJoinFilter.isPresent()
              // || areExpressionsEquivalent(newJoinFilter.get(), node.getFilter().get());
              );

      PlanNode output = node;
      if (leftSource != node.getLeftChild()
          || rightSource != node.getRightChild()
          || !filtersEquivalent
          || !equiJoinClausesUnmodified) {
        // this branch is always executed in current version
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
                node.getAsofCriteria(),
                leftSource.getOutputSymbols(),
                rightSource.getOutputSymbols(),
                newJoinFilter,
                node.isSpillable());
      }

      JoinNode outputJoinNode = (JoinNode) output;
      if (!((JoinNode) output).isCrossJoin()) {
        // inner join or full join, use MergeSortJoinNode
        appendSortNodeForMergeSortJoin(outputJoinNode);
      }

      if (!TRUE_LITERAL.equals(postJoinPredicate)) {
        output =
            new FilterNode(
                queryContext.getQueryId().genPlanNodeId(), outputJoinNode, postJoinPredicate);
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

    private boolean joinEqualityExpressionOnOneColumn(Expression conjunct, JoinNode node) {
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
      Expression left = equality.getLeft();
      Expression right = equality.getRight();
      return (left instanceof SymbolReference && right instanceof SymbolReference);
    }

    private Symbol symbolForExpression(Expression expression) {
      if (expression instanceof SymbolReference) {
        return Symbol.from(expression);
      }

      return symbolAllocator.newSymbol(expression, analysis.getType(expression));
    }

    private void appendSortNodeForMergeSortJoin(JoinNode joinNode) {
      int size = joinNode.getCriteria().size();
      JoinNode.AsofJoinClause asofJoinClause = joinNode.getAsofCriteria().orElse(null);
      if (asofJoinClause != null) {
        size++;
      }
      List<Symbol> leftOrderBy = new ArrayList<>(size);
      List<Symbol> rightOrderBy = new ArrayList<>(size);
      Map<Symbol, SortOrder> leftOrderings = new HashMap<>(size);
      Map<Symbol, SortOrder> rightOrderings = new HashMap<>(size);
      for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
        leftOrderBy.add(equiJoinClause.getLeft());
        leftOrderings.put(equiJoinClause.getLeft(), ASC_NULLS_LAST);
        rightOrderBy.add(equiJoinClause.getRight());
        rightOrderings.put(equiJoinClause.getRight(), ASC_NULLS_LAST);
      }
      if (asofJoinClause != null) {
        // if operator of AsofJoinClause is '>' or '>=', use DESC ordering for convenience of
        // process in BE
        boolean needDesc = asofJoinClause.isOperatorContainsGreater();
        leftOrderBy.add(asofJoinClause.getLeft());
        leftOrderings.put(asofJoinClause.getLeft(), needDesc ? DESC_NULLS_LAST : ASC_NULLS_LAST);
        rightOrderBy.add(asofJoinClause.getRight());
        rightOrderings.put(asofJoinClause.getRight(), needDesc ? DESC_NULLS_LAST : ASC_NULLS_LAST);
      }
      OrderingScheme leftOrderingScheme = new OrderingScheme(leftOrderBy, leftOrderings);
      OrderingScheme rightOrderingScheme = new OrderingScheme(rightOrderBy, rightOrderings);
      SortNode leftSortNode =
          new SortNode(
              queryId.genPlanNodeId(), joinNode.getLeftChild(), leftOrderingScheme, false, false);
      SortNode rightSortNode =
          new SortNode(
              queryId.genPlanNodeId(), joinNode.getRightChild(), rightOrderingScheme, false, false);
      joinNode.setLeftChild(leftSortNode);
      joinNode.setRightChild(rightSortNode);
    }

    @Override
    public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext context) {
      Expression inheritedPredicate =
          context.inheritedPredicate != null ? context.inheritedPredicate : TRUE_LITERAL;
      if (!extractConjuncts(inheritedPredicate)
          .contains(node.getSemiJoinOutput().toSymbolReference())) {
        return visitNonFilteringSemiJoin(node, context);
      }
      return visitFilteringSemiJoin(node, context);
    }

    private PlanNode visitNonFilteringSemiJoin(SemiJoinNode node, RewriteContext context) {
      Expression inheritedPredicate =
          context.inheritedPredicate != null ? context.inheritedPredicate : TRUE_LITERAL;

      List<Expression> sourceConjuncts = new ArrayList<>();
      List<Expression> postJoinConjuncts = new ArrayList<>();

      // TODO: see if there are predicates that can be inferred from the semi join output
      PlanNode rewrittenFilteringSource =
          node.getFilteringSource().accept(this, new RewriteContext());

      // Push inheritedPredicates down to the source if they don't involve the semi join output
      ImmutableSet<Symbol> sourceScope = ImmutableSet.copyOf(node.getSource().getOutputSymbols());
      EqualityInference inheritedInference = new EqualityInference(metadata, inheritedPredicate);
      EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate)
          .forEach(
              conjunct -> {
                Expression rewrittenConjunct = inheritedInference.rewrite(conjunct, sourceScope);
                // Since each source row is reflected exactly once in the output, ok to push
                // non-deterministic predicates down
                if (rewrittenConjunct != null) {
                  sourceConjuncts.add(rewrittenConjunct);
                } else {
                  postJoinConjuncts.add(conjunct);
                }
              });

      // Add the inherited equality predicates back in
      EqualityInference.EqualityPartition equalityPartition =
          inheritedInference.generateEqualitiesPartitionedBy(sourceScope);
      sourceConjuncts.addAll(equalityPartition.getScopeEqualities());
      postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
      postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

      PlanNode rewrittenSource =
          node.getSource().accept(this, new RewriteContext(combineConjuncts(sourceConjuncts)));

      PlanNode output = appendSortNodeForSemiJoin(node, rewrittenSource, rewrittenFilteringSource);

      if (!postJoinConjuncts.isEmpty()) {
        output =
            new FilterNode(queryId.genPlanNodeId(), output, combineConjuncts(postJoinConjuncts));
      }
      return output;
    }

    private SemiJoinNode appendSortNodeForSemiJoin(
        SemiJoinNode node, PlanNode rewrittenSource, PlanNode rewrittenFilteringSource) {
      OrderingScheme sourceOrderingScheme =
          new OrderingScheme(
              ImmutableList.of(node.getSourceJoinSymbol()),
              ImmutableMap.of(node.getSourceJoinSymbol(), ASC_NULLS_LAST));
      OrderingScheme filteringSourceOrderingScheme =
          new OrderingScheme(
              ImmutableList.of(node.getFilteringSourceJoinSymbol()),
              // NULL first is used to make sure that we can know if there's null value in the
              // result set of right table.
              // For x in (subquery), if subquery returns null and some value a,b, and x is not
              // in(a,b), the result of SemiJoinOutput should be NULL.
              ImmutableMap.of(node.getFilteringSourceJoinSymbol(), ASC_NULLS_FIRST));
      SortNode sourceSortNode =
          new SortNode(
              queryId.genPlanNodeId(), rewrittenSource, sourceOrderingScheme, false, false);
      SortNode filteringSourceSortNode =
          new SortNode(
              queryId.genPlanNodeId(),
              rewrittenFilteringSource,
              filteringSourceOrderingScheme,
              false,
              false);
      return new SemiJoinNode(
          node.getPlanNodeId(),
          sourceSortNode,
          filteringSourceSortNode,
          node.getSourceJoinSymbol(),
          node.getFilteringSourceJoinSymbol(),
          node.getSemiJoinOutput());
    }

    private PlanNode visitFilteringSemiJoin(SemiJoinNode node, RewriteContext context) {
      Expression inheritedPredicate =
          context.inheritedPredicate != null ? context.inheritedPredicate : TRUE_LITERAL;
      Expression deterministicInheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);
      Expression sourceEffectivePredicate = TRUE_LITERAL;
      Expression filteringSourceEffectivePredicate = TRUE_LITERAL;
      // Expression sourceEffectivePredicate =
      // filterDeterministicConjuncts(effectivePredicateExtractor.extract(session, node.getSource(),
      // types, typeAnalyzer));
      // Expression filteringSourceEffectivePredicate = filterDeterministicConjuncts(metadata,
      // effectivePredicateExtractor.extract(session, node.getFilteringSource(), types,
      // typeAnalyzer));
      Expression joinExpression =
          new ComparisonExpression(
              EQUAL,
              node.getSourceJoinSymbol().toSymbolReference(),
              node.getFilteringSourceJoinSymbol().toSymbolReference());

      List<Symbol> sourceSymbols = node.getSource().getOutputSymbols();
      List<Symbol> filteringSourceSymbols = node.getFilteringSource().getOutputSymbols();

      List<Expression> sourceConjuncts = new ArrayList<>();
      List<Expression> filteringSourceConjuncts = new ArrayList<>();
      List<Expression> postJoinConjuncts = new ArrayList<>();

      // Generate equality inferences
      EqualityInference allInference =
          new EqualityInference(
              metadata,
              deterministicInheritedPredicate,
              sourceEffectivePredicate,
              filteringSourceEffectivePredicate,
              joinExpression);
      EqualityInference allInferenceWithoutSourceInferred =
          new EqualityInference(
              metadata,
              deterministicInheritedPredicate,
              filteringSourceEffectivePredicate,
              joinExpression);
      EqualityInference allInferenceWithoutFilteringSourceInferred =
          new EqualityInference(
              metadata, deterministicInheritedPredicate, sourceEffectivePredicate, joinExpression);

      // Push inheritedPredicates down to the source if they don't involve the semi join output
      Set<Symbol> sourceScope = ImmutableSet.copyOf(sourceSymbols);
      EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate)
          .forEach(
              conjunct -> {
                Expression rewrittenConjunct = allInference.rewrite(conjunct, sourceScope);
                // Since each source row is reflected exactly once in the output, ok to push
                // non-deterministic predicates down
                if (rewrittenConjunct != null) {
                  sourceConjuncts.add(rewrittenConjunct);
                } else {
                  postJoinConjuncts.add(conjunct);
                }
              });

      // Push inheritedPredicates down to the filtering source if possible
      Set<Symbol> filterScope = ImmutableSet.copyOf(filteringSourceSymbols);
      EqualityInference.nonInferrableConjuncts(metadata, deterministicInheritedPredicate)
          .forEach(
              conjunct -> {
                Expression rewrittenConjunct = allInference.rewrite(conjunct, filterScope);
                // We cannot push non-deterministic predicates to filtering side. Each filtering
                // side row have to be
                // logically reevaluated for each source row.
                if (rewrittenConjunct != null) {
                  filteringSourceConjuncts.add(rewrittenConjunct);
                }
              });

      // move effective predicate conjuncts source <-> filter
      // See if we can push the filtering source effective predicate to the source side
      EqualityInference.nonInferrableConjuncts(metadata, filteringSourceEffectivePredicate)
          .map(conjunct -> allInference.rewrite(conjunct, sourceScope))
          .filter(Objects::nonNull)
          .forEach(sourceConjuncts::add);

      // See if we can push the source effective predicate to the filtering source side
      EqualityInference.nonInferrableConjuncts(metadata, sourceEffectivePredicate)
          .map(conjunct -> allInference.rewrite(conjunct, filterScope))
          .filter(Objects::nonNull)
          .forEach(filteringSourceConjuncts::add);

      // Add equalities from the inference back in
      sourceConjuncts.addAll(
          allInferenceWithoutSourceInferred
              .generateEqualitiesPartitionedBy(sourceScope)
              .getScopeEqualities());
      filteringSourceConjuncts.addAll(
          allInferenceWithoutFilteringSourceInferred
              .generateEqualitiesPartitionedBy(filterScope)
              .getScopeEqualities());

      PlanNode rewrittenSource =
          node.getSource().accept(this, new RewriteContext(combineConjuncts(sourceConjuncts)));
      PlanNode rewrittenFilteringSource =
          node.getFilteringSource()
              .accept(this, new RewriteContext(combineConjuncts(filteringSourceConjuncts)));

      PlanNode output = appendSortNodeForSemiJoin(node, rewrittenSource, rewrittenFilteringSource);
      if (!postJoinConjuncts.isEmpty()) {
        output =
            new FilterNode(queryId.genPlanNodeId(), output, combineConjuncts(postJoinConjuncts));
      }
      return output;
    }

    @Override
    public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext context) {
      Expression inheritedPredicate =
          context.inheritedPredicate != null ? context.inheritedPredicate : TRUE_LITERAL;
      Set<Symbol> predicateSymbols = extractUnique(inheritedPredicate);
      checkState(
          !predicateSymbols.contains(node.getIdColumn()),
          "UniqueId in predicate is not yet supported");
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      return node.replaceChildren(ImmutableList.of(rewrittenChild));
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
        final String
            database, // for tree view, database should be the real tree db name with `root.` prefix
        final List<DeviceEntry> deviceEntries,
        final Filter globalTimeFilter) {
      final Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
          getTimePartitionSlotList(globalTimeFilter, queryContext);

      // there is no satisfied time range
      if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
        return new DataPartition(
            Collections.emptyMap(),
            CONFIG.getSeriesPartitionExecutorClass(),
            CONFIG.getSeriesPartitionSlotNum());
      }

      final List<DataPartitionQueryParam> dataPartitionQueryParams =
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

    private JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, Expression inheritedPredicate) {
      checkArgument(
          EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getJoinType()),
          "Unsupported join type: %s",
          node.getJoinType());

      if (node.getJoinType() == JoinNode.JoinType.INNER) {
        return node;
      }

      if (node.getJoinType() == JoinNode.JoinType.FULL) {
        boolean canConvertToLeftJoin =
            canConvertOuterToInner(node.getLeftChild().getOutputSymbols(), inheritedPredicate);
        boolean canConvertToRightJoin =
            canConvertOuterToInner(node.getRightChild().getOutputSymbols(), inheritedPredicate);
        if (!canConvertToLeftJoin && !canConvertToRightJoin) {
          return node;
        }
        if (canConvertToLeftJoin && canConvertToRightJoin) {
          return new JoinNode(
              node.getPlanNodeId(),
              INNER,
              node.getLeftChild(),
              node.getRightChild(),
              node.getCriteria(),
              node.getAsofCriteria(),
              node.getLeftOutputSymbols(),
              node.getRightOutputSymbols(),
              node.getFilter(),
              node.isSpillable());
        }
        if (canConvertToLeftJoin) {
          return new JoinNode(
              node.getPlanNodeId(),
              LEFT,
              node.getLeftChild(),
              node.getRightChild(),
              node.getCriteria(),
              node.getAsofCriteria(),
              node.getLeftOutputSymbols(),
              node.getRightOutputSymbols(),
              node.getFilter(),
              node.isSpillable());
        } else {
          // temp fix because right join is not supported for now.
          return node;
        }
        //        return new JoinNode(
        //            node.getPlanNodeId(),
        //            canConvertToLeftJoin ? LEFT : RIGHT,
        //            node.getLeftChild(),
        //            node.getRightChild(),
        //            node.getCriteria(),
        //            node.getLeftOutputSymbols(),
        //            node.getRightOutputSymbols(),
        //            node.getFilter(),
        //            node.isSpillable());
      }

      if (node.getJoinType() == JoinNode.JoinType.LEFT
              && !canConvertOuterToInner(
                  node.getRightChild().getOutputSymbols(), inheritedPredicate)
          || node.getJoinType() == JoinNode.JoinType.RIGHT
              && !canConvertOuterToInner(
                  node.getLeftChild().getOutputSymbols(), inheritedPredicate)) {
        return node;
      }
      return new JoinNode(
          node.getPlanNodeId(),
          JoinNode.JoinType.INNER,
          node.getLeftChild(),
          node.getRightChild(),
          node.getCriteria(),
          node.getAsofCriteria(),
          node.getLeftOutputSymbols(),
          node.getRightOutputSymbols(),
          node.getFilter(),
          node.isSpillable());
    }

    private boolean canConvertOuterToInner(
        List<Symbol> innerSymbolsForOuterJoin, Expression inheritedPredicate) {
      Set<Symbol> innerSymbols = ImmutableSet.copyOf(innerSymbolsForOuterJoin);
      for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
        if (isDeterministic(conjunct)) {
          // Ignore a conjunct for this test if we cannot deterministically get responses from it
          Object response = nullInputEvaluator(innerSymbols, conjunct);
          if (response == null
              || response instanceof NullLiteral
              || Boolean.FALSE.equals(response)) {
            // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for
            // the inner side symbols of an outer join
            // then this conjunct removes all effects of the outer join, and effectively turns this
            // into an equivalent of an inner join.
            // So, let's just rewrite this join as an INNER join
            return true;
          }
        }
      }
      return false;
    }

    /** Evaluates an expression's response to binding the specified input symbols to NULL */
    private Object nullInputEvaluator(Collection<Symbol> nullSymbols, Expression expression) {
      Map<NodeRef<Expression>, Type> expressionTypes =
          typeAnalyzer.getTypes(queryContext.getSession(), symbolAllocator.getTypes(), expression);
      return new IrExpressionInterpreter(
              expression, plannerContext, queryContext.getSession(), expressionTypes)
          .optimize(symbol -> nullSymbols.contains(symbol) ? null : symbol.toSymbolReference());
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
