/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
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
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.MetadataExpressionTransformForJoin;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor.extractUnique;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator.isDeterministic;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.extractGlobalTimeFilter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.filterDeterministicConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.FULL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.RIGHT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>After the optimized rule {@link SimplifyExpressions} finished, predicate expression in
 * FilterNode has been transformed to conjunctive normal forms(CNF).
 *
 * <p>In this class, we examine each expression in CNFs, determine how to use it, in metadata query,
 * or pushed down into ScanOperators, or it can only be used in FilterNode above with TableScanNode.
 *
 * <ul>
 *   <li>For metadata query expressions, it will be used in {@code tableIndexScan} method to
 *       generate the deviceEntries and DataPartition used for TableScanNode.
 *   <li>For expressions which can be pushed into TableScanNode, we will execute {@code
 *       extractGlobalTimeFilter}, to extract the timePredicate and pushDownValuePredicate.
 *   <li>Expression which can not be pushed down into TableScanNode, will be used in the FilterNode
 *       above of TableScanNode.
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
        null);
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Void> {
    private final MPPQueryContext queryContext;
    private final Analysis analysis;
    private final Metadata metadata;
    private Expression predicate;
    private final SymbolAllocator symbolAllocator;
    private final QueryId queryId;
    private boolean hasJoinNode;

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
    public PlanNode visitPlan(PlanNode node, Void context) {
      throw new IllegalArgumentException(
          String.format("Unexpected plan node: %s in rule PushPredicateIntoTableScan", node));
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, Void context) {
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitTwoChildProcess(TwoChildProcessNode node, Void context) {
      node.setLeftChild(node.getLeftChild().accept(this, context));
      node.setRightChild(node.getRightChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, Void context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Void context) {

      if (node.getPredicate() != null) {

        predicate = node.getPredicate();

        // when exist diff function, predicate can not be pushed down into TableScanNode
        if (containsDiffFunction(predicate)) {
          node.setChild(node.getChild().accept(this, context));
          return node;
        }

        if (node.getChild() instanceof TableScanNode) {
          // child of FilterNode is TableScanNode, means FilterNode must get from where clause
          return combineFilterAndScan((TableScanNode) node.getChild());
        } else if (node.getChild() instanceof JoinNode) {
          return visitJoin((JoinNode) node.getChild(), context);
        } else {
          // FilterNode may get from having or subquery
          node.setChild(node.getChild().accept(this, context));
          return node;
        }

      } else {
        throw new IllegalStateException(
            "Filter node has no predicate, node: " + node.getPlanNodeId());
      }
    }

    public PlanNode combineFilterAndScan(TableScanNode tableScanNode) {
      SplitExpression splitExpression = splitPredicate(tableScanNode);

      // exist expressions can push down to scan operator
      if (!splitExpression.getExpressionsCanPushDown().isEmpty()) {
        List<Expression> expressions = splitExpression.getExpressionsCanPushDown();
        Expression pushDownPredicate =
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions);

        // extract global time filter and set it to TableScanNode
        Pair<Expression, Boolean> resultPair = extractGlobalTimeFilter(pushDownPredicate);
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
      PlanNode resultNode =
          tableMetadataIndexScan(tableScanNode, splitExpression.getMetadataExpressions());

      // exist expressions can not push down to scan operator
      if (!splitExpression.getExpressionsCannotPushDown().isEmpty()) {
        List<Expression> expressions = splitExpression.getExpressionsCannotPushDown();
        return new FilterNode(
            queryId.genPlanNodeId(),
            resultNode,
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions));
      }

      return resultNode;
    }

    private SplitExpression splitPredicate(TableScanNode node) {
      Set<String> idOrAttributeColumnNames = new HashSet<>(node.getAssignments().size());
      Set<String> measurementColumnNames = new HashSet<>(node.getAssignments().size());
      for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
        Symbol columnSymbol = entry.getKey();
        ColumnSchema columnSchema = entry.getValue();
        if (MEASUREMENT.equals(columnSchema.getColumnCategory())
            || TIME.equals(columnSchema.getColumnCategory())) {
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
            metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
      }

      if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, predicate)) {
        metadataExpressions.add(predicate);
      } else if (PredicateCombineIntoTableScanChecker.check(measurementColumnNames, predicate)) {
        expressionsCanPushDown.add(predicate);
      } else {
        expressionsCannotPushDown.add(predicate);
      }

      return new SplitExpression(
          metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Void context) {
      hasJoinNode = true;
      Expression inheritedPredicate = predicate;

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
          InnerJoinPushDownResult innerJoinPushDownResult =
              processInnerJoin(
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
      for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
        if (joinEqualityExpression(
            conjunct,
            node.getLeftChild().getOutputSymbols(),
            node.getRightChild().getOutputSymbols())) {
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
        }
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
      // TODO(beyyes) make the judgement code tidy
      if (!equiJoinClausesUnmodified) {
        ProjectNode projectNode =
            new ProjectNode(queryId.genPlanNodeId(), node.getLeftChild(), leftProjections.build());
        if (TRUE_LITERAL.equals(leftPredicate)) {
          leftSource = projectNode.accept(this, context);
        } else {
          FilterNode filterNode =
              new FilterNode(queryId.genPlanNodeId(), projectNode, leftPredicate);
          leftSource = filterNode.accept(this, context);
        }

        projectNode =
            new ProjectNode(
                queryId.genPlanNodeId(), node.getRightChild(), rightProjections.build());
        if (TRUE_LITERAL.equals(rightPredicate)) {
          rightSource = projectNode.accept(this, context);
        } else {
          FilterNode filterNode =
              new FilterNode(queryId.genPlanNodeId(), projectNode, rightPredicate);
          rightSource = filterNode.accept(this, context);
        }
      } else {
        if (TRUE_LITERAL.equals(leftPredicate)) {
          leftSource = node.getLeftChild().accept(this, context);
        } else {
          FilterNode filterNode =
              new FilterNode(queryId.genPlanNodeId(), node.getLeftChild(), leftPredicate);
          leftSource = filterNode.accept(this, context);
        }
        if (TRUE_LITERAL.equals(rightPredicate)) {
          rightSource = node.getRightChild().accept(this, context);
        } else {
          FilterNode filterNode =
              new FilterNode(queryId.genPlanNodeId(), node.getRightChild(), rightPredicate);
          rightSource = filterNode.accept(this, context);
        }
      }

      Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(joinFilter));
      if (newJoinFilter.get().equals(TRUE_LITERAL)) {
        newJoinFilter = Optional.empty();
      }

      if (node.getJoinType() == INNER && newJoinFilter.isPresent() && equiJoinClauses.isEmpty()) {
        throw new IllegalStateException("INNER JOIN only support equiJoinClauses");
        // if we do not have any equi conjunct we do not pushdown non-equality condition into
        // inner join, so we plan execution as nested-loops-join followed by filter instead
        // hash join.
        // postJoinPredicate = combineConjuncts(postJoinPredicate, newJoinFilter.get());
        // newJoinFilter = Optional.empty();
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
                node.isMaySkipOutputDuplicates(),
                newJoinFilter,
                node.getLeftHashSymbol(),
                node.getRightHashSymbol(),
                node.isSpillable());
      }
      Symbol timeSymbol = Symbol.of("time");
      OrderingScheme orderingScheme =
          new OrderingScheme(
              Collections.singletonList(timeSymbol),
              Collections.singletonMap(timeSymbol, ASC_NULLS_LAST));
      SortNode leftSortNode =
          new SortNode(
              queryId.genPlanNodeId(),
              ((JoinNode) output).getLeftChild(),
              orderingScheme,
              false,
              false);
      SortNode rightSortNode =
          new SortNode(
              queryId.genPlanNodeId(),
              ((JoinNode) output).getRightChild(),
              orderingScheme,
              false,
              false);
      ((JoinNode) output).setLeftChild(leftSortNode);
      ((JoinNode) output).setRightChild(rightSortNode);

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
              node.getLeftOutputSymbols(),
              node.getRightOutputSymbols(),
              node.isMaySkipOutputDuplicates(),
              node.getFilter(),
              node.getLeftHashSymbol(),
              node.getRightHashSymbol(),
              node.isSpillable());
        }
        return new JoinNode(
            node.getPlanNodeId(),
            canConvertToLeftJoin ? LEFT : RIGHT,
            node.getLeftChild(),
            node.getRightChild(),
            node.getCriteria(),
            node.getLeftOutputSymbols(),
            node.getRightOutputSymbols(),
            node.isMaySkipOutputDuplicates(),
            node.getFilter(),
            node.getLeftHashSymbol(),
            node.getRightHashSymbol(),
            node.isSpillable());
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
          node.getLeftOutputSymbols(),
          node.getRightOutputSymbols(),
          node.isMaySkipOutputDuplicates(),
          node.getFilter(),
          node.getLeftHashSymbol(),
          node.getRightHashSymbol(),
          node.isSpillable());
    }

    private boolean canConvertOuterToInner(
        List<Symbol> innerSymbolsForOuterJoin, Expression inheritedPredicate) {
      Set<Symbol> innerSymbols = ImmutableSet.copyOf(innerSymbolsForOuterJoin);
      for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
        if (isDeterministic(conjunct)) {
          // Ignore a conjunct for this test if we cannot deterministically get responses from it
          // Object response = nullInputEvaluator(innerSymbols, conjunct);
          // if (response == null || response instanceof NullLiteral ||
          // Boolean.FALSE.equals(response)) {
          // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for the
          // inner side symbols of an outer join
          // then this conjunct removes all effects of the outer join, and effectively turns this
          // into an equivalent of an inner join.
          // So, let's just rewrite this join as an INNER join
          return true;
          // }
        }
      }
      return false;
    }

    private InnerJoinPushDownResult processInnerJoin(
        Expression inheritedPredicate,
        Expression leftEffectivePredicate,
        Expression rightEffectivePredicate,
        Expression joinPredicate,
        Collection<Symbol> leftSymbols,
        Collection<Symbol> rightSymbols) {
      checkArgument(
          leftSymbols.containsAll(extractUnique(leftEffectivePredicate)),
          "leftEffectivePredicate must only contain symbols from leftSymbols");
      checkArgument(
          rightSymbols.containsAll(extractUnique(rightEffectivePredicate)),
          "rightEffectivePredicate must only contain symbols from rightSymbols");

      ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.builder();
      ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.builder();
      ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

      // Strip out non-deterministic conjuncts
      extractConjuncts(inheritedPredicate).stream()
          .filter(deterministic -> !isDeterministic(deterministic))
          .forEach(joinConjuncts::add);
      inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

      extractConjuncts(joinPredicate).stream()
          .filter(expression -> !isDeterministic(expression))
          .forEach(joinConjuncts::add);
      joinPredicate = filterDeterministicConjuncts(joinPredicate);

      leftEffectivePredicate = filterDeterministicConjuncts(leftEffectivePredicate);
      rightEffectivePredicate = filterDeterministicConjuncts(rightEffectivePredicate);

      ImmutableSet<Symbol> leftScope = ImmutableSet.copyOf(leftSymbols);
      ImmutableSet<Symbol> rightScope = ImmutableSet.copyOf(rightSymbols);

      // Generate equality inferences
      EqualityInference allInference =
          new EqualityInference(
              metadata,
              inheritedPredicate,
              leftEffectivePredicate,
              rightEffectivePredicate,
              joinPredicate);
      EqualityInference allInferenceWithoutLeftInferred =
          new EqualityInference(
              metadata, inheritedPredicate, rightEffectivePredicate, joinPredicate);
      EqualityInference allInferenceWithoutRightInferred =
          new EqualityInference(
              metadata, inheritedPredicate, leftEffectivePredicate, joinPredicate);

      // Add equalities from the inference back in
      leftPushDownConjuncts.addAll(
          allInferenceWithoutLeftInferred
              .generateEqualitiesPartitionedBy(leftScope)
              .getScopeEqualities());
      rightPushDownConjuncts.addAll(
          allInferenceWithoutRightInferred
              .generateEqualitiesPartitionedBy(rightScope)
              .getScopeEqualities());
      joinConjuncts.addAll(
          allInference
              .generateEqualitiesPartitionedBy(leftScope)
              .getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as
      // part of the join predicate

      // Sort through conjuncts in inheritedPredicate that were not used for inference
      EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate)
          .forEach(
              conjunct -> {
                Expression leftRewrittenConjunct = allInference.rewrite(conjunct, leftScope);
                if (leftRewrittenConjunct != null) {
                  leftPushDownConjuncts.add(leftRewrittenConjunct);
                }

                Expression rightRewrittenConjunct = allInference.rewrite(conjunct, rightScope);
                if (rightRewrittenConjunct != null) {
                  rightPushDownConjuncts.add(rightRewrittenConjunct);
                }

                // Drop predicate after join only if unable to push down to either side
                if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                  joinConjuncts.add(conjunct);
                }
              });

      // See if we can push the right effective predicate to the left side
      EqualityInference.nonInferrableConjuncts(metadata, rightEffectivePredicate)
          .map(conjunct -> allInference.rewrite(conjunct, leftScope))
          .filter(Objects::nonNull)
          .forEach(leftPushDownConjuncts::add);

      // See if we can push the left effective predicate to the right side
      EqualityInference.nonInferrableConjuncts(metadata, leftEffectivePredicate)
          .map(conjunct -> allInference.rewrite(conjunct, rightScope))
          .filter(Objects::nonNull)
          .forEach(rightPushDownConjuncts::add);

      // See if we can push any parts of the join predicates to either side
      EqualityInference.nonInferrableConjuncts(metadata, joinPredicate)
          .forEach(
              conjunct -> {
                Expression leftRewritten = allInference.rewrite(conjunct, leftScope);
                if (leftRewritten != null) {
                  leftPushDownConjuncts.add(leftRewritten);
                }

                Expression rightRewritten = allInference.rewrite(conjunct, rightScope);
                if (rightRewritten != null) {
                  rightPushDownConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                  joinConjuncts.add(conjunct);
                }
              });

      return new InnerJoinPushDownResult(
          combineConjuncts(leftPushDownConjuncts.build()),
          combineConjuncts(rightPushDownConjuncts.build()),
          combineConjuncts(joinConjuncts.build()),
          TRUE_LITERAL);
    }

    private Expression extractJoinPredicate(JoinNode joinNode) {
      ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
        builder.add(equiJoinClause.toExpression());
      }
      joinNode.getFilter().ifPresent(builder::add);
      return combineConjuncts(builder.build());
    }

    private boolean joinEqualityExpression(
        Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols) {
      return joinComparisonExpression(
          expression,
          leftSymbols,
          rightSymbols,
          ImmutableSet.of(ComparisonExpression.Operator.EQUAL));
    }

    private boolean joinComparisonExpression(
        Expression expression,
        Collection<Symbol> leftSymbols,
        Collection<Symbol> rightSymbols,
        Set<ComparisonExpression.Operator> operators) {
      // At this point in time, our join predicates need to be deterministic
      if (expression instanceof ComparisonExpression && isDeterministic(expression)) {
        ComparisonExpression comparison = (ComparisonExpression) expression;
        if (operators.contains(comparison.getOperator())) {
          Set<Symbol> symbols1 = extractUnique(comparison.getLeft());
          Set<Symbol> symbols2 = extractUnique(comparison.getRight());
          if (symbols1.isEmpty() || symbols2.isEmpty()) {
            return false;
          }
          return (leftSymbols.containsAll(symbols1) && rightSymbols.containsAll(symbols2))
              || (rightSymbols.containsAll(symbols1) && leftSymbols.containsAll(symbols2));
        }
      }
      return false;
    }

    private Symbol symbolForExpression(Expression expression) {
      if (expression instanceof SymbolReference) {
        return Symbol.from(expression);
      }

      // TODO(beyyes) verify the rightness of type
      return symbolAllocator.newSymbol(expression, analysis.getType(expression));
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Void context) {
      return tableMetadataIndexScan(node, Collections.emptyList());
    }

    @Override
    public PlanNode visitInsertTablet(InsertTabletNode node, Void context) {
      return node;
    }

    @Override
    public PlanNode visitRelationalInsertTablet(RelationalInsertTabletNode node, Void context) {
      return node;
    }

    /** Get deviceEntries and DataPartition used in TableScan. */
    private PlanNode tableMetadataIndexScan(
        TableScanNode tableScanNode, List<Expression> metadataExpressions) {
      // for join operator, columnSymbols in TableScanNode is renamed, which adds suffix for origin
      // column name,
      // add a new ProjectNode above TableScanNode.
      boolean tableScanNodeColumnsRenamed = false;
      for (Map.Entry<Symbol, ColumnSchema> entry : tableScanNode.getAssignments().entrySet()) {
        Symbol columnSymbol = entry.getKey();
        ColumnSchema columnSchema = entry.getValue();
        if (!columnSymbol.getName().equals(columnSchema.getName())) {
          tableScanNodeColumnsRenamed = true;
          break;
        }
      }
      ProjectNode newProjectNode = null;
      if (tableScanNodeColumnsRenamed) {
        metadataExpressions.replaceAll(
            expression1 ->
                MetadataExpressionTransformForJoin.transform(
                    expression1, tableScanNode.getAssignments()));

        Assignments.Builder projectAssignments = Assignments.builder();
        ImmutableMap.Builder<Symbol, ColumnSchema> tableScanAssignments = ImmutableMap.builder();
        for (Map.Entry<Symbol, ColumnSchema> entry : tableScanNode.getAssignments().entrySet()) {
          Symbol columnSymbol = entry.getKey();
          ColumnSchema columnSchema = entry.getValue();
          projectAssignments.put(columnSymbol, new SymbolReference(columnSchema.getName()));
          tableScanAssignments.put(Symbol.of(columnSchema.getName()), columnSchema);
        }
        newProjectNode =
            new ProjectNode(queryId.genPlanNodeId(), tableScanNode, projectAssignments.build());
        tableScanNode.setAssignments(tableScanAssignments.build());
      }

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
      List<DeviceEntry> deviceEntries =
          metadata.indexScan(
              tableScanNode.getQualifiedObjectName(),
              metadataExpressions,
              attributeColumns,
              queryContext);
      tableScanNode.setDeviceEntries(deviceEntries);

      if (deviceEntries.isEmpty()) {
        analysis.setFinishQueryAfterAnalyze();
        analysis.setEmptyDataSource(true);
      } else {
        Filter timeFilter =
            tableScanNode
                .getTimePredicate()
                .map(value -> value.accept(new ConvertPredicateToTimeFilterVisitor(), null))
                .orElse(null);
        tableScanNode.setTimeFilter(timeFilter);
        String treeModelDatabase =
            "root." + tableScanNode.getQualifiedObjectName().getDatabaseName();
        DataPartition dataPartition =
            fetchDataPartitionByDevices(treeModelDatabase, deviceEntries, timeFilter);

        if (dataPartition.getDataPartitionMap().size() > 1) {
          throw new IllegalStateException(
              "Table model can only process data only in one database yet!");
        }

        if (dataPartition.getDataPartitionMap().isEmpty()) {
          analysis.setFinishQueryAfterAnalyze();
          analysis.setEmptyDataSource(true);
        } else {
          analysis.upsertDataPartition(dataPartition);
        }
      }

      if (tableScanNodeColumnsRenamed) {
        return newProjectNode;
      } else {
        return tableScanNode;
      }
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

  private static class SplitExpression {
    // indexed tag expressions, such as `tag1 = 'A'`
    List<Expression> metadataExpressions;
    // expressions can push down into TableScan, such as `time > 1 and s_1 = 1`
    List<Expression> expressionsCanPushDown;
    // expressions can not push down into TableScan, such as `s_1 is null`
    List<Expression> expressionsCannotPushDown;

    public SplitExpression(
        List<Expression> metadataExpressions,
        List<Expression> expressionsCanPushDown,
        List<Expression> expressionsCannotPushDown) {
      this.metadataExpressions = requireNonNull(metadataExpressions, "metadataExpressions is null");
      this.expressionsCanPushDown =
          requireNonNull(expressionsCanPushDown, "expressionsCanPushDown is null");
      this.expressionsCannotPushDown =
          requireNonNull(expressionsCannotPushDown, "expressionsCannotPushDown is null");
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
  }

  private static class InnerJoinPushDownResult {
    private final Expression leftPredicate;
    private final Expression rightPredicate;
    private final Expression joinPredicate;
    private final Expression postJoinPredicate;

    private InnerJoinPushDownResult(
        Expression leftPredicate,
        Expression rightPredicate,
        Expression joinPredicate,
        Expression postJoinPredicate) {
      this.leftPredicate = leftPredicate;
      this.rightPredicate = rightPredicate;
      this.joinPredicate = joinPredicate;
      this.postJoinPredicate = postJoinPredicate;
    }

    private Expression getLeftPredicate() {
      return leftPredicate;
    }

    private Expression getRightPredicate() {
      return rightPredicate;
    }

    private Expression getJoinPredicate() {
      return joinPredicate;
    }

    private Expression getPostJoinPredicate() {
      return postJoinPredicate;
    }
  }
}
